# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`dns-cache` — a local DNS caching server (Go, single static binary) that listens on `:53`, forwards misses to upstream resolvers, persists the cache to SQLite, and exposes stats over a Unix socket and an HTTP dashboard.

The goal behind the design: keep the domains the user actually uses always in cache and always fresh, refreshed in the background even while idle, so that coming back to a site hours later is still an instant answer. Changes that trade that away (dropping idle domains, forgetting them on restart) go against the point of the project.

## Commands

```bash
make build                    # go build -o dns-cache -ldflags="-s -w" .
go vet ./...
gofmt -l .
sudo make install             # binary + /etc/dns-cache.yaml + systemd unit; runs daemon-reload
```

Tests: `cache/cache_test.go` (sliding stats window, `Set()` inheritance, negative TTL, usage days, serving past TTL), `handler_test.go` (query coalescing, serve-past-TTL + background refresh, against a fake upstream on 127.0.0.1), `refresher_test.go` (candidate selection), `persistence_test.go` (`LoadAll` on a temp DB). `go test -race ./...`, or `go test ./cache -run TestName -v` for a single one.

### Running locally

Port 53 needs root or `CAP_NET_BIND_SERVICE` (the systemd unit grants it). For iteration, write a throwaway config pointing everything at unprivileged paths and pass `-config`:

```bash
cat > /tmp/dc.yaml <<'YAML'
listen: ":5353"
persistence: { db_path: "/tmp/dc-cache.db" }
stats: { socket_path: "/tmp/dc.sock" }
web: { listen: ":8054" }
YAML
./dns-cache -config /tmp/dc.yaml
dig @127.0.0.1 -p 5353 google.com A
```

Other subcommands: `-stats` (reads JSON from the Unix socket of a running server), `-flush` (deletes all rows from the SQLite cache), `-version`.

## Architecture

Package layout is a deliberate one-way dependency: `cache/`, `stats/`, and `web/` are libraries that know nothing about the server; package `main` (`main.go`, `config.go`, `handler.go`, `resolver.go`, `refresher.go`, `persistence.go`) wires them together. Keep it that way — `cache` in particular must not import DNS-server or SQLite concerns.

`stats.Store` is the seam that lets both the socket server and the web dashboard read the cache: any type with `Stats()`, `Config()` and `ForEach()` satisfies it, and `stats.BuildSnapshot()` is the single shared snapshot builder used by both.

### Request path (`handler.go`)

1. Key the question as `<fqdn>:<qtype>` via `cache.Key()` — always build keys with that function, never by hand.
2. Cache hit → `cache.CopyAndSetTTL()` rewrites TTLs on a **copy** (the stored `*dns.Msg` is never mutated; the OPT record in Extra is skipped) → reply. A hit past TTL (allowed while the answer is younger than `max_age`) is answered immediately with TTL 30s and re-resolved in the background through the same `singleflight` group, with a fresh query message (the client's must not be used after the handler returns).
3. Miss → `Resolver.Resolve()` tries upstreams in random permutation order, first success wins, and aggregates per-upstream errors into one message. Concurrent misses on the same key are coalesced with `singleflight` (clients often send the same query twice within milliseconds); the shared `*dns.Msg` is copied per caller because `respond()` mutates it.
4. Upstream failure → `cache.GetStale()` serves an entry expired less than 24h ago with a 30s TTL (RFC 8767), else SERVFAIL.

### Cache invariants (`cache/`)

- Entries are **immutable once stored**, except `HitCount`, `LastHitAt` and `UsedDays`, which are read and written with `sync/atomic` from multiple goroutines. A refresh calls `Set()` to replace the whole `*Entry`, inheriting those three from the old one; do not mutate other fields in place.
- `UsedDays` packs the local day of last use (high 32 bits) and a 32-day bitmask (low 32 bits) in one `uint64`, updated with a CAS loop so day and mask never go out of sync. `Get()` records recency and the usage day on **every** request for an existing key, including misses on entries too old to serve: a domain reopened after hours is always a miss, and counting only hits would never make it usual.
- `cache.Config.Usual()` is the single definition of a usual domain (used on ≥ `keep_min_days` of the last `keep_window_days`); the refresher, the stats and anything new must go through it.
- `Set()` clamps the upstream TTL to `[ttl_min, ttl_max]`, and for answer-less responses takes `min(SOA TTL, SOA MINIMUM)` (RFC 2308 §5 — MINIMUM alone is 86400 on Route53 domains, a full day of negative caching), defaulting to 60s.
- Eviction is sampled, not a true LRU: `evictOne()` scans at most 8 entries, dropping a long-expired one immediately if it sees one, otherwise the oldest `LastHitAt` in the sample. It runs under the write lock, so it must stay O(small).
- `Snapshot()` copies the map (pointers, not entries) so long-running scans don't hold the lock; `ForEach()` holds `RLock` for its whole run, so callers must keep the callback cheap and must not call back into the cache.

### Background loops

- **`persistLoop`** (main.go, 30s): writes, in one `SaveBatch` transaction, only the entries changed since the previous save (`StoredAt` or `LastHitAt` newer than the last save start). This is the only writer — nothing is flushed at shutdown, so up to 30s of changes can be lost. Shutdown order matters: `close(persistStop)` → `persistWg.Wait()` → `p.Close()`, so the goroutine never touches a closed DB (a past bug).
- **`cleanupLoop`** (hourly): deletes rows older than `max(cleanup_after_hours, keep_window_days)` — never shorter than the usage window, or the history that decides usual domains would be deleted.
- **`Refresher`** (`refresher.go`, `refresh_interval`): keeps usual domains fresh. `selectCandidates()` skips non-usual entries entirely (they expire and are misses on return — keeping them warm would cost upstream queries for names seen once), and refreshes usual ones before the answer gets older than `max(CachedTTL, max_age)`, not before the TTL: until then the handler serves them anyway. The prefetch window is `max(RefreshThreshold()*life, refresh_interval*1.5)`, capped at the life; `RefreshThreshold()` scales 10–30% with hit count. Each cycle is bounded at 500 entries with 5 concurrent resolves, sorted overdue-first then by hit count (after a restart all reloaded usual entries are overdue). Failed refreshes get exponential backoff (state lives in the Refresher, not the Entry). A non-`NOERROR` upstream reply keeps the old entry rather than poisoning the cache. `pruneUnused()` deletes expired entries not requested for longer than the usage window — before that they are kept, expired, because they carry the usage history.

### Persistence (`persistence.go`)

SQLite via `modernc.org/sqlite` (pure Go, no cgo — keeps the binary static). WAL + `synchronous=NORMAL`, and `MaxOpenConns(1)` because the driver plus a single-writer workload makes a connection pool pointless. Responses are stored as packed wire-format blobs. `LoadAll()` reconstructs `ExpiresAt` from `cached_ttl` (the TTL the cache actually applied after clamping, not the upstream's) and loads every row requested within the usage window **including expired ones** — after a night with the machine off everything is expired, and skipping them (as an earlier version did) wiped the usual set; the refresher re-resolves the usual ones in its first cycles. `last_hit_at` and `used_days` are persisted too; rows written before `used_days` existed get it seeded from `last_hit_at`. Columns are added to existing DBs via `addColumnIfMissing`, since `CREATE TABLE IF NOT EXISTS` won't touch an existing table.

### Config (`config.go`)

`LoadConfig()` starts from `DefaultConfig()`, unmarshals YAML on top, then `normalize()` clamps values the code can't use (`keep_window_days` to [1, 32] because of the bitmask, `keep_min_days` to [1, window], `max_age` ≥ 0) with a `[config]` log instead of refusing to start. Config files can be partial and every key has a working default. An empty `db_path`, `socket_path`, or `web.listen` disables that subsystem entirely — check for the empty string when adding new optional components. See `dns-cache.yaml.example`.

## Conventions

- Log lines are tagged with a bracketed subsystem prefix: `[config]`, `[persist]`, `[refresher]`, `[stale]`, `[query]`, `[web]`. Match the existing prefix when adding logs to a file. `[query]` is logged only for client misses (not background refreshes), so counting those lines counts misses.
- Code comments and commit messages in this repo are written in Italian; keep new ones consistent with the file you're editing.
- The `dns-cache` binary in the repo root is build output (gitignored), not a tracked file.
