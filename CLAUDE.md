# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project

`dns-cache` — a local DNS caching server (Go, single static binary) that listens on `:53`, forwards misses to upstream resolvers, persists the cache to SQLite, and exposes stats over a Unix socket and an HTTP dashboard.

## Commands

```bash
make build                    # go build -o dns-cache -ldflags="-s -w" .
go vet ./...
gofmt -l .
sudo make install             # binary + /etc/dns-cache.yaml + systemd unit; runs daemon-reload
```

Tests live in `cache/cache_test.go` (sliding stats window, `Set()` hit-count inheritance). `go test ./...`, or `go test ./cache -run TestName -v` for a single one.

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

`stats.Store` is the seam that lets both the socket server and the web dashboard read the cache: any type with `Stats()` and `ForEach()` satisfies it, and `stats.BuildSnapshot()` is the single shared snapshot builder used by both.

### Request path (`handler.go`)

1. Key the question as `<fqdn>:<qtype>` via `cache.Key()` — always build keys with that function, never by hand.
2. Cache hit → `cache.CopyAndSetTTL()` rewrites TTLs on a **copy** (the stored `*dns.Msg` is never mutated; the OPT record in Extra is skipped) → reply.
3. Miss → `Resolver.Resolve()` tries upstreams in random permutation order, first success wins, and aggregates per-upstream errors into one message.
4. Upstream failure → `cache.GetStale()` serves the expired entry with a 30s TTL (RFC 8767), else SERVFAIL.

### Cache invariants (`cache/`)

- Entries are **immutable once stored**, except `HitCount` and `LastHitAt`, which are read and written with `sync/atomic` from multiple goroutines. A refresh calls `Set()` to replace the whole `*Entry`; do not mutate fields in place.
- `Set()` clamps the upstream TTL to `[ttl_min, ttl_max]`, and for answer-less responses pulls the TTL from the SOA `MinTTL` (RFC 2308 negative caching), defaulting to 60s.
- Eviction is sampled, not a true LRU: `evictOne()` scans at most 8 entries, dropping a long-expired one immediately if it sees one, otherwise the oldest `LastHitAt` in the sample. It runs under the write lock, so it must stay O(small).
- `Snapshot()` copies the map (pointers, not entries) so long-running scans don't hold the lock; `ForEach()` holds `RLock` for its whole run, so callers must keep the callback cheap and must not call back into the cache.

### Background loops

- **`persistLoop`** (main.go, 30s): `ForEach` collects entries with `HitCount > 0` and writes them in one `SaveBatch` transaction. This is the only writer — nothing is flushed at shutdown, so up to 30s of entries can be lost. Shutdown order matters: `close(persistStop)` → `persistWg.Wait()` → `p.Close()`, so the goroutine never touches a closed DB (a past bug).
- **`cleanupLoop`** (hourly): deletes rows older than `cleanup_after_hours`.
- **`Refresher`** (`refresher.go`, `refresh_interval`): predictive prefetch. `Entry.RefreshThreshold()` returns 10–30% of TTL scaled by hit count, so hot domains refresh earlier. Candidates are gated on recency: only entries a client asked for in the last hour (`refreshMaxIdle`), because a refresh keeps an entry alive forever and without the gate nothing ever leaves the cache on its own. The refresh window is `max(pct*TTL, refresh_interval*1.5)` — a window narrower than the tick means the entry expires before the prefetch ever fires. Each cycle is bounded at 500 entries with 5 concurrent resolves, sorted expired-first then by hit count. Entries that fail a refresh get exponential backoff (state lives in the Refresher, not the Entry). A non-`NOERROR` upstream reply keeps the old entry rather than poisoning the cache. With stale serving on, it prunes entries expired more than 24h.

### Persistence (`persistence.go`)

SQLite via `modernc.org/sqlite` (pure Go, no cgo — keeps the binary static). WAL + `synchronous=NORMAL`, and `MaxOpenConns(1)` because the driver plus a single-writer workload makes a connection pool pointless. Responses are stored as packed wire-format blobs. `LoadAll()` reconstructs `ExpiresAt` from `cached_ttl` (the TTL the cache actually applied after clamping, not the upstream's) and **skips entries already expired** — reloading them just filled the cache and the refresher queue with rows that need re-resolving anyway. `last_hit_at` is persisted too: without it the recency gate reads the last *refresh* time and never filters anything after a restart. Columns are added to existing DBs via `addColumnIfMissing`, since `CREATE TABLE IF NOT EXISTS` won't touch an existing table.

### Config (`config.go`)

`LoadConfig()` starts from `DefaultConfig()` and unmarshals YAML on top, so config files can be partial and every key has a working default. An empty `db_path`, `socket_path`, or `web.listen` disables that subsystem entirely — check for the empty string when adding new optional components. See `dns-cache.yaml.example`.

## Conventions

- Log lines are tagged with a bracketed subsystem prefix: `[config]`, `[persist]`, `[refresher]`, `[stale]`, `[query]`, `[web]`. Match the existing prefix when adding logs to a file.
- Code comments and commit messages in this repo are written in Italian; keep new ones consistent with the file you're editing.
- The `dns-cache` binary in the repo root is build output (gitignored), not a tracked file.
