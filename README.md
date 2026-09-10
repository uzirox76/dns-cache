# dns-cache

A lightweight, high-performance local DNS caching server for Linux. Written in Go.

## Features

- **Local DNS server** — listens on `:53`, intercepts all DNS queries
- **In-memory cache** — thread-safe (`sync.RWMutex`), instant lookups
- **TTL-aware** — respects upstream TTLs with configurable min/max bounds
- **Always-fresh usual domains** — domains you use on at least 2 of the last 7 days are refreshed in the background even while idle, and survive restarts
- **Serve while refreshing** — past its TTL, an answer up to 30 minutes old is served instantly and re-resolved in the background at the same time
- **Stale serving** — serves expired entries (up to 24h) if upstream is unreachable (RFC 8767)
- **Predictive prefetch** — hot domains (more hits) are refreshed earlier
- **Query coalescing** — identical concurrent queries share a single upstream request
- **SQLite persistence** — survives restarts, WAL mode, periodic cleanup
- **Upstream fallback** — tries multiple resolvers (1.1.1.1, 9.9.9.9, etc.) in random order
- **NXDOMAIN caching** — caches negative responses for min(SOA TTL, SOA MINIMUM) (RFC 2308)
- **LRU eviction** — drops least recently used entries when cache is full
- **CLI stats** — `dns-cache -stats` for live JSON stats via Unix socket
- **Web dashboard** — `http://localhost:8053` with auto-refresh HTML + JSON API
- **systemd integration** — service file included, `CAP_NET_BIND_SERVICE` for port 53
- **Zero dependencies** — single static binary, ~13MB

## Quick start

### 1. Build

```bash
git clone https://github.com/uzirox76/dns-cache.git
cd dns-cache
go build -o dns-cache -ldflags="-s -w" .
```

### 2. Configure

Edit `/etc/dns-cache.yaml`:

```yaml
listen: ":53"
upstreams:
  - "1.1.1.1:53"
  - "9.9.9.9:53"
cache:
  ttl_min: 60
  ttl_max: 86400
  refresh_interval: 30
  stale_serving: true
  max_entries: 10000
  max_age: 1800          # serve past TTL (refreshing in background) up to this age, seconds
  keep_min_days: 2       # usual domains: used on at least this many days...
  keep_window_days: 7    # ...of the last this many days
persistence:
  db_path: "/var/cache/dns-cache/cache.db"
  cleanup_after_hours: 48
stats:
  socket_path: "/var/run/dns-cache.sock"
web:
  listen: ":8053"
```

All keys are optional: missing ones take the defaults shown above.

### 3. Install as a systemd service

```bash
sudo mkdir -p /var/cache/dns-cache /var/run/dns-cache
sudo install -m 755 dns-cache /usr/local/bin/dns-cache
sudo install -m 644 dns-cache.yaml.example /etc/dns-cache.yaml
sudo install -m 644 dns-cache.service /etc/systemd/system/dns-cache.service
sudo systemctl daemon-reload
sudo systemctl enable --now dns-cache
```

### 4. Make it the system DNS resolver (if not using systemd-resolved)

```bash
echo "nameserver 127.0.0.1" | sudo tee /etc/resolv.conf
```

Or disable systemd-resolved first:

```bash
sudo systemctl stop systemd-resolved
sudo systemctl disable systemd-resolved
sudo rm -f /etc/resolv.conf
echo "nameserver 127.0.0.1" | sudo tee /etc/resolv.conf
```

### 5. Test

```bash
dig @127.0.0.1 google.com A +short
dns-cache -stats
# or open http://localhost:8053
```

## Commands

| Command | Description |
|---------|-------------|
| `dns-cache` | Start the server |
| `dns-cache -stats` | Show live statistics from running server |
| `dns-cache -flush` | Clear the persistent cache database |
| `dns-cache -version` | Show version |
| `dns-cache -config /path` | Use a custom config file |

## Architecture

```
┌─────────────┐    :53/udp     ┌───────────────────┐
│  browser /   │ ── DNS query → │   dns-cache       │
│  system      │ ←── response ─ │   (server)        │
└─────────────┘                └───────┬───────────┘
                                       │
                            cache hit? │ TTL expired?
                            ┌──────────┴──────────┐
                            │                     │
                         serve from           forward to
                         cache                upstream
                                             1.1.1.1:53
                                             9.9.9.9:53
```

### Components

- **`cache/`** — In-memory store with `sync.RWMutex`, TTL checking, LRU eviction, hit and usage-day tracking
- **`resolver.go`** — Forwards queries to upstream servers with random-order fallback
- **`handler.go`** — DNS request handler: cache → forward → save → respond
- **`persistence.go`** — SQLite backend (WAL mode) for cache durability across restarts
- **`refresher.go`** — Background goroutine that keeps usual domains fresh
- **`stats/`** — Unix socket stats server + shared `BuildSnapshot()` for CLI and web
- **`web/`** — HTTP server with HTML dashboard and `/api/stats` JSON endpoint

### Cache lifecycle

1. Query arrives → check in-memory cache
2. **Cache hit** → respond immediately with the remaining TTL
3. **Past TTL, answer younger than `max_age`** → respond immediately (TTL 30s) and re-resolve in the background
4. **Cache miss** → forward to upstream (identical concurrent queries share one request), store in cache + SQLite, respond
5. **Upstream error** → serve stale entry (up to 24h) if available, otherwise SERVFAIL
6. **Background** → every 30s, refresh usual domains (used on `keep_min_days` of the last `keep_window_days` days) before their answer gets older than max(TTL, `max_age`)

## Performance

- **Cache hits**: ~1000+ qps (single core, pure in-memory lookup)
- **Cache misses**: bounded by upstream latency (~30-60ms typical)
- **Memory**: ~6MB baseline + ~1KB per cached entry
- **Concurrency**: goroutine-per-request, zero contention on cache reads

## License

MIT
