# dns-cache

A lightweight, high-performance local DNS caching server for Linux. Written in Go.

## Features

- **Local DNS server** — listens on `:53`, intercepts all DNS queries
- **In-memory cache** — thread-safe (`sync.RWMutex`), instant lookups
- **TTL-aware** — respects upstream TTLs with configurable min/max bounds
- **Stale serving** — serves expired entries if upstream is unreachable (RFC 8767)
- **Predictive prefetch** — hot domains (more hits) are refreshed earlier (up to 30% of TTL)
- **Background refresher** — periodically refreshes near-expiry entries, max 5 concurrent
- **SQLite persistence** — survives restarts, WAL mode, periodic cleanup
- **Upstream fallback** — tries multiple resolvers (1.1.1.1, 9.9.9.9, etc.) in random order
- **NXDOMAIN caching** — caches negative responses with SOA MinTTL (RFC 2308)
- **LRU eviction** — drops least recently used entries when cache is full
- **CLI stats** — `dns-cache -stats` for live JSON stats via Unix socket
- **Web dashboard** — `http://localhost:8080` with auto-refresh HTML + JSON API
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
persistence:
  db_path: "/var/cache/dns-cache/cache.db"
  cleanup_after_hours: 48
stats:
  socket_path: "/var/run/dns-cache.sock"
web:
  listen: ":8080"
```

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
# or open http://localhost:8080
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

- **`cache/`** — In-memory store with `sync.RWMutex`, TTL checking, LRU eviction, hit tracking
- **`resolver.go`** — Forwards queries to upstream servers with random-order fallback
- **`handler.go`** — DNS request handler: cache → forward → save → respond
- **`persistence.go`** — SQLite backend (WAL mode) for cache durability across restarts
- **`refresher.go`** — Background goroutine that preemptively refreshes near-expiry entries
- **`stats/`** — Unix socket stats server + shared `BuildSnapshot()` for CLI and web
- **`web/`** — HTTP server with HTML dashboard and `/api/stats` JSON endpoint

### Cache lifecycle

1. Query arrives → check in-memory cache
2. **Cache hit** → respond immediately with adjusted TTL
3. **Cache miss** → forward to upstream, store in cache + SQLite, respond
4. **Upstream error** → serve stale entry if available, otherwise SERVFAIL
5. **Background** → every 30s, refresh entries nearing expiry (hotter = earlier refresh)

## Performance

- **Cache hits**: ~1000+ qps (single core, pure in-memory lookup)
- **Cache misses**: bounded by upstream latency (~30-60ms typical)
- **Memory**: ~6MB baseline + ~1KB per cached entry
- **Concurrency**: goroutine-per-request, zero contention on cache reads

## License

MIT
