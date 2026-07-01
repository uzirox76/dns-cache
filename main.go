package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/miekg/dns"

	"dns-cache/cache"
	"dns-cache/stats"
	"dns-cache/web"
)

var version = "0.1.0"

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: dns-cache [options]\n")
		fmt.Fprintf(os.Stderr, "       dns-cache -stats\n")
		fmt.Fprintf(os.Stderr, "       dns-cache -flush\n\n")
		fmt.Fprintf(os.Stderr, "A local DNS cache server with persistence.\n\n")
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	configPath := flag.String("config", "/etc/dns-cache.yaml", "config file path")
	showVersion := flag.Bool("version", false, "show version")
	statsCmd := flag.Bool("stats", false, "show server stats")
	flushCmd := flag.Bool("flush", false, "flush all cached entries")
	flag.Parse()

	if *showVersion {
		fmt.Println("dns-cache v" + version)
		return
	}

	if *statsCmd {
		runStats(*configPath)
		return
	}

	if *flushCmd {
		runFlush(*configPath)
		return
	}

	runServer(*configPath)
}

func loadConfigOrDefault(path string) *Config {
	cfg, err := LoadConfig(path)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[config] %s not found, using defaults", path)
			return DefaultConfig()
		}
		log.Fatalf("[config] load error: %v", err)
	}
	log.Printf("[config] loaded from %s", path)
	return cfg
}

func runServer(configPath string) {
	cfg := loadConfigOrDefault(configPath)

	log.Printf("[start] dns-cache v%s", version)
	log.Printf("[start] listen %s, upstreams %v", cfg.Listen, cfg.Upstreams)

	cacheCfg := cache.Config{
		TTLMin:       cfg.CacheTTLMin(),
		TTLMax:       cfg.CacheTTLMax(),
		MaxEntries:   cfg.CacheCfg.MaxEntries,
		StaleServing: cfg.CacheCfg.StaleServing,
	}
	c := cache.New(cacheCfg)

	if cfg.PersistCfg.DBPath != "" {
		p, err := NewPersistence(cfg.PersistCfg.DBPath)
		if err != nil {
			log.Fatalf("[persist] init: %v", err)
		}
		defer p.Close()

		entries, err := p.LoadAll()
		if err != nil {
			log.Printf("[persist] load error: %v", err)
		} else {
			for _, e := range entries {
				c.LoadEntry(e)
			}
			log.Printf("[persist] loaded %d entries from %s", len(entries), cfg.PersistCfg.DBPath)
		}

		go persistLoop(c, p)

		cleanupAfter := time.Duration(cfg.PersistCfg.CleanupAfter) * time.Hour
		if cleanupAfter > 0 {
			go cleanupLoop(p, cleanupAfter)
		}
	}

	resolver := NewResolver(cfg.Upstreams, 5*time.Second)

	handler := NewDNSHandler(c, resolver)

	refresher := NewRefresher(c, resolver, cfg.CacheRefreshInterval())
	refresher.Start()
	defer refresher.Stop()

	if cfg.StatsCfg.SocketPath != "" {
		statsSrv := stats.New(cfg.StatsCfg.SocketPath, c)
		if err := statsSrv.Start(); err != nil {
			log.Printf("[stats] start error: %v", err)
		} else {
			defer statsSrv.Stop()
			log.Printf("[stats] listening on %s", cfg.StatsCfg.SocketPath)
		}
	}

	if cfg.WebCfg.Listen != "" {
		webSrv := web.New(cfg.WebCfg.Listen, c)
		if err := webSrv.Start(); err != nil {
			log.Printf("[web] start error: %v", err)
		} else {
			defer webSrv.Stop()
		}
	}

	server := &dns.Server{
		Addr:    cfg.Listen,
		Net:     "udp",
		Handler: handler,
		UDPSize: 1232,
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	log.Printf("[ready] dns-cache listening on %s", cfg.Listen)

	select {
	case err := <-errCh:
		if err != nil {
			log.Fatalf("[server] %v", err)
		}
	case <-sigCh:
		log.Print("[shutdown] signal received, stopping...")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.ShutdownContext(ctx)
	}

	log.Print("[shutdown] server stopped")
}

func persistLoop(c *cache.Cache, p *Persistence) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		snapshot := c.Snapshot()
		var saved int
		for _, e := range snapshot {
			if e.HitCount > 0 {
				if err := p.SaveEntry(e); err != nil {
					log.Printf("[persist] save error: %v", err)
				}
				saved++
			}
		}
		if saved > 0 {
			log.Printf("[persist] saved %d entries", saved)
		}
	}
}

func cleanupLoop(p *Persistence, after time.Duration) {
	for {
		time.Sleep(1 * time.Hour)
		if _, err := p.Cleanup(after); err != nil {
			log.Printf("[cleanup] error: %v", err)
		}
	}
}

func getSocketPath(configPath string) string {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return "/var/run/dns-cache.sock"
	}
	return cfg.StatsCfg.SocketPath
}

func runStats(configPath string) {
	addr := getSocketPath(configPath)
	log.Printf("[stats] connecting to %s", addr)

	conn, err := net.DialTimeout("unix", addr, 3*time.Second)
	if err != nil {
		log.Fatalf("[stats] connect: %v\nMake sure the server is running.", err)
	}
	defer conn.Close()

	buf := make([]byte, 65536)
	n, err := conn.Read(buf)
	if err != nil {
		log.Fatalf("[stats] read: %v", err)
	}

	os.Stdout.Write(buf[:n])
}

func runFlush(configPath string) {
	cfg := loadConfigOrDefault(configPath)
	if cfg.PersistCfg.DBPath != "" {
		p, err := NewPersistence(cfg.PersistCfg.DBPath)
		if err != nil {
			log.Fatalf("[flush] open db: %v", err)
		}
		defer p.Close()

		if _, err := p.db.Exec(`DELETE FROM cache_entries`); err != nil {
			log.Fatalf("[flush] delete: %v", err)
		}
		fmt.Println("Cache flushed.")
	} else {
		fmt.Println("No persistence configured. Nothing to flush.")
	}
}
