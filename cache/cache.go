package cache

import (
	"sync"
	"time"

	"github.com/miekg/dns"
)

type Stats struct {
	Entries     int     `json:"entries"`
	Hits        uint64  `json:"hits"`
	Misses      uint64  `json:"misses"`
	StaleServes uint64  `json:"stale_serves"`
	Errors      uint64  `json:"errors"`
	HitRatio    float64 `json:"hit_ratio"`
}

type Config struct {
	TTLMin       time.Duration
	TTLMax       time.Duration
	MaxEntries   int
	StaleServing bool
}

type Cache struct {
	mu      sync.RWMutex
	entries map[string]*Entry
	hits    uint64
	misses  uint64
	stale   uint64
	errs    uint64
	config  Config
}

func New(cfg Config) *Cache {
	return &Cache{
		entries: make(map[string]*Entry),
		config:  cfg,
	}
}

func (c *Cache) Get(key string) (*Entry, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return nil, false
	}

	if e.IsExpired() {
		if !c.config.StaleServing {
			c.mu.Lock()
			c.misses++
			c.mu.Unlock()
			return nil, false
		}
		c.mu.Lock()
		e.HitCount++
		e.LastHitAt = time.Now()
		c.stale++
		c.mu.Unlock()
		return e, true
	}

	c.mu.Lock()
	e.HitCount++
	e.LastHitAt = time.Now()
	c.hits++
	c.mu.Unlock()
	return e, true
}

func (c *Cache) GetStale(key string) (*Entry, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return e, true
}

func (c *Cache) Set(qname string, qtype uint16, resp *dns.Msg, originalTTL uint32) {
	if len(resp.Answer) == 0 {
		// NXDOMAIN/NODATA — extract TTL from SOA (RFC 2308)
		for _, rr := range resp.Ns {
			if soa, ok := rr.(*dns.SOA); ok {
				originalTTL = soa.Minttl
				break
			}
		}
		if originalTTL == 0 {
			originalTTL = 60
		}
	}

	ttl := time.Duration(originalTTL) * time.Second
	if ttl < c.config.TTLMin {
		ttl = c.config.TTLMin
	}
	if ttl > c.config.TTLMax {
		ttl = c.config.TTLMax
	}

	now := time.Now()
	e := &Entry{
		QuestionName: qname,
		QuestionType: qtype,
		Response:     resp.Copy(),
		StoredAt:     now,
		OriginalTTL:  originalTTL,
		CachedTTL:    ttl,
		ExpiresAt:    now.Add(ttl),
		HitCount:     1,
		LastHitAt:    now,
	}

	c.mu.Lock()
	if len(c.entries) >= c.config.MaxEntries {
		c.evictOne()
	}
	c.entries[e.Key()] = e
	c.mu.Unlock()
}

func (c *Cache) evictOne() {
	var oldestKey string
	var oldest time.Time
	now := time.Now()
	for k, v := range c.entries {
		if v.IsExpired() && now.After(v.ExpiresAt.Add(24*time.Hour)) {
			delete(c.entries, k)
			return
		}
		if oldestKey == "" || v.LastHitAt.Before(oldest) {
			oldestKey = k
			oldest = v.LastHitAt
		}
	}
	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

func (c *Cache) Delete(key string) {
	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

func (c *Cache) Config() Config {
	return c.config
}

func (c *Cache) ForEach(fn func(key string, e *Entry) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for k, v := range c.entries {
		if !fn(k, v) {
			break
		}
	}
}

func (c *Cache) Snapshot() map[string]*Entry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	m := make(map[string]*Entry, len(c.entries))
	for k, v := range c.entries {
		m[k] = v
	}
	return m
}

func (c *Cache) LoadEntry(e *Entry) {
	c.mu.Lock()
	c.entries[e.Key()] = e
	c.mu.Unlock()
}

func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var ratio float64
	total := c.hits + c.misses
	if total > 0 {
		ratio = float64(c.hits) / float64(total) * 100
	}
	return Stats{
		Entries:     len(c.entries),
		Hits:        c.hits,
		Misses:      c.misses,
		StaleServes: c.stale,
		Errors:      c.errs,
		HitRatio:    ratio,
	}
}

func (c *Cache) IncrErrors() {
	c.mu.Lock()
	c.errs++
	c.mu.Unlock()
}
