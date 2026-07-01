package cache

import (
	"sync"
	"time"

	"github.com/miekg/dns"
)

type Stats struct {
	Entries      int     `json:"entries"`
	MaxEntries   int     `json:"max_entries"`
	Hits         uint64  `json:"hits"`
	Misses       uint64  `json:"misses"`
	StaleServes  uint64  `json:"stale_serves"`
	Errors       uint64  `json:"errors"`
	HitRatio     float64 `json:"hit_ratio"`
	TotalQueries uint64  `json:"total_queries"`
	QPSHistory   []int   `json:"qps_history"`
	AvgQPS       float64 `json:"avg_qps"`
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

	totalQueries uint64
	qpsRing      [60]int
	qpsBase      int64
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
		for _, rr := range resp.Ns {
			if soa, ok := rr.(*dns.SOA); ok {
				originalTTL = soa.Minttl
				break
			}
		}
	}
	if originalTTL == 0 {
		originalTTL = 60
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
		vc := *v
		m[k] = &vc
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

	var sum int
	for _, v := range c.qpsRing {
		sum += v
	}
	var avgQPS float64
	window := 60
	if c.qpsBase > 0 {
		elapsed := time.Now().Unix() - c.qpsBase
		if elapsed > 0 && int(elapsed) < window {
			window = int(elapsed)
		}
	}
	if window > 0 {
		avgQPS = float64(sum) / float64(window)
	}

	qpsHist := make([]int, len(c.qpsRing))
	copy(qpsHist, c.qpsRing[:])

	return Stats{
		Entries:      len(c.entries),
		MaxEntries:   c.config.MaxEntries,
		Hits:         c.hits,
		Misses:       c.misses,
		StaleServes:  c.stale,
		Errors:       c.errs,
		HitRatio:     ratio,
		TotalQueries: c.totalQueries,
		QPSHistory:   qpsHist,
		AvgQPS:       avgQPS,
	}
}

func (c *Cache) IncrQueries() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trackQPS()
}

func (c *Cache) trackQPS() {
	now := time.Now().Unix()
	c.totalQueries++

	if c.qpsBase == 0 {
		c.qpsBase = now
	}

	sec := int(now - c.qpsBase)
	if sec >= 60 {
		shift := sec - 59
		if shift >= 60 {
			c.qpsBase = now
			c.qpsRing = [60]int{}
			c.qpsRing[0] = 1
			return
		}
		copy(c.qpsRing[:], c.qpsRing[shift:])
		for i := 60 - shift; i < 60; i++ {
			c.qpsRing[i] = 0
		}
		c.qpsBase += int64(shift)
		sec = 59
	} else if sec < 0 {
		c.qpsBase = now
		c.qpsRing = [60]int{}
		c.qpsRing[0] = 1
		return
	}
	c.qpsRing[sec]++
}

func (c *Cache) IncrErrors() {
	c.mu.Lock()
	c.errs++
	c.mu.Unlock()
}
