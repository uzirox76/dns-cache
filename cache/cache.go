package cache

import (
	"math"
	"sync"
	"sync/atomic"
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
	config  Config

	hits         uint64
	misses       uint64
	staleServes  uint64
	errs         uint64
	totalQueries uint64
	qpsCount     uint64
	qpsBase      time.Time
}

func New(cfg Config) *Cache {
	return &Cache{
		entries: make(map[string]*Entry),
		config:  cfg,
		qpsBase: time.Now(),
	}
}

func (c *Cache) Get(key string) (*Entry, bool) {
	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	// Una entry scaduta e' un miss: si rinterroga l'upstream. Lo stale
	// serving vale solo quando l'upstream fallisce, via GetStale (RFC 8767).
	if !ok || e.IsExpired() {
		// La entry c'era ma era scaduta: il client questo nome l'ha chiesto
		// lo stesso, quindi si aggiorna la recency (non HitCount, che conta
		// le risposte servite dalla cache). E' su LastHitAt che il refresher
		// decide chi vale la pena tenere caldo: senza questo aggiornamento un
		// dominio con TTL corto uscirebbe dal set caldo proprio perche'
		// scade sempre prima del tick successivo.
		if ok {
			atomic.StoreInt64(&e.LastHitAt, time.Now().UnixNano())
		}
		atomic.AddUint64(&c.misses, 1)
		return nil, false
	}

	atomic.AddUint64(&e.HitCount, 1)
	atomic.StoreInt64(&e.LastHitAt, time.Now().UnixNano())
	atomic.AddUint64(&c.hits, 1)
	return e, true
}

// GetStale ritorna una entry scaduta da servire quando tutti gli upstream
// hanno fallito (RFC 8767). Ritorna false se lo stale serving e' disattivato.
func (c *Cache) GetStale(key string) (*Entry, bool) {
	if !c.config.StaleServing {
		return nil, false
	}

	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	atomic.AddUint64(&e.HitCount, 1)
	atomic.StoreInt64(&e.LastHitAt, time.Now().UnixNano())
	atomic.AddUint64(&c.staleServes, 1)
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
		QuestionName: dns.CanonicalName(qname),
		QuestionType: qtype,
		Response:     resp.Copy(),
		StoredAt:     now,
		OriginalTTL:  originalTTL,
		CachedTTL:    ttl,
		ExpiresAt:    now.Add(ttl),
		HitCount:     1,
		LastHitAt:    now.UnixNano(),
	}

	key := e.Key()

	c.mu.Lock()
	// Un refresh sostituisce la entry ma non e' un accesso del client: hit
	// count e recency si ereditano da quella vecchia. Azzerandoli ogni
	// refresh raffredderebbe il dominio, RefreshThreshold() tornerebbe al
	// 10% proprio sulle entry piu' calde e LastHitAt segnerebbe l'ultimo
	// refresh invece dell'ultima richiesta vera.
	if old, replacing := c.entries[key]; replacing {
		e.HitCount = atomic.LoadUint64(&old.HitCount)
		e.LastHitAt = atomic.LoadInt64(&old.LastHitAt)
	} else if len(c.entries) >= c.config.MaxEntries {
		// Si sfratta solo quando la chiave e' nuova: sostituire una entry
		// esistente non fa crescere la mappa.
		c.evictOne()
	}
	c.entries[key] = e
	c.mu.Unlock()
}

func (c *Cache) evictOne() {
	const sampleSize = 8
	now := time.Now()
	n := len(c.entries)
	var oldestKey string
	var oldest int64 = math.MaxInt64
	count := 0

	for k, v := range c.entries {
		if now.After(v.ExpiresAt) && now.After(v.ExpiresAt.Add(24*time.Hour)) {
			delete(c.entries, k)
			return
		}
		lha := atomic.LoadInt64(&v.LastHitAt)
		if lha < oldest {
			oldest = lha
			oldestKey = k
		}
		count++
		if count >= sampleSize || count >= n {
			break
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
	hits := atomic.LoadUint64(&c.hits)
	misses := atomic.LoadUint64(&c.misses)

	var ratio float64
	total := hits + misses
	if total > 0 {
		ratio = float64(hits) / float64(total) * 100
	}

	qps := atomic.LoadUint64(&c.qpsCount)
	elapsed := time.Since(c.qpsBase).Seconds()
	var avgQPS float64
	if elapsed > 0 {
		avgQPS = float64(qps) / elapsed
	}

	c.mu.RLock()
	n := len(c.entries)
	c.mu.RUnlock()

	return Stats{
		Entries:      n,
		MaxEntries:   c.config.MaxEntries,
		Hits:         hits,
		Misses:       misses,
		StaleServes:  atomic.LoadUint64(&c.staleServes),
		Errors:       atomic.LoadUint64(&c.errs),
		HitRatio:     ratio,
		TotalQueries: atomic.LoadUint64(&c.totalQueries),
		QPSHistory:   nil,
		AvgQPS:       avgQPS,
	}
}

func (c *Cache) IncrQueries() {
	atomic.AddUint64(&c.totalQueries, 1)
	atomic.AddUint64(&c.qpsCount, 1)
}

func (c *Cache) IncrErrors() {
	atomic.AddUint64(&c.errs, 1)
}
