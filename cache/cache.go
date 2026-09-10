package cache

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"
)

type Stats struct {
	Entries    int    `json:"entries"`
	MaxEntries int    `json:"max_entries"`
	Hits       uint64 `json:"hits"`
	// LateHits e' la parte di Hits servita oltre il TTL (entro max_age),
	// mentre l'handler rinfrescava la entry in background.
	LateHits    uint64  `json:"late_hits"`
	Misses      uint64  `json:"misses"`
	StaleServes uint64  `json:"stale_serves"`
	Errors      uint64  `json:"errors"`
	HitRatio    float64 `json:"hit_ratio"`
	// Rapporto sulla finestra scorrevole: HitRatio e' cumulativo dall'avvio,
	// quindi dopo un riavvio resta per un pezzo dominato dal cold start e non
	// dice mai come sta andando adesso.
	HitRatioWindow float64 `json:"hit_ratio_window"`
	WindowQueries  uint64  `json:"window_queries"`
	WindowMinutes  int     `json:"window_minutes"`
	TotalQueries   uint64  `json:"total_queries"`
	QPSHistory     []int   `json:"qps_history"`
	AvgQPS         float64 `json:"avg_qps"`
}

// windowMinutes e' l'ampiezza della finestra scorrevole, un bucket al
// minuto: 15 bucket da 24 byte, e per query un modulo e due atomiche.
const windowMinutes = 15

// windowBucket accumula gli esiti di un singolo minuto. minute e' l'indice
// del minuto a cui appartiene: quando non corrisponde il bucket e' vecchio e
// va riusato.
type windowBucket struct {
	minute int64
	hits   uint64
	misses uint64
}

// staleMaxAge limita lo stale serving quando l'upstream non risponde (la RFC
// 8767 suggerisce da uno a tre giorni). Il limite va detto esplicitamente
// perche' le entry scadute restano in cache per tutta la finestra dei giorni
// d'uso.
const staleMaxAge = 24 * time.Hour

type Config struct {
	TTLMin       time.Duration
	TTLMax       time.Duration
	MaxEntries   int
	StaleServing bool
	// MaxAge: oltre il TTL una entry si serve ancora finche' la risposta ha
	// meno di MaxAge dal fetch, e intanto l'handler la rinfresca in
	// background. 0 = mai oltre il TTL.
	MaxAge time.Duration
	// Domini abituali: chiesti in almeno KeepMinDays degli ultimi
	// KeepWindowDays giorni. Sono quelli che il refresher tiene aggiornati.
	KeepMinDays    int
	KeepWindowDays int
}

// Usual dice se la entry e' di un dominio abituale.
func (cfg Config) Usual(e *Entry, now time.Time) bool {
	return e.DaysUsed(now, cfg.KeepWindowDays) >= cfg.KeepMinDays
}

// KeepWindow e' la finestra dei giorni d'uso come durata: per tanto va tenuto
// lo storico di una entry, anche scaduta.
func (cfg Config) KeepWindow() time.Duration {
	return time.Duration(cfg.KeepWindowDays) * 24 * time.Hour
}

type Cache struct {
	mu      sync.RWMutex
	entries map[string]*Entry
	config  Config

	hits         uint64
	lateHits     uint64
	misses       uint64
	staleServes  uint64
	errs         uint64
	totalQueries uint64
	qpsCount     uint64
	qpsBase      time.Time

	window [windowMinutes]windowBucket
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

	if !ok {
		atomic.AddUint64(&c.misses, 1)
		c.recordWindow(false)
		return nil, false
	}

	// Il client il nome l'ha chiesto, che la entry si possa servire o no:
	// recency e giorni d'uso si aggiornano comunque (HitCount no, conta le
	// risposte servite dalla cache). E' su questi che il refresher decide chi
	// tenere aggiornato: se contassero solo gli hit, un dominio che si riapre
	// dopo ore -- e che quindi finora era sempre un miss -- non diventerebbe
	// mai abituale.
	now := time.Now()
	atomic.StoreInt64(&e.LastHitAt, now.UnixNano())
	e.MarkUsed(now)

	// Oltre il TTL la entry si serve solo finche' la risposta ha meno di
	// MaxAge, e l'handler intanto la rinfresca. Piu' vecchia e' un miss: si
	// rinterroga l'upstream. Lo stale serving vero e proprio vale solo quando
	// l'upstream fallisce, via GetStale (RFC 8767).
	expired := e.IsExpiredAt(now)
	if expired && now.Sub(e.StoredAt) > c.config.MaxAge {
		atomic.AddUint64(&c.misses, 1)
		c.recordWindow(false)
		return nil, false
	}

	atomic.AddUint64(&e.HitCount, 1)
	atomic.AddUint64(&c.hits, 1)
	if expired {
		atomic.AddUint64(&c.lateHits, 1)
	}
	c.recordWindow(true)
	return e, true
}

// GetStale ritorna una entry scaduta da servire quando tutti gli upstream
// hanno fallito (RFC 8767). Ritorna false se lo stale serving e' disattivato
// o se la entry e' scaduta da piu' di staleMaxAge.
func (c *Cache) GetStale(key string) (*Entry, bool) {
	if !c.config.StaleServing {
		return nil, false
	}

	c.mu.RLock()
	e, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok || time.Since(e.ExpiresAt) > staleMaxAge {
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
				// RFC 2308 §5: il TTL negativo e' il minimo fra il campo
				// MINIMUM e il TTL del SOA stesso. Col solo MINIMUM i domini
				// su Route53, che lo mette a 86400, restavano in cache un
				// giorno: un record aggiunto nel frattempo non si vedeva.
				originalTTL = min(soa.Minttl, soa.Hdr.Ttl)
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
		UsedDays:     PackUsedDays(now),
	}

	key := e.Key()

	c.mu.Lock()
	// Un refresh sostituisce la entry ma non e' un accesso del client: hit
	// count, recency e giorni d'uso si ereditano da quella vecchia. Azzerandoli
	// ogni refresh raffredderebbe il dominio, RefreshThreshold() tornerebbe al
	// 10% proprio sulle entry piu' calde, LastHitAt segnerebbe l'ultimo
	// refresh invece dell'ultima richiesta vera, e un dominio abituale
	// smetterebbe di esserlo al primo refresh.
	if old, replacing := c.entries[key]; replacing {
		e.HitCount = atomic.LoadUint64(&old.HitCount)
		e.LastHitAt = atomic.LoadInt64(&old.LastHitAt)
		e.UsedDays = atomic.LoadUint64(&old.UsedDays)
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

// recordWindow segna l'esito nel bucket del minuto corrente.
func (c *Cache) recordWindow(hit bool) {
	minute := time.Now().Unix() / 60
	b := &c.window[minute%windowMinutes]

	// Bucket di un minuto vecchio: va riusato. La CAS fa vincere un solo
	// chiamante, gli altri vedono gia' il minuto nuovo e passano a contare.
	// Nella finestra fra la CAS e gli Store si puo' perdere qualche conteggio
	// di chi stava scrivendo sul bucket vecchio: e' un contatore da cruscotto,
	// non una statistica contabile.
	if got := atomic.LoadInt64(&b.minute); got != minute {
		if atomic.CompareAndSwapInt64(&b.minute, got, minute) {
			atomic.StoreUint64(&b.hits, 0)
			atomic.StoreUint64(&b.misses, 0)
		}
	}

	if hit {
		atomic.AddUint64(&b.hits, 1)
	} else {
		atomic.AddUint64(&b.misses, 1)
	}
}

// windowStats somma i bucket che ricadono nella finestra.
func (c *Cache) windowStats() (hits, misses uint64) {
	cutoff := time.Now().Unix()/60 - windowMinutes
	for i := range c.window {
		b := &c.window[i]
		if atomic.LoadInt64(&b.minute) > cutoff {
			hits += atomic.LoadUint64(&b.hits)
			misses += atomic.LoadUint64(&b.misses)
		}
	}
	return hits, misses
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

	wHits, wMisses := c.windowStats()
	var wRatio float64
	if wTotal := wHits + wMisses; wTotal > 0 {
		wRatio = float64(wHits) / float64(wTotal) * 100
	}

	c.mu.RLock()
	n := len(c.entries)
	c.mu.RUnlock()

	return Stats{
		Entries:        n,
		MaxEntries:     c.config.MaxEntries,
		Hits:           hits,
		LateHits:       atomic.LoadUint64(&c.lateHits),
		Misses:         misses,
		StaleServes:    atomic.LoadUint64(&c.staleServes),
		Errors:         atomic.LoadUint64(&c.errs),
		HitRatio:       ratio,
		HitRatioWindow: wRatio,
		WindowQueries:  wHits + wMisses,
		WindowMinutes:  windowMinutes,
		TotalQueries:   atomic.LoadUint64(&c.totalQueries),
		QPSHistory:     nil,
		AvgQPS:         avgQPS,
	}
}

func (c *Cache) IncrQueries() {
	atomic.AddUint64(&c.totalQueries, 1)
	atomic.AddUint64(&c.qpsCount, 1)
}

func (c *Cache) IncrErrors() {
	atomic.AddUint64(&c.errs, 1)
}
