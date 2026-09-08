package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

// maxRefreshPerCycle limita il lavoro di un singolo ciclo di refresh.
const maxRefreshPerCycle = 500

// refreshMaxIdle: si tiene calda solo la coda di domini che il client ha
// chiesto di recente. Senza questo limite il refresher rinterroga in eterno
// ogni entry mai vista una volta -- il refresh stesso la tiene viva, quindi
// non scade mai fuori dalla cache -- e il tetto per ciclo se lo mangiano le
// entry fredde invece di quelle che il client sta davvero usando.
const refreshMaxIdle = time.Hour

// Backoff esponenziale sulle entry che falliscono il refresh: restano
// scadute, l'ordinamento le rimette in testa e senza backoff si riprovano a
// ogni ciclo bruciando il tetto sempre sulle stesse.
const (
	refreshBackoffMax   = 30 * time.Minute
	refreshBackoffShift = 10
)

type Refresher struct {
	cache    *cache.Cache
	resolver *Resolver
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup

	mu       sync.Mutex
	failures map[string]*refreshFailure
}

type refreshFailure struct {
	count   uint
	nextTry time.Time
}

func NewRefresher(c *cache.Cache, r *Resolver, interval time.Duration) *Refresher {
	return &Refresher{
		cache:    c,
		resolver: r,
		interval: interval,
		stopCh:   make(chan struct{}),
		failures: make(map[string]*refreshFailure),
	}
}

func (rf *Refresher) Start() {
	rf.wg.Add(1)
	go rf.loop()
	log.Printf("[refresher] started (interval: %v)", rf.interval)
}

func (rf *Refresher) Stop() {
	close(rf.stopCh)

	done := make(chan struct{})
	go func() {
		rf.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Print("[refresher] stopped")
	case <-time.After(6 * time.Second):
	}
}

func (rf *Refresher) loop() {
	defer rf.wg.Done()
	ticker := time.NewTicker(rf.interval)
	defer ticker.Stop()

	for {
		select {
		case <-rf.stopCh:
			return
		case <-ticker.C:
			rf.refreshCycle()
		}
	}
}

func (rf *Refresher) refreshCycle() {
	snapshot := rf.cache.Snapshot()
	if len(snapshot) == 0 {
		return
	}

	rf.pruneFailures(snapshot)

	now := time.Now()

	// La finestra di prefetch non puo' essere piu' stretta dell'intervallo
	// del refresher: con un TTL di 60s il 10% sono 6 secondi, il tick
	// successivo arriva quando la entry e' gia' scaduta e il client si becca
	// un miss. Un intervallo e mezzo garantisce che almeno un tick cada
	// dentro la finestra.
	minWindow := rf.interval * 3 / 2

	var toRefresh []*cache.Entry
	var skippedIdle, skippedBackoff int

	for key, e := range snapshot {
		lastHit := time.Unix(0, atomic.LoadInt64(&e.LastHitAt))
		if now.Sub(lastHit) > refreshMaxIdle {
			skippedIdle++
			continue
		}
		if rf.backingOff(key, now) {
			skippedBackoff++
			continue
		}

		remaining := e.ExpiresAt.Sub(now)
		if remaining <= 0 {
			toRefresh = append(toRefresh, e)
			continue
		}

		threshold := time.Duration(float64(e.CachedTTL) * e.RefreshThreshold())
		if threshold < minWindow {
			threshold = minWindow
		}
		// Una entry non puo' essere in finestra da prima di esistere: con
		// TTL <= minWindow si rinfresca a ogni ciclo, che e' l'unico modo di
		// tenerla calda.
		if threshold > e.CachedTTL {
			threshold = e.CachedTTL
		}
		if remaining <= threshold {
			toRefresh = append(toRefresh, e)
		}
	}

	if len(toRefresh) == 0 {
		return
	}

	// Ordinare prima di tagliare: al contrario il cap scarterebbe entry
	// scelte a caso (l'ordine di iterazione della mappa) proprio quando la
	// priorita' serve.
	sort.Slice(toRefresh, func(i, j int) bool {
		ie := toRefresh[i].IsExpiredAt(now)
		je := toRefresh[j].IsExpiredAt(now)
		if ie != je {
			return ie
		}
		return atomic.LoadUint64(&toRefresh[i].HitCount) > atomic.LoadUint64(&toRefresh[j].HitCount)
	})

	if len(toRefresh) > maxRefreshPerCycle {
		toRefresh = toRefresh[:maxRefreshPerCycle]
	}

	log.Printf("[refresher] refreshing %d/%d entries (%d fredde, %d in backoff; top: %s %d hits)",
		len(toRefresh), len(snapshot), skippedIdle, skippedBackoff,
		dns.TypeToString[toRefresh[0].QuestionType]+" "+toRefresh[0].QuestionName,
		atomic.LoadUint64(&toRefresh[0].HitCount))

	sem := make(chan struct{}, 5)
	var wg sync.WaitGroup

	for _, e := range toRefresh {
		select {
		case <-rf.stopCh:
			wg.Wait()
			return
		default:
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(entry *cache.Entry) {
			defer wg.Done()
			defer func() { <-sem }()
			rf.refreshEntry(entry)
		}(e)
	}

	wg.Wait()

	if rf.cache.Config().StaleServing {
		rf.pruneExpired()
	}
}

func (rf *Refresher) refreshEntry(e *cache.Entry) {
	key := e.Key()

	m := new(dns.Msg)
	m.SetQuestion(e.QuestionName, e.QuestionType)

	resp, _, _, err := rf.resolver.Resolve(context.Background(), m)
	if err != nil {
		log.Printf("[refresher] refresh %s %s failed: %v",
			dns.TypeToString[e.QuestionType], e.QuestionName, err)
		rf.noteFailure(key)
		return
	}

	if resp.Rcode != dns.RcodeSuccess {
		log.Printf("[refresher] refresh %s %s returned rcode %d, keeping old entry",
			dns.TypeToString[e.QuestionType], e.QuestionName, resp.Rcode)
		rf.noteFailure(key)
		return
	}

	var originalTTL uint32
	if len(resp.Answer) > 0 {
		originalTTL = resp.Answer[0].Header().Ttl
	}
	if originalTTL == 0 {
		originalTTL = 60
	}

	rf.noteSuccess(key)
	rf.cache.Set(e.QuestionName, e.QuestionType, resp, originalTTL)
}

// backingOff dice se la entry sta scontando un fallimento recente.
func (rf *Refresher) backingOff(key string, now time.Time) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	f, ok := rf.failures[key]
	return ok && now.Before(f.nextTry)
}

func (rf *Refresher) noteFailure(key string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	f, ok := rf.failures[key]
	if !ok {
		f = &refreshFailure{}
		rf.failures[key] = f
	}
	f.count++

	shift := f.count
	if shift > refreshBackoffShift {
		shift = refreshBackoffShift
	}
	wait := rf.interval << shift
	if wait > refreshBackoffMax || wait <= 0 {
		wait = refreshBackoffMax
	}
	f.nextTry = time.Now().Add(wait)
}

func (rf *Refresher) noteSuccess(key string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	delete(rf.failures, key)
}

// pruneFailures scarta lo stato di backoff delle entry non piu' in cache,
// altrimenti la mappa cresce quanto tutti i nomi mai visti.
func (rf *Refresher) pruneFailures(snapshot map[string]*cache.Entry) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for k := range rf.failures {
		if _, ok := snapshot[k]; !ok {
			delete(rf.failures, k)
		}
	}
}

func (rf *Refresher) pruneExpired() {
	snapshot := rf.cache.Snapshot()
	now := time.Now()
	for _, e := range snapshot {
		if e.IsExpiredAt(now) && now.After(e.ExpiresAt.Add(24*time.Hour)) {
			rf.cache.Delete(e.Key())
		}
	}
}
