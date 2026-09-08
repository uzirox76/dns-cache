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

type Refresher struct {
	cache    *cache.Cache
	resolver *Resolver
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup
}

func NewRefresher(c *cache.Cache, r *Resolver, interval time.Duration) *Refresher {
	return &Refresher{
		cache:    c,
		resolver: r,
		interval: interval,
		stopCh:   make(chan struct{}),
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

	now := time.Now()
	var toRefresh []*cache.Entry

	for _, e := range snapshot {
		if e.IsExpiredAt(now) {
			toRefresh = append(toRefresh, e)
			continue
		}

		remaining := e.ExpiresAt.Sub(now)
		if remaining <= 0 {
			toRefresh = append(toRefresh, e)
			continue
		}

		pct := e.RefreshThreshold()
		threshold := time.Duration(float64(e.CachedTTL) * pct)
		if threshold <= 0 {
			threshold = 10 * time.Second
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

	log.Printf("[refresher] refreshing %d/%d entries (top: %s %d hits)",
		len(toRefresh), len(snapshot),
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
	m := new(dns.Msg)
	m.SetQuestion(e.QuestionName, e.QuestionType)

	resp, _, _, err := rf.resolver.Resolve(context.Background(), m)
	if err != nil {
		log.Printf("[refresher] refresh %s %s failed: %v",
			dns.TypeToString[e.QuestionType], e.QuestionName, err)
		return
	}

	if resp.Rcode != dns.RcodeSuccess {
		log.Printf("[refresher] refresh %s %s returned rcode %d, keeping old entry",
			dns.TypeToString[e.QuestionType], e.QuestionName, resp.Rcode)
		return
	}

	var originalTTL uint32
	if len(resp.Answer) > 0 {
		originalTTL = resp.Answer[0].Header().Ttl
	}
	if originalTTL == 0 {
		originalTTL = 60
	}

	rf.cache.Set(e.QuestionName, e.QuestionType, resp, originalTTL)
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
