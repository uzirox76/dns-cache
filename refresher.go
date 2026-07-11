package main

import (
	"context"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

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
		log.Print("[refresher] stop: timeout, abandoning workers")
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

	var toRefresh []*cache.Entry

	for _, e := range snapshot {
		if e.IsExpired() {
			toRefresh = append(toRefresh, e)
			continue
		}

		remaining := e.TTLRemaining()
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

	// ponytail: cap altrimenti Stop() aspetta tutto
	if len(toRefresh) > 500 {
		toRefresh = toRefresh[:500]
	}

	sort.Slice(toRefresh, func(i, j int) bool {
		ie := toRefresh[i].IsExpired()
		je := toRefresh[j].IsExpired()
		if ie != je {
			return ie
		}
		return toRefresh[i].HitCount > toRefresh[j].HitCount
	})

	log.Printf("[refresher] refreshing %d/%d entries (top: %s %d hits)",
		len(toRefresh), len(snapshot),
		dns.TypeToString[toRefresh[0].QuestionType]+" "+toRefresh[0].QuestionName,
		toRefresh[0].HitCount)

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
		if e.IsExpired() && now.After(e.ExpiresAt.Add(24*time.Hour)) {
			rf.cache.Delete(e.Key())
		}
	}
}
