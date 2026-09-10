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

	now := time.Now()
	rf.pruneFailures(snapshot)
	rf.pruneUnused(snapshot, now)

	toRefresh, skippedCold, skippedBackoff := rf.selectCandidates(snapshot, now)
	if len(toRefresh) == 0 {
		return
	}

	log.Printf("[refresher] refreshing %d/%d entries (%d non abituali, %d in backoff; top: %s %d hits)",
		len(toRefresh), len(snapshot), skippedCold, skippedBackoff,
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
}

// selectCandidates sceglie le entry da rinfrescare in questo ciclo.
//
// Si tengono aggiornati solo i domini abituali (cache.Config.Usual), anche
// quando non li si chiede da ore: e' lo scopo della cache, averli pronti e
// freschi quando si torna a usarli. Gli altri scadono e al ritorno sono un
// miss: tenerli caldi costerebbe query all'upstream per nomi visti una volta.
//
// La scadenza da anticipare non e' il TTL ma max(TTL, max_age) dal fetch:
// fin li' la entry si serve comunque (oltre il TTL rinfrescandola in
// background, vedi Cache.Get). Con TTL di 60s e max_age di 30 minuti un
// dominio inattivo si rinfresca ogni 25-30 minuti invece che a ogni ciclo.
func (rf *Refresher) selectCandidates(snapshot map[string]*cache.Entry, now time.Time) (toRefresh []*cache.Entry, skippedCold, skippedBackoff int) {
	cfg := rf.cache.Config()

	// La finestra di prefetch non puo' essere piu' stretta dell'intervallo
	// del refresher: il tick successivo arriverebbe a scadenza gia' passata.
	// Un intervallo e mezzo garantisce che almeno un tick cada dentro.
	minWindow := rf.interval * 3 / 2

	type candidate struct {
		e       *cache.Entry
		overdue bool
	}
	var cands []candidate

	for key, e := range snapshot {
		if !cfg.Usual(e, now) {
			skippedCold++
			continue
		}
		if rf.backingOff(key, now) {
			skippedBackoff++
			continue
		}

		life := max(e.CachedTTL, cfg.MaxAge)
		remaining := e.StoredAt.Add(life).Sub(now)
		if remaining <= 0 {
			cands = append(cands, candidate{e, true})
			continue
		}

		// Una entry non puo' essere in finestra da prima di esistere: con una
		// vita <= minWindow si rinfresca a ogni ciclo, che e' l'unico modo di
		// tenerla calda.
		threshold := min(max(time.Duration(float64(life)*e.RefreshThreshold()), minWindow), life)
		if remaining <= threshold {
			cands = append(cands, candidate{e, false})
		}
	}

	// Ordinare prima di tagliare: al contrario il cap scarterebbe entry
	// scelte a caso (l'ordine di iterazione della mappa) proprio quando la
	// priorita' serve, per esempio al primo ciclo dopo un riavvio, quando
	// tutte le abituali ricaricate dal DB sono oltre la scadenza.
	sort.Slice(cands, func(i, j int) bool {
		if cands[i].overdue != cands[j].overdue {
			return cands[i].overdue
		}
		return atomic.LoadUint64(&cands[i].e.HitCount) > atomic.LoadUint64(&cands[j].e.HitCount)
	})

	if len(cands) > maxRefreshPerCycle {
		cands = cands[:maxRefreshPerCycle]
	}

	toRefresh = make([]*cache.Entry, len(cands))
	for i, c := range cands {
		toRefresh[i] = c.e
	}
	return toRefresh, skippedCold, skippedBackoff
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

// pruneUnused toglie le entry scadute che nessun client chiede da piu' della
// finestra dei giorni d'uso. Prima vanno tenute anche se scadute: portano lo
// storico che decide chi e' abituale, e senza un dominio usato lunedi' e
// giovedi' non arriverebbe mai a due giorni.
func (rf *Refresher) pruneUnused(snapshot map[string]*cache.Entry, now time.Time) {
	window := rf.cache.Config().KeepWindow()
	for key, e := range snapshot {
		lastHit := time.Unix(0, atomic.LoadInt64(&e.LastHitAt))
		if e.IsExpiredAt(now) && now.Sub(lastHit) > window {
			rf.cache.Delete(key)
		}
	}
}
