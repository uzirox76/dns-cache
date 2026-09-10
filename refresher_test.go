package main

import (
	"testing"
	"time"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

// Il refresher prende solo i domini abituali, e li rinfresca prima che la
// risposta superi max(TTL, max_age): non a ogni TTL da 60 secondi, e anche se
// nessuno li chiede da ore.
func TestSelectCandidatesAbitualiVersoMaxAge(t *testing.T) {
	c := cache.New(cache.Config{
		TTLMin: time.Minute, TTLMax: 24 * time.Hour, MaxEntries: 100,
		MaxAge: 30 * time.Minute, KeepMinDays: 2, KeepWindowDays: 7,
	})
	rf := NewRefresher(c, nil, 30*time.Second)
	now := time.Now()

	casi := []struct {
		nome     string
		ttl      time.Duration
		eta      time.Duration
		abituale bool
		atteso   bool
	}{
		// TTL scaduto ma servibile fino a 30 minuti: non ancora.
		{"corto-presto.test.", time.Minute, 5 * time.Minute, true, false},
		// Nella finestra prima dei 30 minuti (10% di 30m = 3m).
		{"corto-quasi.test.", time.Minute, 28 * time.Minute, true, true},
		// Ricaricata dal DB dopo una notte: oltre la scadenza, in testa.
		{"corto-notte.test.", time.Minute, 10 * time.Hour, true, true},
		// TTL piu' lungo di max_age: si anticipa il TTL.
		{"lungo-presto.test.", 2 * time.Hour, time.Hour, true, false},
		{"lungo-quasi.test.", 2 * time.Hour, 110 * time.Minute, true, true},
		// Visto un giorno solo: mai, per quanto vecchio.
		{"occasionale.test.", time.Minute, 10 * time.Hour, false, false},
	}
	for _, tc := range casi {
		e := &cache.Entry{
			QuestionName: tc.nome,
			QuestionType: dns.TypeA,
			Response:     new(dns.Msg),
			StoredAt:     now.Add(-tc.eta),
			CachedTTL:    tc.ttl,
			ExpiresAt:    now.Add(tc.ttl - tc.eta),
			HitCount:     1,
		}
		if tc.abituale {
			e.MarkUsed(now.AddDate(0, 0, -1))
		}
		e.MarkUsed(now)
		c.LoadEntry(e)
	}

	scelte, nonAbituali, _ := rf.selectCandidates(c.Snapshot(), now)

	selezionata := map[string]bool{}
	for _, e := range scelte {
		selezionata[e.QuestionName] = true
	}
	for _, tc := range casi {
		if selezionata[tc.nome] != tc.atteso {
			t.Errorf("%s: selezionata = %v, atteso %v", tc.nome, selezionata[tc.nome], tc.atteso)
		}
	}
	if nonAbituali != 1 {
		t.Errorf("non abituali = %d, atteso 1", nonAbituali)
	}
	if len(scelte) > 0 && scelte[0].QuestionName != "corto-notte.test." {
		t.Errorf("in testa %s, attesa la entry oltre la scadenza", scelte[0].QuestionName)
	}
}
