package main

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

// LoadAll ricarica anche le entry scadute, se usate dentro la finestra dei
// giorni d'uso: dopo una notte a macchina spenta sono tutte scadute, e
// scartarle faceva ripartire da zero i domini abituali. Per le righe scritte
// prima di used_days lo storico si ricostruisce da last_hit_at.
func TestLoadAllTieneLeScaduteUsateNellaFinestra(t *testing.T) {
	p, err := NewPersistence(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	now := time.Now()
	riga := func(nome string, ultimoUso time.Duration) *cache.Entry {
		resp := new(dns.Msg)
		resp.SetQuestion(nome, dns.TypeA)
		return &cache.Entry{
			QuestionName: nome,
			QuestionType: dns.TypeA,
			Response:     resp,
			StoredAt:     now.Add(-ultimoUso),
			OriginalTTL:  60,
			CachedTTL:    time.Minute,
			ExpiresAt:    now.Add(-ultimoUso + time.Minute),
			HitCount:     1,
			LastHitAt:    now.Add(-ultimoUso).UnixNano(),
		}
	}
	if err := p.SaveBatch([]*cache.Entry{
		riga("ieri.test.", 26*time.Hour),
		riga("dimenticata.test.", 9*24*time.Hour),
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := p.LoadAll(7 * 24 * time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].QuestionName != "ieri.test." {
		var nomi []string
		for _, e := range entries {
			nomi = append(nomi, e.QuestionName)
		}
		t.Fatalf("caricate %v, attesa solo ieri.test. (scaduta ma usata dentro la finestra)", nomi)
	}
	if got := entries[0].DaysUsed(now, 7); got != 1 {
		t.Errorf("DaysUsed = %d, atteso 1: giorni d'uso non ricostruiti da last_hit_at", got)
	}
}
