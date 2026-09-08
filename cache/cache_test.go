package cache

import (
	"testing"
	"time"

	"github.com/miekg/dns"
)

// TestWindowStatsEsclude i bucket fuori finestra: e' l'unico comportamento
// della finestra scorrevole che non si vede in un test manuale, perche'
// richiederebbe di aspettare 15 minuti.
func TestWindowStatsEscludeIBucketVecchi(t *testing.T) {
	c := New(Config{TTLMin: time.Minute, TTLMax: time.Hour, MaxEntries: 10})
	now := time.Now().Unix() / 60

	// Un bucket dentro la finestra e uno appena fuori.
	c.window[0] = windowBucket{minute: now, hits: 7, misses: 3}
	c.window[1] = windowBucket{minute: now - windowMinutes, hits: 100, misses: 100}

	hits, misses := c.windowStats()
	if hits != 7 || misses != 3 {
		t.Fatalf("windowStats() = (%d, %d), atteso (7, 3): il bucket a %d minuti non e' stato escluso",
			hits, misses, windowMinutes)
	}
}

func TestRecordWindowRiusaIlBucketDelMinutoVecchio(t *testing.T) {
	c := New(Config{TTLMin: time.Minute, TTLMax: time.Hour, MaxEntries: 10})
	minute := time.Now().Unix() / 60

	// Bucket sporco di un giro precedente del ring, stesso indice.
	c.window[minute%windowMinutes] = windowBucket{minute: minute - windowMinutes, hits: 42, misses: 42}

	c.recordWindow(true)

	hits, misses := c.windowStats()
	if hits != 1 || misses != 0 {
		t.Fatalf("windowStats() = (%d, %d), atteso (1, 0): il bucket vecchio non e' stato azzerato", hits, misses)
	}
}

// TestSetEredita: un refresh sostituisce la entry ma non e' un accesso del
// client, quindi hit count e recency devono sopravvivere alla sostituzione.
func TestSetEreditaHitCountERecency(t *testing.T) {
	c := New(Config{TTLMin: time.Minute, TTLMax: time.Hour, MaxEntries: 10})

	resp := new(dns.Msg)
	resp.SetQuestion("esempio.test.", dns.TypeA)
	rr, err := dns.NewRR("esempio.test. 300 IN A 192.0.2.1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Answer = []dns.RR{rr}

	c.Set("esempio.test.", dns.TypeA, resp, 300)

	key := Key("esempio.test.", dns.TypeA)
	for i := 0; i < 5; i++ {
		if _, ok := c.Get(key); !ok {
			t.Fatal("Get() dopo Set(): atteso hit")
		}
	}

	e, _ := c.Get(key)
	hitsPrima := e.HitCount
	recencyPrima := e.LastHitAt

	// Un refresh: stessa chiave, risposta nuova.
	c.Set("esempio.test.", dns.TypeA, resp, 300)

	e2, ok := c.Get(key)
	if !ok {
		t.Fatal("Get() dopo il refresh: atteso hit")
	}
	// Il Get() qui sopra conta come accesso, quindi il confronto e' su >=.
	if e2.HitCount < hitsPrima {
		t.Errorf("HitCount = %d dopo il refresh, era %d: azzerato invece che ereditato", e2.HitCount, hitsPrima)
	}
	if e2.LastHitAt < recencyPrima {
		t.Errorf("LastHitAt tornato indietro dopo il refresh: %d < %d", e2.LastHitAt, recencyPrima)
	}
}
