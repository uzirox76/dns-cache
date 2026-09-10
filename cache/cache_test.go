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

// Una risposta senza answer si cacha per il minimo fra TTL e MINIMUM del SOA
// (RFC 2308 §5), non per il solo MINIMUM: sui domini Route53 il MINIMUM e'
// 86400 e il SOA ha TTL 900.
func TestSetNegativoUsaIlMinimoFraTTLeMinimumDelSOA(t *testing.T) {
	c := New(Config{TTLMin: time.Minute, TTLMax: 24 * time.Hour, MaxEntries: 10})

	resp := new(dns.Msg)
	resp.SetQuestion("esempio.test.", dns.TypeHTTPS)
	soa, err := dns.NewRR("esempio.test. 900 IN SOA ns.esempio.test. host.esempio.test. 1 7200 900 1209600 86400")
	if err != nil {
		t.Fatal(err)
	}
	resp.Ns = []dns.RR{soa}

	c.Set("esempio.test.", dns.TypeHTTPS, resp, 0)

	e, ok := c.Get(Key("esempio.test.", dns.TypeHTTPS))
	if !ok {
		t.Fatal("Get() dopo Set(): atteso hit")
	}
	if e.CachedTTL != 900*time.Second {
		t.Errorf("CachedTTL = %v, atteso 15m0s (il TTL del SOA, minore del MINIMUM)", e.CachedTTL)
	}
}

// TestSetEredita: un refresh sostituisce la entry ma non e' un accesso del
// client, quindi hit count, recency e giorni d'uso devono sopravvivere alla
// sostituzione.
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
	// Uno storico con anche ieri, che una entry nuova non avrebbe.
	e.UsedDays |= 0b10
	giorniPrima := e.UsedDays

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
	if e2.UsedDays != giorniPrima {
		t.Errorf("UsedDays = %#x dopo il refresh, era %#x: storico dei giorni d'uso perso", e2.UsedDays, giorniPrima)
	}
}

func TestGiorniDUsoNellaFinestra(t *testing.T) {
	e := &Entry{}
	giorno0 := time.Date(2026, 9, 1, 12, 0, 0, 0, time.Local)

	e.MarkUsed(giorno0)
	e.MarkUsed(giorno0.Add(time.Hour)) // stesso giorno: conta una volta
	e.MarkUsed(giorno0.AddDate(0, 0, 3))

	casi := []struct {
		quando time.Time
		atteso int
	}{
		{giorno0.AddDate(0, 0, 3), 2},
		{giorno0.AddDate(0, 0, 6), 2}, // giorno0 e' ancora il settimo giorno della finestra
		{giorno0.AddDate(0, 0, 7), 1}, // giorno0 esce
		{giorno0.AddDate(0, 0, 10), 0},
	}
	for _, c := range casi {
		if got := e.DaysUsed(c.quando, 7); got != c.atteso {
			t.Errorf("DaysUsed(giorno %d, 7) = %d, atteso %d",
				int(c.quando.Sub(giorno0).Hours()/24), got, c.atteso)
		}
	}

	// Un uso dopo piu' di 32 giorni riparte da zero invece di sporcare la
	// maschera con bit rimasti da prima.
	e.MarkUsed(giorno0.AddDate(0, 0, 40))
	if got := e.DaysUsed(giorno0.AddDate(0, 0, 40), 32); got != 1 {
		t.Errorf("dopo 40 giorni DaysUsed = %d, atteso 1", got)
	}
}

// Oltre il TTL una entry si serve finche' la risposta ha meno di MaxAge; oltre
// e' un miss. In entrambi i casi la richiesta conta come giorno d'uso.
func TestGetServeOltreIlTTLSoloEntroMaxAge(t *testing.T) {
	c := New(Config{TTLMin: time.Minute, TTLMax: time.Hour, MaxEntries: 10, MaxAge: 30 * time.Minute})
	now := time.Now()

	carica := func(nome string, eta time.Duration) {
		resp := new(dns.Msg)
		resp.SetQuestion(nome, dns.TypeA)
		c.LoadEntry(&Entry{
			QuestionName: nome,
			QuestionType: dns.TypeA,
			Response:     resp,
			StoredAt:     now.Add(-eta),
			CachedTTL:    time.Minute,
			ExpiresAt:    now.Add(-eta + time.Minute),
		})
	}
	carica("recente.test.", 10*time.Minute)
	carica("vecchia.test.", 40*time.Minute)

	if _, ok := c.Get(Key("recente.test.", dns.TypeA)); !ok {
		t.Error("TTL scaduto da 9 minuti, risposta di 10: attesa servita")
	}
	vecchia := Key("vecchia.test.", dns.TypeA)
	if _, ok := c.Get(vecchia); ok {
		t.Error("risposta di 40 minuti con max_age 30: atteso miss")
	}

	st := c.Stats()
	if st.Hits != 1 || st.LateHits != 1 || st.Misses != 1 {
		t.Errorf("Stats: hits %d, late %d, misses %d; attesi 1, 1, 1", st.Hits, st.LateHits, st.Misses)
	}
	if got := c.entries[vecchia].DaysUsed(now, 7); got != 1 {
		t.Errorf("DaysUsed dopo un miss = %d, atteso 1: la richiesta non e' stata contata", got)
	}
}
