package main

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

// fakeWriter tiene la risposta che l'handler scrive al client.
type fakeWriter struct {
	msg *dns.Msg
}

func (w *fakeWriter) LocalAddr() net.Addr { return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53} }
func (w *fakeWriter) RemoteAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 40000}
}
func (w *fakeWriter) WriteMsg(m *dns.Msg) error   { w.msg = m; return nil }
func (w *fakeWriter) Write(b []byte) (int, error) { return len(b), nil }
func (w *fakeWriter) Close() error                { return nil }
func (w *fakeWriter) TsigStatus() error           { return nil }
func (w *fakeWriter) TsigTimersOnly(bool)         {}
func (w *fakeWriter) Hijack()                     {}

// fakeUpstream risponde a ogni query con un record A dopo delay, contando le
// query ricevute.
func fakeUpstream(t *testing.T, delay time.Duration, queries *atomic.Int32) string {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	srv := &dns.Server{
		PacketConn:        pc,
		NotifyStartedFunc: func() { close(started) },
		Handler: dns.HandlerFunc(func(w dns.ResponseWriter, r *dns.Msg) {
			queries.Add(1)
			time.Sleep(delay)
			m := new(dns.Msg)
			m.SetReply(r)
			rr, _ := dns.NewRR(r.Question[0].Name + " 300 IN A 192.0.2.1")
			m.Answer = []dns.RR{rr}
			w.WriteMsg(m)
		}),
	}
	go srv.ActivateAndServe()
	<-started
	t.Cleanup(func() { srv.Shutdown() })
	return pc.LocalAddr().String()
}

// Query identiche concorrenti vanno all'upstream una volta sola, e ogni client
// riceve una risposta sua: con la risposta condivisa senza copia, la SetReply
// di un client sovrascriverebbe l'ID di un altro (e -race lo segnala).
func TestServeDNSAccorpaLeQueryConcorrenti(t *testing.T) {
	var queries atomic.Int32
	addr := fakeUpstream(t, 200*time.Millisecond, &queries)

	c := cache.New(cache.Config{TTLMin: time.Minute, TTLMax: time.Hour, MaxEntries: 10})
	h := NewDNSHandler(c, NewResolver([]string{addr}, time.Second))

	const clients = 5
	writers := make([]*fakeWriter, clients)
	reqs := make([]*dns.Msg, clients)
	var wg sync.WaitGroup
	for i := range clients {
		writers[i] = &fakeWriter{}
		reqs[i] = new(dns.Msg)
		reqs[i].SetQuestion("esempio.test.", dns.TypeA)
		reqs[i].Id = uint16(1000 + i)
		reqs[i].SetEdns0(1232, false)

		wg.Add(1)
		go func() {
			defer wg.Done()
			h.ServeDNS(writers[i], reqs[i])
		}()
	}
	wg.Wait()

	if got := queries.Load(); got != 1 {
		t.Errorf("query all'upstream = %d, attesa 1: le richieste concorrenti non sono state accorpate", got)
	}
	for i, w := range writers {
		if w.msg == nil || len(w.msg.Answer) != 1 {
			t.Fatalf("client %d: risposta mancante o senza answer: %v", i, w.msg)
		}
		if w.msg.Id != reqs[i].Id {
			t.Errorf("client %d: risposta con ID %d, atteso %d: la risposta e' condivisa fra i client", i, w.msg.Id, reqs[i].Id)
		}
	}
}

// Una entry oltre il TTL ma entro max_age si serve subito dalla cache, e
// intanto un refresh in background la sostituisce con quella dell'upstream.
func TestServeDNSOltreIlTTLServeSubitoERinfresca(t *testing.T) {
	var queries atomic.Int32
	addr := fakeUpstream(t, 0, &queries)

	c := cache.New(cache.Config{TTLMin: time.Minute, TTLMax: time.Hour, MaxEntries: 10, MaxAge: 30 * time.Minute})
	h := NewDNSHandler(c, NewResolver([]string{addr}, time.Second))

	vecchia := new(dns.Msg)
	vecchia.SetQuestion("esempio.test.", dns.TypeA)
	rr, err := dns.NewRR("esempio.test. 60 IN A 192.0.2.99")
	if err != nil {
		t.Fatal(err)
	}
	vecchia.Answer = []dns.RR{rr}
	now := time.Now()
	c.LoadEntry(&cache.Entry{
		QuestionName: "esempio.test.",
		QuestionType: dns.TypeA,
		Response:     vecchia,
		StoredAt:     now.Add(-10 * time.Minute),
		CachedTTL:    time.Minute,
		ExpiresAt:    now.Add(-9 * time.Minute),
		HitCount:     1,
	})

	w := &fakeWriter{}
	req := new(dns.Msg)
	req.SetQuestion("esempio.test.", dns.TypeA)
	h.ServeDNS(w, req)

	if w.msg == nil || len(w.msg.Answer) != 1 {
		t.Fatalf("attesa la risposta dalla cache: %v", w.msg)
	}
	if ip := w.msg.Answer[0].(*dns.A).A.String(); ip != "192.0.2.99" {
		t.Errorf("servito %s, atteso l'IP in cache 192.0.2.99: il client ha aspettato l'upstream", ip)
	}
	if ttl := w.msg.Answer[0].Header().Ttl; ttl != 30 {
		t.Errorf("TTL = %d, atteso 30 per una risposta oltre il TTL", ttl)
	}

	key := cache.Key("esempio.test.", dns.TypeA)
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if e, ok := c.Get(key); ok && !e.IsExpired() {
			if ip := e.Response.Answer[0].(*dns.A).A.String(); ip != "192.0.2.1" {
				t.Errorf("dopo il refresh in cache c'e' %s, atteso 192.0.2.1", ip)
			}
			if got := queries.Load(); got != 1 {
				t.Errorf("query all'upstream = %d, attesa 1", got)
			}
			return
		}
	}
	t.Fatal("la entry non e' stata rinfrescata in background")
}
