package main

import (
	"context"
	"log"
	"net"

	"github.com/miekg/dns"
	"golang.org/x/sync/singleflight"

	"dns-cache/cache"
)

// maxUDPSize e' il limite UDP annunciato e rispettato dal server: 1232 byte
// stanno sotto la MTU tipica e non fanno frammentare il datagramma
// (raccomandazione DNS Flag Day 2020).
const maxUDPSize = 1232

type DNSHandler struct {
	cache    *cache.Cache
	resolver *Resolver
	// inflight accorpa le query identiche che arrivano mentre la prima e'
	// ancora in volo verso l'upstream: il client ne manda spesso due a pochi
	// millisecondi di distanza, e senza accorparle partivano due richieste.
	// Ci passa anche il refresh in background delle entry servite oltre il
	// TTL, cosi' un miss che arriva nel frattempo aspetta quello.
	inflight singleflight.Group
}

func NewDNSHandler(c *cache.Cache, r *Resolver) *DNSHandler {
	return &DNSHandler{cache: c, resolver: r}
}

func (h *DNSHandler) ServeDNS(w dns.ResponseWriter, req *dns.Msg) {
	if len(req.Question) == 0 {
		return
	}

	h.cache.IncrQueries()

	q := req.Question[0]
	key := cache.Key(q.Name, q.Qtype)

	if entry, ok := h.cache.Get(key); ok {
		if entry.IsExpired() {
			// Oltre il TTL ma entro max_age: si risponde subito con la copia
			// in cache e intanto la si richiede all'upstream, cosi' i 30-40ms
			// li aspetta il server e non il client. Se l'IP e' cambiato, gia'
			// la richiesta successiva trova quello nuovo. La query e' nuova:
			// quella del client non va usata dopo che l'handler e' tornato.
			refresh := new(dns.Msg)
			refresh.SetQuestion(q.Name, q.Qtype)
			h.inflight.DoChan(key, h.resolveAndCache(refresh, true))
		}
		respond(w, req, cache.CopyAndSetTTL(entry.Response, ttlForEntry(entry)))
		return
	}

	v, err, _ := h.inflight.Do(key, h.resolveAndCache(req, false))
	if err != nil {
		h.cache.IncrErrors()

		if entry, ok := h.cache.GetStale(key); ok {
			log.Printf("[stale] serving stale for %s (upstream error)", q.Name)
			respond(w, req, cache.CopyAndSetTTL(entry.Response, 30))
			return
		}

		m := new(dns.Msg)
		m.SetReply(req)
		m.Rcode = dns.RcodeServerFailure
		w.WriteMsg(m)
		return
	}

	// La risposta e' condivisa fra le richieste accorpate e respond() la
	// modifica (SetReply, OPT, Truncate): ognuna ne prende una copia, anche
	// la prima, che altrimenti la cambierebbe mentre le altre la copiano.
	respond(w, req, v.(*dns.Msg).Copy())
}

// resolveAndCache ritorna la funzione che interroga l'upstream per msg e mette
// in cache la risposta. Gira dentro inflight, quindi una volta sola per
// chiave anche con piu' richieste in attesa. Il refresh in background non
// logga le risposte, come il refresher: [query] resta la riga di un miss.
func (h *DNSHandler) resolveAndCache(msg *dns.Msg, background bool) func() (any, error) {
	q := msg.Question[0]
	return func() (any, error) {
		resp, upstream, rtt, err := h.resolver.Resolve(context.Background(), msg)
		if err != nil {
			log.Printf("[error] resolve %s: %v", q.Name, err)
			return nil, err
		}

		if !background {
			log.Printf("[query] %s %s via %s (%.0fms)",
				dns.TypeToString[q.Qtype], q.Name, upstream, rtt.Seconds()*1000)
		}

		var originalTTL uint32
		if len(resp.Answer) > 0 {
			originalTTL = resp.Answer[0].Header().Ttl
		}

		if isCacheable(resp) {
			h.cache.Set(q.Name, q.Qtype, resp, originalTTL)
		}
		return resp, nil
	}
}

// isCacheable: si cachano solo NOERROR e NXDOMAIN (RFC 2308). SERVFAIL e
// REFUSED sono guasti transitori dell'upstream e tenerli per ttl_min
// significherebbe propagare per un minuto un problema non nostro. Una risposta
// troncata e' incompleta e non va cachata in nessun caso.
func isCacheable(resp *dns.Msg) bool {
	if resp.Truncated {
		return false
	}
	return resp.Rcode == dns.RcodeSuccess || resp.Rcode == dns.RcodeNameError
}

// respond adatta una risposta gia' formata (dall'upstream o dalla cache) alla
// domanda del client e la scrive, troncandola se il client non puo' riceverla
// intera.
func respond(w dns.ResponseWriter, req, resp *dns.Msg) {
	// SetReply da solo non basta: forza l'Rcode a NOERROR e trasformerebbe
	// gli NXDOMAIN in risposte vuote.
	rcode := resp.Rcode
	resp.SetReply(req)
	resp.Rcode = rcode

	// L'OPT presente descrive la sessione con l'upstream, non quella col
	// client: si rimuove e, se il client usa EDNS0, se ne rimette uno suo.
	if resp.IsEdns0() != nil {
		resp.Extra = stripOPT(resp.Extra)
	}

	size := dns.MinMsgSize
	if opt := req.IsEdns0(); opt != nil {
		if s := int(opt.UDPSize()); s > size {
			size = s
		}
		if size > maxUDPSize {
			size = maxUDPSize
		}
		resp.SetEdns0(uint16(size), opt.Do())
	}

	// Su TCP non serve troncare: c'e' il campo lunghezza a 16 bit.
	if _, isTCP := w.RemoteAddr().(*net.TCPAddr); isTCP {
		size = dns.MaxMsgSize
	}

	resp.Compress = true
	resp.Truncate(size)

	if err := w.WriteMsg(resp); err != nil {
		log.Printf("[error] write reply %s: %v", req.Question[0].Name, err)
	}
}

func stripOPT(rrs []dns.RR) []dns.RR {
	out := make([]dns.RR, 0, len(rrs))
	for _, rr := range rrs {
		if rr.Header().Rrtype != dns.TypeOPT {
			out = append(out, rr)
		}
	}
	return out
}

func ttlForEntry(entry *cache.Entry) uint32 {
	r := entry.TTLRemaining()
	if r <= 0 {
		return 30
	}
	return uint32(r.Seconds())
}
