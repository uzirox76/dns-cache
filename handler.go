package main

import (
	"context"
	"log"

	"github.com/miekg/dns"

	"dns-cache/cache"
)

type DNSHandler struct {
	cache    *cache.Cache
	resolver *Resolver
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
		resp := cache.CopyAndSetTTL(entry.Response, ttlForEntry(entry))
		resp.SetReply(req)
		resp.Compress = true
		w.WriteMsg(resp)
		return
	}

	resp, upstream, rtt, err := h.resolver.Resolve(context.Background(), req)
	if err != nil {
		h.cache.IncrErrors()
		log.Printf("[error] resolve %s: %v", q.Name, err)

		if entry, ok := h.cache.GetStale(key); ok {
			log.Printf("[stale] serving stale for %s (upstream error)", q.Name)
			resp := cache.CopyAndSetTTL(entry.Response, 30)
			resp.SetReply(req)
			resp.Compress = true
			w.WriteMsg(resp)
			return
		}

		m := new(dns.Msg)
		m.SetReply(req)
		m.Rcode = dns.RcodeServerFailure
		w.WriteMsg(m)
		return
	}

	log.Printf("[query] %s %s via %s (%.0fms)",
		dns.TypeToString[q.Qtype], q.Name, upstream, rtt.Seconds()*1000)

	var originalTTL uint32
	if len(resp.Answer) > 0 {
		originalTTL = resp.Answer[0].Header().Ttl
	}

	h.cache.Set(q.Name, q.Qtype, resp, originalTTL)

	resp.SetReply(req)
	resp.Compress = true
	w.WriteMsg(resp)
}

func ttlForEntry(entry *cache.Entry) uint32 {
	r := entry.TTLRemaining()
	if r <= 0 {
		return 30
	}
	return uint32(r.Seconds())
}
