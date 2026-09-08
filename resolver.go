package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/miekg/dns"
)

type Resolver struct {
	upstreams []string
	udp       *dns.Client
	tcp       *dns.Client
}

func NewResolver(upstreams []string, timeout time.Duration) *Resolver {
	return &Resolver{
		upstreams: upstreams,
		// UDPSize e' il buffer di lettura e deve coprire quanto annunciamo
		// all'upstream in SetEdns0, altrimenti le risposte grosse non si
		// riescono a spacchettare.
		udp: &dns.Client{Net: "udp", UDPSize: dns.DefaultMsgSize, Timeout: timeout},
		tcp: &dns.Client{Net: "tcp", Timeout: timeout},
	}
}

type upstreamError struct {
	server string
	err    error
}

func (r *Resolver) Resolve(ctx context.Context, msg *dns.Msg) (*dns.Msg, string, time.Duration, error) {
	if len(r.upstreams) == 0 {
		return nil, "", 0, fmt.Errorf("no upstream servers configured")
	}

	// Non si tocca il messaggio del client: l'OPT serve a noi verso l'upstream,
	// e piu' avanti l'handler deve poter sapere se il client usava EDNS0.
	out := msg.Copy()
	// Il client puo' aver mandato un OPT suo e SetEdns0 appende senza
	// sostituire: due OPT nella stessa query violano la RFC 6891 e c'e'
	// chi risponde FORMERR.
	out.Extra = stripOPT(out.Extra)
	out.SetEdns0(dns.DefaultMsgSize, true)

	// rand.Perm globale: e' safe per uso concorrente, un *rand.Rand per-istanza
	// no (Resolve gira su una goroutine per query + 5 del refresher).
	// Dal Go 1.20 la sorgente globale e' gia' seedata a random, niente Seed().
	indices := rand.Perm(len(r.upstreams))
	var errs []upstreamError

	for _, idx := range indices {
		if err := ctx.Err(); err != nil {
			errs = append(errs, upstreamError{server: "context", err: err})
			break
		}

		upstream := r.upstreams[idx]
		resp, rtt, err := r.udp.Exchange(out, upstream)
		if err != nil {
			errs = append(errs, upstreamError{server: upstream, err: err})
			continue
		}
		if resp == nil {
			continue
		}

		// Risposta troncata: si rifa' la query in TCP, altrimenti si
		// servirebbe (e cacherebbe) una risposta incompleta.
		if resp.Truncated {
			full, rtt2, terr := r.tcp.Exchange(out, upstream)
			if terr == nil && full != nil {
				return full, upstream, rtt + rtt2, nil
			}
			log.Printf("[resolve] retry TCP su %s fallito: %v", upstream, terr)
		}

		return resp, upstream, rtt, nil
	}

	var details []string
	for _, e := range errs {
		details = append(details, fmt.Sprintf("%s: %v", e.server, e.err))
	}

	return nil, "", 0, fmt.Errorf("all upstreams failed: %s", strings.Join(details, "; "))
}
