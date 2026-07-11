package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/miekg/dns"
)

type Resolver struct {
	upstreams []string
	timeout   time.Duration
	rng       *rand.Rand
}

func NewResolver(upstreams []string, timeout time.Duration) *Resolver {
	return &Resolver{
		upstreams: upstreams,
		timeout:   timeout,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
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

	msg.SetEdns0(4096, true)

	indices := r.rng.Perm(len(r.upstreams))
	var errs []upstreamError

	c := &dns.Client{UDPSize: 1232, Timeout: r.timeout}
	for _, idx := range indices {
		if err := ctx.Err(); err != nil {
			errs = append(errs, upstreamError{server: "context", err: err})
			break
		}

		upstream := r.upstreams[idx]
		clone := msg.Copy()
		resp, rtt, err := c.Exchange(clone, upstream)
		if err == nil && resp != nil {
			return resp, upstream, rtt, nil
		}
		if err != nil {
			errs = append(errs, upstreamError{server: upstream, err: err})
		}
	}

	var details []string
	for _, e := range errs {
		details = append(details, fmt.Sprintf("%s: %v", e.server, e.err))
	}

	return nil, "", 0, fmt.Errorf("all upstreams failed: %s", strings.Join(details, "; "))
}
