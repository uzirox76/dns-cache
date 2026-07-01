package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/miekg/dns"
)

type Resolver struct {
	upstreams []string
	timeout   time.Duration
}

func NewResolver(upstreams []string, timeout time.Duration) *Resolver {
	return &Resolver{
		upstreams: upstreams,
		timeout:   timeout,
	}
}

func (r *Resolver) Resolve(msg *dns.Msg) (*dns.Msg, string, time.Duration, error) {
	if len(r.upstreams) == 0 {
		return nil, "", 0, fmt.Errorf("no upstream servers configured")
	}

	indices := rand.Perm(len(r.upstreams))
	var errs []string

	for _, idx := range indices {
		upstream := r.upstreams[idx]
		c := &dns.Client{
			UDPSize: 1232,
			Timeout: r.timeout,
		}

		resp, rtt, err := c.Exchange(msg, upstream)
		if err == nil && resp != nil {
			return resp, upstream, rtt, nil
		}
		if err != nil {
			errs = append(errs, err.Error())
		}
	}

	return nil, "", 0, fmt.Errorf("all upstreams failed: %s", strings.Join(errs, "; "))
}
