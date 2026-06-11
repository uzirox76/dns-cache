package cache

import (
	"fmt"
	"time"

	"github.com/miekg/dns"
)

type Entry struct {
	QuestionName string
	QuestionType uint16
	Response     *dns.Msg
	StoredAt     time.Time
	OriginalTTL  uint32
	CachedTTL    time.Duration
	ExpiresAt    time.Time
	HitCount     uint64
	LastHitAt    time.Time
}

func (e *Entry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

func (e *Entry) TTLRemaining() time.Duration {
	return time.Until(e.ExpiresAt)
}

func (e *Entry) Key() string {
	return Key(e.QuestionName, e.QuestionType)
}

func (e *Entry) RefreshThreshold() float64 {
	switch {
	case e.HitCount >= 100:
		return 0.30
	case e.HitCount >= 20:
		return 0.20
	case e.HitCount >= 5:
		return 0.15
	default:
		return 0.10
	}
}

func Key(name string, qtype uint16) string {
	return fmt.Sprintf("%s:%d", dns.Fqdn(name), qtype)
}

func CopyAndSetTTL(msg *dns.Msg, ttl uint32) *dns.Msg {
	m := msg.Copy()
	for i := range m.Answer {
		m.Answer[i].Header().Ttl = ttl
	}
	for i := range m.Ns {
		m.Ns[i].Header().Ttl = ttl
	}
	for i := range m.Extra {
		if m.Extra[i].Header().Rrtype == dns.TypeOPT {
			continue
		}
		m.Extra[i].Header().Ttl = ttl
	}
	return m
}
