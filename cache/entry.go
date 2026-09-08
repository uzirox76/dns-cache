package cache

import (
	"fmt"
	"sync/atomic"
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
	LastHitAt    int64 // unix nano, atomica
}

func (e *Entry) IsExpired() bool {
	return time.Now().After(e.ExpiresAt)
}

func (e *Entry) IsExpiredAt(now time.Time) bool {
	return now.After(e.ExpiresAt)
}

func (e *Entry) TTLRemaining() time.Duration {
	return time.Until(e.ExpiresAt)
}

func (e *Entry) Key() string {
	return Key(e.QuestionName, e.QuestionType)
}

func (e *Entry) RefreshThreshold() float64 {
	hc := atomic.LoadUint64(&e.HitCount)
	switch {
	case hc >= 100:
		return 0.30
	case hc >= 20:
		return 0.20
	case hc >= 5:
		return 0.15
	default:
		return 0.10
	}
}

// Key normalizza il nome: i nomi DNS sono case-insensitive, quindi senza
// canonicalizzare "Google.com." e "google.com." sarebbero due entry distinte
// (e un client con 0x20 randomization manderebbe l'hit ratio a zero).
func Key(name string, qtype uint16) string {
	return fmt.Sprintf("%s:%d", dns.CanonicalName(name), qtype)
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
