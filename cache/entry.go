package cache

import (
	"fmt"
	"math/bits"
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
	// UsedDays sono i giorni in cui un client ha chiesto il nome, atomico: i
	// 32 bit alti sono il giorno locale dell'ultimo uso, i 32 bassi una
	// maschera in cui il bit i vale quel giorno meno i. Giorno e maschera
	// stanno in un solo uint64 perche' vanno cambiati insieme, con una CAS.
	UsedDays uint64
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

// localDay numera i giorni di calendario nel fuso locale: in UTC una serata a
// cavallo delle 2 di notte conterebbe come due giorni d'uso.
func localDay(t time.Time) int64 {
	_, offset := t.Zone()
	return (t.Unix() + int64(offset)) / 86400
}

// PackUsedDays e' il valore di UsedDays con il solo giorno di t.
func PackUsedDays(t time.Time) uint64 {
	return uint64(localDay(t))<<32 | 1
}

// MarkUsed segna il giorno di t fra quelli d'uso. Sul percorso di ogni query:
// nel caso comune (giorno gia' segnato) e' una Load e un confronto.
func (e *Entry) MarkUsed(t time.Time) {
	today := localDay(t)
	for {
		old := atomic.LoadUint64(&e.UsedDays)
		next := PackUsedDays(t)
		switch shift := today - int64(old>>32); {
		case shift >= 32:
			// Storico vuoto, o tutto piu' vecchio della maschera.
		case shift <= 0:
			// Stesso giorno, o orologio tornato indietro: si segna l'ultimo
			// giorno noto invece di riscrivere lo storico.
			next = old | 1
		default:
			next = uint64(today)<<32 | uint64(uint32(old)<<shift|1)
		}
		if next == old || atomic.CompareAndSwapUint64(&e.UsedDays, old, next) {
			return
		}
	}
}

// DaysUsed conta i giorni d'uso fra gli ultimi window (al massimo 32), oggi
// compreso.
func (e *Entry) DaysUsed(t time.Time, window int) int {
	v := atomic.LoadUint64(&e.UsedDays)
	if v == 0 {
		return 0
	}
	shift := max(localDay(t)-int64(v>>32), 0)
	if shift >= int64(window) {
		return 0
	}
	keep := uint32(1)<<(int64(window)-shift) - 1
	return bits.OnesCount32(uint32(v) & keep)
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
