package stats

import (
	"fmt"
	"time"

	"dns-cache/cache"
)

type StatsSnapshot struct {
	Uptime      string       `json:"uptime"`
	Cache       cache.Stats  `json:"cache"`
	TopDomains  []DomainStat `json:"top_domains"`
	MemoryUsage string       `json:"memory_usage"`
}

type DomainStat struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	HitCount uint64 `json:"hit_count"`
	TTL      int    `json:"ttl"`
}

type Store interface {
	Stats() cache.Stats
	ForEach(fn func(key string, e *cache.Entry) bool)
}

func BuildSnapshot(store Store, startedAt time.Time) StatsSnapshot {
	st := store.Stats()

	var top []DomainStat
	store.ForEach(func(key string, e *cache.Entry) bool {
		if len(top) < 20 {
			top = append(top, DomainStat{
				Name:     e.QuestionName,
				Type:     QtypeName(e.QuestionType),
				HitCount: e.HitCount,
				TTL:      int(e.TTLRemaining().Seconds()),
			})
		}
		return true
	})

	sortDomainStats(top)

	if len(top) > 20 {
		top = top[:20]
	}

	return StatsSnapshot{
		Uptime:      time.Since(startedAt).Round(time.Second).String(),
		Cache:       st,
		TopDomains:  top,
		MemoryUsage: "n/a",
	}
}

func QtypeName(qtype uint16) string {
	names := map[uint16]string{
		1:   "A",
		2:   "NS",
		5:   "CNAME",
		6:   "SOA",
		15:  "MX",
		16:  "TXT",
		28:  "AAAA",
		33:  "SRV",
		99:  "SPF",
		255: "ANY",
	}
	if name, ok := names[qtype]; ok {
		return name
	}
	return fmt.Sprintf("TYPE%d", qtype)
}

func sortDomainStats(stats []DomainStat) {
	for i := 0; i < len(stats); i++ {
		for j := i + 1; j < len(stats); j++ {
			if stats[j].HitCount > stats[i].HitCount {
				stats[i], stats[j] = stats[j], stats[i]
			}
		}
	}
}
