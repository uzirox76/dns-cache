package stats

import (
	"fmt"
	"sort"
	"time"

	"dns-cache/cache"
)

type StatsSnapshot struct {
	Uptime         string         `json:"uptime"`
	Cache          cache.Stats    `json:"cache"`
	TopDomains     []DomainStat   `json:"top_domains"`
	MemoryUsage    string         `json:"memory_usage"`
	QueryTypeDist  map[string]int `json:"query_type_dist"`
	ActiveEntries  int            `json:"active_entries"`
	ExpiredEntries int            `json:"expired_entries"`
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
	qtypeDist := make(map[string]int)
	var active, expired int

	store.ForEach(func(key string, e *cache.Entry) bool {
		name := QtypeName(e.QuestionType)
		qtypeDist[name]++
		if e.IsExpired() {
			expired++
		} else {
			active++
		}
		top = append(top, DomainStat{
			Name:     e.QuestionName,
			Type:     name,
			HitCount: e.HitCount,
			TTL:      int(e.TTLRemaining().Seconds()),
		})
		return true
	})

	sort.Slice(top, func(i, j int) bool {
		return top[i].HitCount > top[j].HitCount
	})
	if len(top) > 20 {
		top = top[:20]
	}

	return StatsSnapshot{
		Uptime:         time.Since(startedAt).Round(time.Second).String(),
		Cache:          st,
		TopDomains:     top,
		MemoryUsage:    "n/a",
		QueryTypeDist:  qtypeDist,
		ActiveEntries:  active,
		ExpiredEntries: expired,
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


