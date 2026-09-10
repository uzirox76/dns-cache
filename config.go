package main

import (
	"log"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen     string            `yaml:"listen"`
	Upstreams  []string          `yaml:"upstreams"`
	CacheCfg   CacheConfig       `yaml:"cache"`
	PersistCfg PersistenceConfig `yaml:"persistence"`
	StatsCfg   StatsConfig       `yaml:"stats"`
	WebCfg     WebConfig         `yaml:"web"`
}

type CacheConfig struct {
	TTLMin          int  `yaml:"ttl_min"`
	TTLMax          int  `yaml:"ttl_max"`
	RefreshInterval int  `yaml:"refresh_interval"`
	StaleServing    bool `yaml:"stale_serving"`
	MaxEntries      int  `yaml:"max_entries"`
	// MaxAge, in secondi: oltre il TTL una risposta si serve ancora, e intanto
	// si rinfresca in background, finche' ha meno di max_age. 0 = mai oltre
	// il TTL.
	MaxAge int `yaml:"max_age"`
	// Domini abituali, tenuti aggiornati anche quando non si usano: chiesti in
	// almeno keep_min_days degli ultimi keep_window_days giorni.
	KeepMinDays    int `yaml:"keep_min_days"`
	KeepWindowDays int `yaml:"keep_window_days"`
}

type PersistenceConfig struct {
	DBPath       string `yaml:"db_path"`
	CleanupAfter int    `yaml:"cleanup_after_hours"`
}

type StatsConfig struct {
	SocketPath string `yaml:"socket_path"`
}

type WebConfig struct {
	Listen string `yaml:"listen"`
}

func DefaultConfig() *Config {
	return &Config{
		Listen:    ":53",
		Upstreams: []string{"1.1.1.1:53", "9.9.9.9:53"},
		CacheCfg: CacheConfig{
			TTLMin:          60,
			TTLMax:          86400,
			RefreshInterval: 30,
			StaleServing:    true,
			MaxEntries:      10000,
			MaxAge:          1800,
			KeepMinDays:     2,
			KeepWindowDays:  7,
		},
		PersistCfg: PersistenceConfig{
			DBPath:       "/var/cache/dns-cache/cache.db",
			CleanupAfter: 48,
		},
		StatsCfg: StatsConfig{
			SocketPath: "/var/run/dns-cache.sock",
		},
		WebCfg: WebConfig{
			Listen: ":8053",
		},
	}
}

func LoadConfig(path string) (*Config, error) {
	cfg := DefaultConfig()
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, err
	}
	cfg.normalize()
	return cfg, nil
}

// normalize riporta nei limiti i valori che il codice non puo' usare come
// sono, invece di rifiutare l'avvio di un server DNS per una chiave sbagliata.
func (c *Config) normalize() {
	cc := &c.CacheCfg
	// La maschera dei giorni d'uso e' di 32 bit (cache.Entry.UsedDays).
	if cc.KeepWindowDays < 1 || cc.KeepWindowDays > 32 {
		log.Printf("[config] keep_window_days %d out of [1, 32], using 7", cc.KeepWindowDays)
		cc.KeepWindowDays = 7
	}
	if cc.KeepMinDays < 1 || cc.KeepMinDays > cc.KeepWindowDays {
		v := min(max(cc.KeepMinDays, 1), cc.KeepWindowDays)
		log.Printf("[config] keep_min_days %d out of [1, %d], using %d", cc.KeepMinDays, cc.KeepWindowDays, v)
		cc.KeepMinDays = v
	}
	if cc.MaxAge < 0 {
		log.Printf("[config] max_age %d is negative, using 0 (never past TTL)", cc.MaxAge)
		cc.MaxAge = 0
	}
}

func (c *Config) CacheTTLMin() time.Duration {
	return time.Duration(c.CacheCfg.TTLMin) * time.Second
}

func (c *Config) CacheTTLMax() time.Duration {
	return time.Duration(c.CacheCfg.TTLMax) * time.Second
}

func (c *Config) CacheRefreshInterval() time.Duration {
	return time.Duration(c.CacheCfg.RefreshInterval) * time.Second
}

func (c *Config) CacheMaxAge() time.Duration {
	return time.Duration(c.CacheCfg.MaxAge) * time.Second
}
