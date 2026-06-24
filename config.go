package main

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Listen      string           `yaml:"listen"`
	Upstreams   []string         `yaml:"upstreams"`
	CacheCfg    CacheConfig      `yaml:"cache"`
	PersistCfg  PersistenceConfig `yaml:"persistence"`
	StatsCfg    StatsConfig      `yaml:"stats"`
	WebCfg      WebConfig        `yaml:"web"`
}

type CacheConfig struct {
	TTLMin          int  `yaml:"ttl_min"`
	TTLMax          int  `yaml:"ttl_max"`
	RefreshInterval int  `yaml:"refresh_interval"`
	StaleServing    bool `yaml:"stale_serving"`
	MaxEntries      int  `yaml:"max_entries"`
}

type PersistenceConfig struct {
	DBPath        string `yaml:"db_path"`
	CleanupAfter  int    `yaml:"cleanup_after_hours"`
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
	return cfg, nil
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
