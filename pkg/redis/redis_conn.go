package redis

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Redis topologies accepted by the transport configs' mode field.
const (
	ModeStandalone = "standalone"
	ModeCluster    = "cluster"
	ModeSentinel   = "sentinel"
)

// ConnectionConfig is the topology section shared by the Redis transports.
// URL carries credentials, TLS, the database and client tuning for every
// mode. Mode selects the go-redis client (empty means standalone). Addrs adds
// cluster seed nodes or sentinel addresses beyond the one in URL, and
// MasterName names the sentinel-monitored master.
type ConnectionConfig struct {
	URL        string
	Mode       string
	Addrs      []string
	MasterName string
}

// validateConnection checks the topology fields against each other. URL
// presence is checked by the callers because their wording differs.
func validateConnection(c ConnectionConfig) error {
	switch c.Mode {
	case "", ModeStandalone:
		if len(c.Addrs) > 0 {
			return fmt.Errorf("addrs requires mode %q or %q", ModeCluster, ModeSentinel)
		}
		if c.MasterName != "" {
			return fmt.Errorf("master_name requires mode %q", ModeSentinel)
		}
	case ModeCluster:
		if c.MasterName != "" {
			return fmt.Errorf("master_name requires mode %q", ModeSentinel)
		}
	case ModeSentinel:
		if c.MasterName == "" {
			return fmt.Errorf("master_name is required when mode is %q", ModeSentinel)
		}
	default:
		return fmt.Errorf("unknown mode %q (expected %q, %q or %q)", c.Mode, ModeStandalone, ModeCluster, ModeSentinel)
	}
	return nil
}

// universalOptions maps the URL-derived client options plus the topology
// fields onto go-redis universal options, so one code path yields a
// standalone, cluster or failover client.
func universalOptions(c ConnectionConfig) (*redis.UniversalOptions, error) {
	if c.URL == "" {
		return nil, fmt.Errorf("--redis.url (or REDIS_URL env var) is required")
	}
	if err := validateConnection(c); err != nil {
		return nil, err
	}
	o, err := redis.ParseURL(c.URL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse --redis.url: %w", err)
	}
	if c.Mode == ModeCluster && o.DB != 0 {
		return nil, fmt.Errorf("mode %q does not support selecting database %d; use database 0 in url", ModeCluster, o.DB)
	}
	return &redis.UniversalOptions{
		Addrs:                 append([]string{o.Addr}, c.Addrs...),
		ClientName:            o.ClientName,
		DB:                    o.DB,
		Protocol:              o.Protocol,
		Username:              o.Username,
		Password:              o.Password,
		MaxRetries:            o.MaxRetries,
		MinRetryBackoff:       o.MinRetryBackoff,
		MaxRetryBackoff:       o.MaxRetryBackoff,
		DialTimeout:           o.DialTimeout,
		ReadTimeout:           o.ReadTimeout,
		WriteTimeout:          o.WriteTimeout,
		PoolFIFO:              o.PoolFIFO,
		PoolSize:              o.PoolSize,
		PoolTimeout:           o.PoolTimeout,
		MinIdleConns:          o.MinIdleConns,
		MaxIdleConns:          o.MaxIdleConns,
		MaxActiveConns:        o.MaxActiveConns,
		MaxConcurrentDials:    o.MaxConcurrentDials,
		ConnMaxIdleTime:       o.ConnMaxIdleTime,
		ConnMaxLifetime:       o.ConnMaxLifetime,
		ConnMaxLifetimeJitter: o.ConnMaxLifetimeJitter,
		TLSConfig:             o.TLSConfig,
		IsClusterMode:         c.Mode == ModeCluster,
		MasterName:            c.MasterName,
	}, nil
}

// newUniversalClient builds the go-redis client for the configured topology.
func newUniversalClient(c ConnectionConfig) (redis.UniversalClient, error) {
	opts, err := universalOptions(c)
	if err != nil {
		return nil, err
	}
	return redis.NewUniversalClient(opts), nil
}
