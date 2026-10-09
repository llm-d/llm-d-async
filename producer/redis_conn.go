package producer

import (
	"errors"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// Redis topologies accepted by RedisSortedSetConfig.RedisMode. They mirror
// the dispatcher's transport-config mode values.
const (
	RedisModeStandalone = "standalone"
	RedisModeCluster    = "cluster"
	RedisModeSentinel   = "sentinel"
)

// newRedisClient builds the go-redis client for the configured topology. The
// URL carries credentials, TLS, the database and client tuning for every
// mode; RedisAddrs adds cluster seed nodes or sentinel addresses beyond the
// one in the URL, and RedisMasterName names the sentinel-monitored master.
func newRedisClient(config RedisSortedSetConfig) (redis.UniversalClient, error) {
	switch config.RedisMode {
	case "", RedisModeStandalone:
		if len(config.RedisAddrs) > 0 {
			return nil, fmt.Errorf("RedisAddrs requires RedisMode %q or %q", RedisModeCluster, RedisModeSentinel)
		}
		if config.RedisMasterName != "" {
			return nil, fmt.Errorf("RedisMasterName requires RedisMode %q", RedisModeSentinel)
		}
	case RedisModeCluster:
		if config.RedisMasterName != "" {
			return nil, fmt.Errorf("RedisMasterName requires RedisMode %q", RedisModeSentinel)
		}
	case RedisModeSentinel:
		if config.RedisMasterName == "" {
			return nil, fmt.Errorf("RedisMasterName is required when RedisMode is %q", RedisModeSentinel)
		}
	default:
		return nil, fmt.Errorf("unknown RedisMode %q (expected %q, %q or %q)", config.RedisMode, RedisModeStandalone, RedisModeCluster, RedisModeSentinel)
	}
	if config.RedisURL == "" {
		return nil, errors.New("RedisURL is required when no RedisClient is provided via WithRedisClient")
	}
	o, err := redis.ParseURL(config.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse RedisURL: %w", err)
	}
	if config.RedisMode == RedisModeCluster && o.DB != 0 {
		return nil, fmt.Errorf("RedisMode %q does not support selecting database %d; use database 0 in RedisURL", RedisModeCluster, o.DB)
	}
	return redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:                 append([]string{o.Addr}, config.RedisAddrs...),
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
		IsClusterMode:         config.RedisMode == RedisModeCluster,
		MasterName:            config.RedisMasterName,
	}), nil
}

// isClusterClient reports whether the client speaks to a Redis Cluster, in
// which case private bookkeeping keys must be hash-tagged into the slot of
// the shared key they belong to. Detected from the client rather than the
// config so an injected client gets the right layout too.
func isClusterClient(client redis.UniversalClient) bool {
	_, ok := client.(*redis.ClusterClient)
	return ok
}

// slotKey returns the form of a shared key to embed in the names of its
// private bookkeeping keys so they hash to the same cluster slot. Outside
// cluster mode it is the key itself, preserving the names standalone
// deployments already hold. In cluster mode it is wrapped in a hash tag
// unless it already carries one, because "{p}" hashes exactly like the bare
// key "p"; the shared key itself keeps its name.
func slotKey(primary string, cluster bool) string {
	if !cluster || hasHashTag(primary) {
		return primary
	}
	return "{" + primary + "}"
}

// hasHashTag reports whether key contains a non-empty {...} hash tag.
func hasHashTag(key string) bool {
	start := strings.IndexByte(key, '{')
	if start < 0 {
		return false
	}
	end := strings.IndexByte(key[start+1:], '}')
	return end > 0
}
