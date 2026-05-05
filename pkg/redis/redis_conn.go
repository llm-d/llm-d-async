package redis

import (
	"flag"
	"fmt"
	"os"

	"github.com/redis/go-redis/v9"
)

var (
	RedisURL = flag.String("redis.url", os.Getenv("REDIS_URL"), "Redis URL (e.g. redis://user:pass@host:port/db or rediss://... for TLS)")
)

// RedisOptions returns *redis.Options derived from the --redis.url flag
// (or REDIS_URL env var).
func RedisOptions() (*redis.Options, error) {
	if *RedisURL == "" {
		return nil, fmt.Errorf("--redis.url (or REDIS_URL env var) is required")
	}
	opts, err := redis.ParseURL(*RedisURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse --redis.url: %w", err)
	}
	return opts, nil
}
