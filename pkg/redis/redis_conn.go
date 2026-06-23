package redis

import (
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ParseRedisOptions returns *redis.Options derived from the given URL.
func ParseRedisOptions(url string) (*redis.Options, error) {
	if url == "" {
		return nil, fmt.Errorf("--redis.url (or REDIS_URL env var) is required")
	}
	opts, err := redis.ParseURL(url)
	if err != nil {
		return nil, fmt.Errorf("failed to parse --redis.url: %w", err)
	}
	return opts, nil
}
