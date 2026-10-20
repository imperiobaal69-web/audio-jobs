// Package cache stores analysis results in Redis keyed by input hash and
// algorithm version, so re-uploads of the same recording (retries, duplicate
// meetings, re-processing) skip the work entirely.
package cache

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

type Results struct {
	rdb    *redis.Client
	prefix string
	ttl    time.Duration
}

func New(rdb *redis.Client, prefix string, ttl time.Duration) *Results {
	return &Results{rdb: rdb, prefix: prefix, ttl: ttl}
}

func (c *Results) key(version, sha string) string { return c.prefix + ":result:" + version + ":" + sha }

// Get returns the cached result or nil.
func (c *Results) Get(ctx context.Context, version, sha string) ([]byte, error) {
	b, err := c.rdb.Get(ctx, c.key(version, sha)).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	return b, err
}

func (c *Results) Put(ctx context.Context, version, sha string, result []byte) error {
	return c.rdb.Set(ctx, c.key(version, sha), result, c.ttl).Err()
}
