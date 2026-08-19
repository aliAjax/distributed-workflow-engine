package infrastructure

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const acquireScript = `
local value = redis.call('INCR', KEYS[1])
if value == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end
if value > tonumber(ARGV[2]) then
  redis.call('DECR', KEYS[1])
  return {0, value-1}
end
return {1, value}
`

const leaseScript = `
local current = redis.call('GET', KEYS[1])
if current == false or current == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
  return 1
end
return 0
`

const renewScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
return 0
`

const releaseScript = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`

type RedisStore struct {
	client *redis.Client
}

func NewRedisStore(client *redis.Client) *RedisStore {
	return &RedisStore{client: client}
}

func (s *RedisStore) Acquire(ctx context.Context, key string, limit int, ttl time.Duration) (bool, int, error) {
	values, err := s.client.Eval(ctx, acquireScript, []string{key}, ttl.Milliseconds(), limit).Int64Slice()
	if err != nil {
		return false, 0, fmt.Errorf("acquire concurrency: %w", err)
	}
	return values[0] == 1, int(values[1]), nil
}

func (s *RedisStore) Release(ctx context.Context, key string) error {
	return s.client.Decr(ctx, key).Err()
}

func (s *RedisStore) AcquireLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	value, err := s.client.Eval(ctx, leaseScript, []string{key}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("acquire lease: %w", err)
	}
	return value == 1, nil
}

func (s *RedisStore) RenewLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	value, err := s.client.Eval(ctx, renewScript, []string{key}, owner, ttl.Milliseconds()).Int()
	if err != nil {
		return false, fmt.Errorf("renew lease: %w", err)
	}
	return value == 1, nil
}

func (s *RedisStore) ReleaseLease(ctx context.Context, key, owner string) error {
	_, err := s.client.Eval(ctx, releaseScript, []string{key}, owner).Int()
	return err
}

func (s *RedisStore) RateLimit(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	pipe := s.client.Pipeline()
	now := time.Now().UnixMilli()
	z := redis.Z{Score: float64(now), Member: fmt.Sprintf("%d", now)}
	pipe.ZAdd(ctx, key, z)
	pipe.ZRemRangeByScore(ctx, key, "0", fmt.Sprintf("%d", now-window.Milliseconds()))
	pipe.ZCard(ctx, key)
	pipe.Expire(ctx, key, window)
	results, err := pipe.Exec(ctx)
	if err != nil {
		return false, fmt.Errorf("rate limit: %w", err)
	}
	count := results[2].(*redis.IntCmd).Val()
	return count <= int64(limit), nil
}

func (s *RedisStore) Delay(ctx context.Context, key string, delay time.Duration) error {
	if err := s.client.SetNX(ctx, key, 1, delay).Err(); err != nil {
		return fmt.Errorf("delay: %w", err)
	}
	return nil
}
