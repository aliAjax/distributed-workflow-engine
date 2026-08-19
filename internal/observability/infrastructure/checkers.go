package infrastructure

import (
	"context"
	"database/sql"
	"time"

	"github.com/redis/go-redis/v9"
)

type PostgresChecker struct {
	db *sql.DB
}

func NewPostgresChecker(db *sql.DB) *PostgresChecker {
	return &PostgresChecker{db: db}
}

func (c *PostgresChecker) Name() string { return "postgres" }

func (c *PostgresChecker) Check(ctx context.Context) error {
	return withCheckTimeout(ctx, c.db.PingContext)
}

type RedisChecker struct {
	client *redis.Client
}

func NewRedisChecker(client *redis.Client) *RedisChecker {
	return &RedisChecker{client: client}
}

func (c *RedisChecker) Name() string { return "redis" }

func (c *RedisChecker) Check(ctx context.Context) error {
	return withCheckTimeout(ctx, func(ctx context.Context) error {
		return c.client.Ping(ctx).Err()
	})
}

func withCheckTimeout(ctx context.Context, check func(context.Context) error) error {
	child, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return check(child)
}
