package infrastructure

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"

	"github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
)

type RedisPublisher struct {
	client *redis.Client
}

func NewRedisPublisher(client *redis.Client) *RedisPublisher {
	return &RedisPublisher{client: client}
}

func (p *RedisPublisher) Publish(ctx context.Context, event domain.Event) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	channel := channelName(event.TenantID, event.Type)
	if err := p.client.Publish(ctx, channel, raw).Err(); err != nil {
		return fmt.Errorf("redis publish event: %w", err)
	}
	return nil
}

func channelName(tenantID, eventType string) string {
	return fmt.Sprintf("workflow:events:%s:%s", tenantID, eventType)
}
