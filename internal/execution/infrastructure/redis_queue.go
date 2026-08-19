package infrastructure

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/acme/distributed-workflow-engine/internal/execution/application"
)

const (
	queuePrefix    = "workflow:queue:"
	inflightPrefix = "workflow:inflight:"
	queuePopScript = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local members = redis.call('ZRANGEBYSCORE', key, 0, now, 'LIMIT', 0, limit)
if #members == 0 then return nil end
local member = members[1]
redis.call('ZREM', key, member)
return member
`
)

type RedisQueue struct {
	client *redis.Client
}

func NewRedisQueue(client *redis.Client) *RedisQueue {
	return &RedisQueue{client: client}
}

func (q *RedisQueue) EnqueueNode(ctx context.Context, tenantID, executionID, nodeID string, priority int, availableAt time.Time) error {
	score := float64(availableAt.UnixMicro()) + float64(priority)
	_, err := q.client.ZAdd(ctx, queueKey(tenantID), redis.Z{
		Score:  score,
		Member: queueMember(executionID, nodeID),
	}).Result()
	if err != nil {
		return fmt.Errorf("enqueue node: %w", err)
	}
	return nil
}

func (q *RedisQueue) DequeueNode(ctx context.Context, tenantID string, now time.Time) (application.QueueNode, error) {
	member, err := q.client.Eval(ctx, queuePopScript, []string{queueKey(tenantID)}, now.UnixMicro(), 1).Text()
	if err == redis.Nil {
		return application.QueueNode{}, application.ErrQueueEmpty
	}
	if err != nil {
		return application.QueueNode{}, fmt.Errorf("dequeue node: %w", err)
	}
	executionID, nodeID, ok := parseQueueMember(member)
	if !ok {
		return application.QueueNode{}, fmt.Errorf("invalid queue member %q", member)
	}
	item := application.QueueNode{
		ExecutionID: executionID,
		NodeID:      nodeID,
		TenantID:    tenantID,
		AvailableAt: now,
	}
	if err := q.client.Set(ctx, inflightKey(tenantID, executionID, nodeID), member, 2*time.Minute).Err(); err != nil {
		return application.QueueNode{}, fmt.Errorf("mark node inflight: %w", err)
	}
	return item, nil
}

func (q *RedisQueue) AckNode(ctx context.Context, tenantID, executionID, nodeID string) error {
	if err := q.client.Del(ctx, inflightKey(tenantID, executionID, nodeID)).Err(); err != nil {
		return fmt.Errorf("ack node: %w", err)
	}
	return nil
}

func queueKey(tenantID string) string {
	return queuePrefix + tenantID
}

func inflightKey(tenantID, executionID, nodeID string) string {
	return inflightPrefix + tenantID + ":" + executionID + ":" + nodeID
}

func queueMember(executionID, nodeID string) string {
	return executionID + ":" + nodeID
}

func parseQueueMember(member string) (string, string, bool) {
	parts := strings.SplitN(member, ":", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", false
	}
	return parts[0], parts[1], true
}
