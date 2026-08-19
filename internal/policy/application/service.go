package application

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/policy/domain"
)

type Store interface {
	Acquire(ctx context.Context, key string, limit int, ttl time.Duration) (bool, int, error)
	Release(ctx context.Context, key string) error
	AcquireLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	RenewLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, key, owner string) error
	RateLimit(ctx context.Context, key string, limit int, window time.Duration) (bool, error)
	Delay(ctx context.Context, key string, delay time.Duration) error
}

type Service struct {
	store Store
}

func NewService(store Store) *Service {
	return &Service{store: store}
}

func (s *Service) AcquireTenantSlot(ctx context.Context, tenantID string, limit int) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}
	key := fmt.Sprintf("workflow:concurrency:tenant:%s", tenantID)
	ok, used, err := s.store.Acquire(ctx, key, limit, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("tenant concurrency quota exceeded: used=%d limit=%d", used, limit)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = s.store.Release(ctx, key)
		})
	}, nil
}

func (s *Service) AcquireWorkflowSlot(ctx context.Context, tenantID, workflowID string, limit int) (func(), error) {
	if limit <= 0 {
		return func() {}, nil
	}
	key := fmt.Sprintf("workflow:concurrency:workflow:%s:%s", tenantID, workflowID)
	ok, used, err := s.store.Acquire(ctx, key, limit, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("workflow concurrency quota exceeded: used=%d limit=%d", used, limit)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			_ = s.store.Release(ctx, key)
		})
	}, nil
}

func (s *Service) AcquireNodeLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return s.store.AcquireLease(ctx, key, owner, ttl)
}

func (s *Service) RenewNodeLease(ctx context.Context, key, owner string, ttl time.Duration) (bool, error) {
	return s.store.RenewLease(ctx, key, owner, ttl)
}

func (s *Service) ReleaseNodeLease(ctx context.Context, key, owner string) error {
	return s.store.ReleaseLease(ctx, key, owner)
}

func (s *Service) RateLimit(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	return s.store.RateLimit(ctx, key, limit, window)
}

func (s *Service) Delay(ctx context.Context, key string, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	return s.store.Delay(ctx, key, delay)
}

func (s *Service) Backoff(config domain.BackoffConfig, attempt int) time.Duration {
	return config.ForAttempt(attempt)
}
