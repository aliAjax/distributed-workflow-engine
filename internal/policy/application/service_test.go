package application

import (
	"context"
	"sync"
	"testing"
	"time"
)

type policyFakeStore struct {
	mu       sync.Mutex
	released int
}

func (s *policyFakeStore) Acquire(context.Context, string, int, time.Duration) (bool, int, error) {
	return true, 1, nil
}
func (s *policyFakeStore) Release(context.Context, string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.released++
	return nil
}
func (s *policyFakeStore) AcquireLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *policyFakeStore) RenewLease(context.Context, string, string, time.Duration) (bool, error) {
	return true, nil
}
func (s *policyFakeStore) ReleaseLease(context.Context, string, string) error { return nil }
func (s *policyFakeStore) RateLimit(context.Context, string, int, time.Duration) (bool, error) {
	return true, nil
}
func (s *policyFakeStore) Delay(context.Context, string, time.Duration) error { return nil }

func TestPolicyTenantSlotReleaseRace(t *testing.T) {
	store := &policyFakeStore{}
	svc := NewService(store)
	release, err := svc.AcquireTenantSlot(context.Background(), "tenant-a", 2)
	if err != nil {
		t.Fatalf("acquire tenant slot: %v", err)
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			release()
		}()
	}
	close(start)
	wg.Wait()
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.released != 1 {
		t.Fatalf("expected release to run exactly once, got %d", store.released)
	}
}
