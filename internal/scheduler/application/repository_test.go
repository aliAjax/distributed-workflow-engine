package application

import (
	"context"
	"testing"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/scheduler/domain"
)

type schedulerFakeRepo struct {
	due []domain.Trigger
}

func (r *schedulerFakeRepo) CreateTrigger(context.Context, domain.Trigger) error { return nil }
func (r *schedulerFakeRepo) GetTrigger(context.Context, string) (domain.Trigger, error) {
	return domain.Trigger{}, context.DeadlineExceeded
}
func (r *schedulerFakeRepo) ListTriggers(context.Context, string, string) ([]domain.Trigger, error) {
	return nil, nil
}
func (r *schedulerFakeRepo) ListDue(context.Context, time.Time, int) ([]domain.Trigger, error) {
	return r.due, nil
}
func (r *schedulerFakeRepo) UpdateTrigger(context.Context, domain.Trigger) error { return nil }
func (r *schedulerFakeRepo) DeleteTrigger(context.Context, string) error         { return nil }

type schedulerFakeExecutor struct {
	called int
}

func (e *schedulerFakeExecutor) Trigger(context.Context, domain.Trigger) error {
	e.called++
	return nil
}

func TestSchedulerTickCancelStops(t *testing.T) {
	repo := &schedulerFakeRepo{due: []domain.Trigger{
		{ID: "a", Type: domain.TriggerManual, Enabled: true},
		{ID: "b", Type: domain.TriggerManual, Enabled: true},
		{ID: "c", Type: domain.TriggerManual, Enabled: true},
	}}
	executor := &schedulerFakeExecutor{}
	svc := NewService(repo, executor)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := svc.Tick(ctx, 10); err == nil {
		t.Fatal("expected context cancellation error")
	}
	if executor.called != 0 {
		t.Fatalf("expected no triggers after cancellation, got %d", executor.called)
	}
}

func TestSchedulerEveryRejectsNonPositive(t *testing.T) {
	if _, err := NextRun("@every 0s", time.Now().UTC()); err == nil {
		t.Fatal("expected non-positive every duration to be rejected")
	}
}

func TestSchedulerMatchesFieldRejectsZeroStep(t *testing.T) {
	if matchesField("*/0", 1, 0, 59) {
		t.Fatal("expected zero step to be rejected")
	}
}
