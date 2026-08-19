package application

import (
	"context"
	"testing"
)

type countingChecker struct {
	called int
}

func (c *countingChecker) Name() string             { return "counting" }
func (c *countingChecker) Check(context.Context) error { c.called++; return nil }

func TestHealthCancelStopsCheckers(t *testing.T) {
	checker := &countingChecker{}
	svc := NewService(checker)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := svc.Health(ctx)
	if checker.called != 0 {
		t.Fatalf("expected no checker calls on canceled context, got %d", checker.called)
	}
	if report.Status == "" {
		t.Fatal("expected a health report status")
	}
}

func TestReadyCancelStopsCheckers(t *testing.T) {
	checker := &countingChecker{}
	svc := NewService(checker)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := svc.Ready(ctx)
	if checker.called != 0 {
		t.Fatalf("expected no checker calls on canceled context, got %d", checker.called)
	}
	if report.Status == "" {
		t.Fatal("expected a ready report status")
	}
}
