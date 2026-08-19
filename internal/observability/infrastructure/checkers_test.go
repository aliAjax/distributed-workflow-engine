package infrastructure

import (
	"context"
	"errors"
	"testing"
)

func TestHealthCheckerRespectsParentDeadline(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	cancel()
	err := withCheckTimeout(parent, func(ctx context.Context) error {
		if ctx.Err() == nil {
			return errors.New("parent cancellation was not propagated")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected canceled parent context to propagate: %v", err)
	}
}
