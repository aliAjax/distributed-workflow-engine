package domain

import (
	"testing"
	"time"
)

func TestPolicyBackoffZeroFactor(t *testing.T) {
	config := BackoffConfig{Initial: time.Second, Max: 10 * time.Second, Factor: 0}
	got := config.ForAttempt(3)
	if got != time.Second {
		t.Fatalf("expected initial backoff with zero factor, got %s", got)
	}
}

func TestPolicyRetryMultiplierValidation(t *testing.T) {
	config := RetryConfig{MaxAttempts: 3, InitialBackoff: time.Second, MaxBackoff: time.Second, Multiplier: -1}
	if err := config.Validate(); err == nil {
		t.Fatal("expected negative multiplier to be rejected")
	}
}
