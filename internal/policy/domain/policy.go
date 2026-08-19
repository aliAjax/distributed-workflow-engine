package domain

import (
	"errors"
	"fmt"
	"time"
)

type Quota struct {
	Key       string        `json:"key"`
	Limit     int           `json:"limit"`
	Window    time.Duration `json:"window,omitempty"`
	Used      int           `json:"used"`
	ExpiresAt time.Time     `json:"expires_at,omitempty"`
}

type ConcurrencyPolicy struct {
	WorkflowLimit int `json:"workflow_limit"`
	TenantLimit   int `json:"tenant_limit"`
	ProjectLimit  int `json:"project_limit"`
}

type RetryConfig struct {
	MaxAttempts    int           `json:"max_attempts"`
	InitialBackoff time.Duration `json:"initial_backoff"`
	MaxBackoff     time.Duration `json:"max_backoff"`
	Multiplier     float64       `json:"multiplier"`
}

type BackoffConfig struct {
	Initial time.Duration `json:"initial"`
	Max     time.Duration `json:"max"`
	Factor  float64       `json:"factor"`
}

type RateLimitConfig struct {
	Requests int           `json:"requests"`
	Window   time.Duration `json:"window"`
}

func (r RetryConfig) Validate() error {
	if r.MaxAttempts < 0 {
		return errors.New("max_attempts cannot be negative")
	}
	if r.Multiplier < 0 {
		return errors.New("multiplier cannot be negative")
	}
	if r.InitialBackoff < 0 || r.MaxBackoff < 0 {
		return errors.New("backoff durations cannot be negative")
	}
	if r.InitialBackoff > r.MaxBackoff && r.MaxBackoff != 0 {
		return fmt.Errorf("initial backoff %s exceeds max backoff %s", r.InitialBackoff, r.MaxBackoff)
	}
	return nil
}

func (b BackoffConfig) ForAttempt(attempt int) time.Duration {
	if attempt <= 1 || b.Factor <= 0 {
		return b.Initial
	}
	delay := b.Initial
	for i := 2; i <= attempt; i++ {
		delay = time.Duration(float64(delay) * b.Factor)
		if delay < b.Initial {
			delay = b.Initial
		}
		if b.Max > 0 && delay > b.Max {
			return b.Max
		}
	}
	return delay
}
