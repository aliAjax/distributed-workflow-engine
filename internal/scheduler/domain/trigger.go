package domain

import (
	"errors"
	"strings"
	"time"
)

type TriggerType string

const (
	TriggerCron   TriggerType = "cron"
	TriggerManual TriggerType = "manual"
	TriggerEvent  TriggerType = "event"
)

type Trigger struct {
	ID         string      `json:"id"`
	TenantID   string      `json:"tenant_id"`
	ProjectID  string      `json:"project_id"`
	WorkflowID string      `json:"workflow_id"`
	Version    int         `json:"version"`
	Type       TriggerType `json:"type"`
	Cron       string      `json:"cron,omitempty"`
	Event      string      `json:"event,omitempty"`
	Enabled    bool        `json:"enabled"`
	NextRunAt  *time.Time  `json:"next_run_at,omitempty"`
	LastRunAt  *time.Time  `json:"last_run_at,omitempty"`
	CreatedAt  time.Time   `json:"created_at"`
	UpdatedAt  time.Time   `json:"updated_at"`
}

type TriggerRequest struct {
	WorkflowID     string         `json:"workflow_id"`
	Version        int            `json:"version"`
	Type           TriggerType    `json:"type"`
	Cron           string         `json:"cron,omitempty"`
	Event          string         `json:"event,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	Priority       int            `json:"priority"`
}

// Validate ensures the trigger has the fields required for its type.
func (t Trigger) Validate() error {
	if strings.TrimSpace(t.WorkflowID) == "" {
		return errors.New("workflow id is required")
	}
	switch t.Type {
	case TriggerCron:
		if strings.TrimSpace(t.Cron) == "" {
			return errors.New("cron trigger requires a cron expression")
		}
	case TriggerEvent:
		if strings.TrimSpace(t.Event) == "" {
			return errors.New("event trigger requires an event name")
		}
	case TriggerManual:
		// no extra fields required
	default:
		return errors.New("unknown trigger type")
	}
	return nil
}
