package domain

import (
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
