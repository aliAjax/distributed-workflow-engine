package domain

import (
	"errors"
	"strings"
	"time"
)

type Event struct {
	ID        string         `json:"id"`
	TenantID  string         `json:"tenant_id"`
	ProjectID string         `json:"project_id,omitempty"`
	Type      string         `json:"type"`
	Source    string         `json:"source"`
	SubjectID string         `json:"subject_id,omitempty"`
	Payload   map[string]any `json:"payload,omitempty"`
	CreatedAt time.Time      `json:"created_at"`
}

type ExternalSignal struct {
	TenantID    string         `json:"tenant_id"`
	ProjectID   string         `json:"project_id,omitempty"`
	Event       string         `json:"event"`
	ExecutionID string         `json:"execution_id,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
}

var (
	ErrTenantRequired         = errors.New("event tenant id is required")
	ErrEventTypeRequired      = errors.New("event type is required")
	ErrExternalTenantRequired = errors.New("external signal tenant id is required")
	ErrExternalEventRequired  = errors.New("external signal event is required")
)

func (e Event) Validate() error {
	if e.TenantID == "" {
		return ErrTenantRequired
	}
	if strings.TrimSpace(e.Type) == "" {
		return ErrEventTypeRequired
	}
	return nil
}

func (s ExternalSignal) Validate() error {
	if s.TenantID == "" {
		return ErrExternalTenantRequired
	}
	if strings.TrimSpace(s.Event) == "" {
		return ErrExternalEventRequired
	}
	return nil
}
