package domain

import (
	"errors"
	"strings"
	"time"
)

type Tenant struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	Status    string    `json:"status"`
	Quotas    QuotaSet  `json:"quotas"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Project struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	Name      string    `json:"name"`
	Slug      string    `json:"slug"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type APIKey struct {
	ID         string     `json:"id"`
	TenantID   string     `json:"tenant_id"`
	ProjectID  string     `json:"project_id,omitempty"`
	Name       string     `json:"name"`
	KeyHash    string     `json:"-"`
	Roles      []string   `json:"roles"`
	Scopes     []string   `json:"scopes"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at,omitempty"`
}

type Principal struct {
	TenantID  string   `json:"tenant_id"`
	ProjectID string   `json:"project_id,omitempty"`
	KeyID     string   `json:"key_id"`
	Roles     []string `json:"roles"`
	Scopes    []string `json:"scopes"`
}

type Role string

const (
	RoleAdmin    Role = "admin"
	RoleOperator Role = "operator"
	RoleViewer   Role = "viewer"
)

type Permission struct {
	Resource string `json:"resource"`
	Action   string `json:"action"`
}

type QuotaSet struct {
	MaxWorkflows  int `json:"max_workflows"`
	MaxExecutions int `json:"max_executions"`
	MaxConcurrent int `json:"max_concurrent"`
	MaxProjects   int `json:"max_projects"`
}

var ErrAPIKeyExpired = errors.New("api key is expired")

func (t Tenant) Validate() error {
	if strings.TrimSpace(t.Name) == "" {
		return errors.New("tenant name is required")
	}
	if t.Quotas.MaxConcurrent < 0 {
		return errors.New("tenant max_concurrent quota cannot be negative")
	}
	return nil
}

func (p Project) Validate() error {
	if strings.TrimSpace(p.Name) == "" {
		return errors.New("project name is required")
	}
	if strings.TrimSpace(p.TenantID) == "" {
		return errors.New("project tenant id is required")
	}
	return nil
}

func (p Principal) HasRole(role Role) bool {
	for _, candidate := range p.Roles {
		if strings.EqualFold(candidate, string(role)) {
			return true
		}
	}
	return false
}

func (p Principal) Can(resource, action string) bool {
	if p.HasRole(RoleAdmin) {
		return true
	}
	for _, scope := range p.Scopes {
		if p.scopeMatches(scope, resource, action) {
			return true
		}
	}
	return false
}

func (p Principal) scopeMatches(scope, resource, action string) bool {
	parts := strings.Split(scope, ":")
	if len(parts) != 2 {
		return false
	}
	return parts[0] == resource && (parts[1] == "*" || parts[1] == action)
}

func (k APIKey) Validate() error {
	if strings.TrimSpace(k.Name) == "" {
		return errors.New("api key name is required")
	}
	if k.TenantID == "" {
		return errors.New("api key tenant id is required")
	}
	if k.ExpiresAt != nil && k.ExpiresAt.Before(time.Now()) {
		return ErrAPIKeyExpired
	}
	return nil
}
