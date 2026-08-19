package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/tenant/domain"
)

type Repository interface {
	CreateTenant(ctx context.Context, tenant domain.Tenant) error
	GetTenant(ctx context.Context, id string) (domain.Tenant, error)
	GetTenantBySlug(ctx context.Context, slug string) (domain.Tenant, error)
	ListTenants(ctx context.Context, limit, offset int) ([]domain.Tenant, error)
	CreateProject(ctx context.Context, project domain.Project) error
	GetProject(ctx context.Context, tenantID, id string) (domain.Project, error)
	ListProjects(ctx context.Context, tenantID string) ([]domain.Project, error)
	CreateAPIKey(ctx context.Context, key domain.APIKey) error
	GetAPIKeyByHash(ctx context.Context, hash string) (domain.APIKey, error)
	UpdateAPIKeyLastUsed(ctx context.Context, keyID string) error
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) CreateTenant(ctx context.Context, tenant domain.Tenant) (domain.Tenant, error) {
	if err := tenant.Validate(); err != nil {
		return domain.Tenant{}, err
	}
	if err := s.repo.CreateTenant(ctx, tenant); err != nil {
		return domain.Tenant{}, err
	}
	return tenant, nil
}

func (s *Service) GetTenant(ctx context.Context, id string) (domain.Tenant, error) {
	return s.repo.GetTenant(ctx, id)
}

func (s *Service) GetTenantBySlug(ctx context.Context, slug string) (domain.Tenant, error) {
	return s.repo.GetTenantBySlug(ctx, slug)
}

func (s *Service) ListTenants(ctx context.Context, limit, offset int) ([]domain.Tenant, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	return s.repo.ListTenants(ctx, limit, offset)
}

func (s *Service) CreateProject(ctx context.Context, project domain.Project) (domain.Project, error) {
	if err := project.Validate(); err != nil {
		return domain.Project{}, err
	}
	if err := s.repo.CreateProject(ctx, project); err != nil {
		return domain.Project{}, err
	}
	return project, nil
}

func (s *Service) GetProject(ctx context.Context, tenantID, id string) (domain.Project, error) {
	return s.repo.GetProject(ctx, tenantID, id)
}

func (s *Service) ListProjects(ctx context.Context, tenantID string) ([]domain.Project, error) {
	return s.repo.ListProjects(ctx, tenantID)
}

func (s *Service) CreateAPIKey(ctx context.Context, key domain.APIKey) (domain.APIKey, error) {
	if err := key.Validate(); err != nil {
		return domain.APIKey{}, err
	}
	if err := s.repo.CreateAPIKey(ctx, key); err != nil {
		return domain.APIKey{}, err
	}
	return key, nil
}

func (s *Service) Authenticate(ctx context.Context, apiKey string) (domain.Principal, error) {
	if apiKey == "" {
		return domain.Principal{}, fmt.Errorf("missing api key")
	}
	key, err := s.repo.GetAPIKeyByHash(ctx, HashKey(apiKey))
	if err != nil {
		return domain.Principal{}, fmt.Errorf("authenticate api key: %w", err)
	}
	if key.ExpiresAt != nil && key.ExpiresAt.Before(now()) {
		return domain.Principal{}, domain.ErrAPIKeyExpired
	}
	if err := s.repo.UpdateAPIKeyLastUsed(ctx, key.ID); err != nil {
		return domain.Principal{}, err
	}
	return domain.Principal{
		TenantID:  key.TenantID,
		ProjectID: key.ProjectID,
		KeyID:     key.ID,
		Roles:     key.Roles,
		Scopes:    key.Scopes,
	}, nil
}

func (s *Service) Authorize(principal domain.Principal, resource, action string) error {
	if principal.TenantID == "" {
		return fmt.Errorf("unauthenticated")
	}
	if !principal.Can(resource, action) {
		return fmt.Errorf("permission denied: %s:%s", resource, action)
	}
	return nil
}

func HashKey(apiKey string) string {
	// This is separated so the storage adapter can be tested with a
	// stable value while production uses a keyed digest.
	return sha256Hex(apiKey)
}

func now() time.Time {
	return time.Now().UTC()
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}
