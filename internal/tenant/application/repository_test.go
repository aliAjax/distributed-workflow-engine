package application

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/tenant/domain"
)

type tenantFakeRepo struct {
	key domain.APIKey
	err error
}

func (r *tenantFakeRepo) CreateTenant(context.Context, domain.Tenant) error { return nil }
func (r *tenantFakeRepo) GetTenant(context.Context, string) (domain.Tenant, error) {
	return domain.Tenant{}, context.DeadlineExceeded
}
func (r *tenantFakeRepo) GetTenantBySlug(context.Context, string) (domain.Tenant, error) {
	return domain.Tenant{}, context.DeadlineExceeded
}
func (r *tenantFakeRepo) ListTenants(context.Context, int, int) ([]domain.Tenant, error) { return nil, nil }
func (r *tenantFakeRepo) CreateProject(context.Context, domain.Project) error          { return nil }
func (r *tenantFakeRepo) GetProject(context.Context, string, string) (domain.Project, error) {
	return domain.Project{}, context.DeadlineExceeded
}
func (r *tenantFakeRepo) ListProjects(context.Context, string) ([]domain.Project, error) {
	return nil, nil
}
func (r *tenantFakeRepo) CreateAPIKey(context.Context, domain.APIKey) error { return nil }
func (r *tenantFakeRepo) GetAPIKeyByHash(context.Context, string) (domain.APIKey, error) {
	return r.key, r.err
}
func (r *tenantFakeRepo) UpdateAPIKeyLastUsed(context.Context, string) error { return nil }

func TestTenantAuthenticateRepoErrorChain(t *testing.T) {
	sentinel := errors.New("repo failed")
	svc := NewService(&tenantFakeRepo{err: sentinel})
	_, err := svc.Authenticate(context.Background(), "secret")
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped repository error, got %v", err)
	}
}

func TestTenantAuthenticateExpiredSentinel(t *testing.T) {
	expired := time.Now().Add(-time.Hour)
	svc := NewService(&tenantFakeRepo{key: domain.APIKey{ID: "key-a", TenantID: "tenant-a", Name: "expired", ExpiresAt: &expired}})
	_, err := svc.Authenticate(context.Background(), "secret")
	if !errors.Is(err, domain.ErrAPIKeyExpired) {
		t.Fatalf("expected expired sentinel, got %v", err)
	}
}
