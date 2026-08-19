package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/acme/distributed-workflow-engine/internal/tenant/domain"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateTenant(ctx context.Context, tenant domain.Tenant) error {
	if tenant.ID == "" {
		tenant.ID = uuid.NewString()
	}
	if tenant.Slug == "" {
		tenant.Slug = slugify(tenant.Name)
	}
	quotas, _ := json.Marshal(tenant.Quotas)
	now := time.Now().UTC()
	if tenant.CreatedAt.IsZero() {
		tenant.CreatedAt = now
	}
	if tenant.UpdatedAt.IsZero() {
		tenant.UpdatedAt = now
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO tenants (id, name, slug, status, quotas, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)`,
		tenant.ID, tenant.Name, tenant.Slug, tenant.Status, string(quotas), tenant.CreatedAt, tenant.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("tenant %q already exists: %w", tenant.Slug, err)
		}
		return fmt.Errorf("create tenant: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetTenant(ctx context.Context, id string) (domain.Tenant, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, name, slug, status, quotas, created_at, updated_at FROM tenants WHERE id=$1`, id)
	return scanTenant(row)
}

func (r *PostgresRepository) GetTenantBySlug(ctx context.Context, slug string) (domain.Tenant, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, name, slug, status, quotas, created_at, updated_at FROM tenants WHERE slug=$1`, slug)
	return scanTenant(row)
}

func (r *PostgresRepository) ListTenants(ctx context.Context, limit, offset int) ([]domain.Tenant, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, name, slug, status, quotas, created_at, updated_at FROM tenants ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
		limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list tenants: %w", err)
	}
	defer rows.Close()
	var out []domain.Tenant
	for rows.Next() {
		tenant, err := scanTenant(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, tenant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate tenants: %w", err)
	}
	return out, nil
}

func (r *PostgresRepository) CreateProject(ctx context.Context, project domain.Project) error {
	if project.ID == "" {
		project.ID = uuid.NewString()
	}
	if project.Slug == "" {
		project.Slug = slugify(project.Name)
	}
	now := time.Now().UTC()
	if project.CreatedAt.IsZero() {
		project.CreatedAt = now
	}
	if project.UpdatedAt.IsZero() {
		project.UpdatedAt = now
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO projects (id, tenant_id, name, slug, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		project.ID, project.TenantID, project.Name, project.Slug, project.CreatedAt, project.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("project %q already exists: %w", project.Slug, err)
		}
		return fmt.Errorf("create project: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetProject(ctx context.Context, tenantID, id string) (domain.Project, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, name, slug, created_at, updated_at FROM projects WHERE tenant_id=$1 AND id=$2`, tenantID, id)
	var p domain.Project
	if err := row.Scan(&p.ID, &p.TenantID, &p.Name, &p.Slug, &p.CreatedAt, &p.UpdatedAt); err != nil {
		return p, wrapNotFound("project", err)
	}
	return p, nil
}

func (r *PostgresRepository) ListProjects(ctx context.Context, tenantID string) ([]domain.Project, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, name, slug, created_at, updated_at FROM projects WHERE tenant_id=$1 ORDER BY created_at DESC`, tenantID)
	if err != nil {
		return nil, fmt.Errorf("list projects: %w", err)
	}
	defer rows.Close()
	var out []domain.Project
	for rows.Next() {
		var p domain.Project
		if err := rows.Scan(&p.ID, &p.TenantID, &p.Name, &p.Slug, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan project: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) CreateAPIKey(ctx context.Context, key domain.APIKey) error {
	if key.ID == "" {
		key.ID = uuid.NewString()
	}
	roles, _ := json.Marshal(key.Roles)
	scopes, _ := json.Marshal(key.Scopes)
	if key.CreatedAt.IsZero() {
		key.CreatedAt = time.Now().UTC()
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO api_keys (id, tenant_id, project_id, name, key_hash, roles, scopes, expires_at, created_at)
		VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9)`,
		key.ID, key.TenantID, nullableString(key.ProjectID), key.Name, key.KeyHash, string(roles), string(scopes), key.ExpiresAt, key.CreatedAt)
	if err != nil {
		return fmt.Errorf("create api key: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetAPIKeyByHash(ctx context.Context, hash string) (domain.APIKey, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, COALESCE(project_id::text,''), name, key_hash, roles, scopes, expires_at, created_at, last_used_at
		FROM api_keys WHERE key_hash=$1`, hash)
	var key domain.APIKey
	var rolesRaw, scopesRaw []byte
	var projectID string
	if err := row.Scan(&key.ID, &key.TenantID, &projectID, &key.Name, &key.KeyHash, &rolesRaw, &scopesRaw, &key.ExpiresAt, &key.CreatedAt, &key.LastUsedAt); err != nil {
		return key, wrapNotFound("api key", err)
	}
	key.ProjectID = projectID
	if err := json.Unmarshal(rolesRaw, &key.Roles); err != nil {
		return key, fmt.Errorf("unmarshal roles: %w", err)
	}
	if err := json.Unmarshal(scopesRaw, &key.Scopes); err != nil {
		return key, fmt.Errorf("unmarshal scopes: %w", err)
	}
	return key, nil
}

func (r *PostgresRepository) UpdateAPIKeyLastUsed(ctx context.Context, keyID string) error {
	_, err := r.db.ExecContext(ctx, `UPDATE api_keys SET last_used_at=NOW() WHERE id=$1`, keyID)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTenant(row scanner) (domain.Tenant, error) {
	var t domain.Tenant
	var raw []byte
	if err := row.Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &raw, &t.CreatedAt, &t.UpdatedAt); err != nil {
		return t, wrapNotFound("tenant", err)
	}
	if err := json.Unmarshal(raw, &t.Quotas); err != nil {
		return t, fmt.Errorf("unmarshal tenant quotas: %w", err)
	}
	return t, nil
}

func wrapNotFound(name string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s not found: %w", name, err)
	}
	return fmt.Errorf("get %s: %w", name, err)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func slugify(value string) string {
	return strings.ToLower(strings.ReplaceAll(value, " ", "-"))
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
