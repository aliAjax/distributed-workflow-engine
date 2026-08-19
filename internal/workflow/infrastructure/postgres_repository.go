package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateWorkflow(ctx context.Context, workflow domain.Workflow) error {
	if workflow.ID == "" {
		workflow.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if workflow.CreatedAt.IsZero() {
		workflow.CreatedAt = now
	}
	if workflow.UpdatedAt.IsZero() {
		workflow.UpdatedAt = now
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workflows (id, tenant_id, project_id, name, description, current_version, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		workflow.ID, workflow.TenantID, workflow.ProjectID, workflow.Name, workflow.Description,
		workflow.CurrentVersion, workflow.CreatedAt, workflow.UpdatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("workflow %q already exists: %w", workflow.Name, err)
		}
		return fmt.Errorf("create workflow: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetWorkflow(ctx context.Context, tenantID, projectID, id string) (domain.Workflow, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, project_id, name, description, current_version, created_at, updated_at
		FROM workflows WHERE tenant_id=$1 AND project_id=$2 AND id=$3`, tenantID, projectID, id)
	var w domain.Workflow
	if err := row.Scan(&w.ID, &w.TenantID, &w.ProjectID, &w.Name, &w.Description, &w.CurrentVersion, &w.CreatedAt, &w.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return w, fmt.Errorf("workflow %s not found: %w", id, err)
		}
		return w, fmt.Errorf("get workflow: %w", err)
	}
	return w, nil
}

func (r *PostgresRepository) ListWorkflows(ctx context.Context, tenantID, projectID string, limit, offset int) ([]domain.Workflow, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, project_id, name, description, current_version, created_at, updated_at
		FROM workflows WHERE tenant_id=$1 AND project_id=$2 ORDER BY created_at DESC LIMIT $3 OFFSET $4`,
		tenantID, projectID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list workflows: %w", err)
	}
	defer rows.Close()
	var out []domain.Workflow
	for rows.Next() {
		var w domain.Workflow
		if err := rows.Scan(&w.ID, &w.TenantID, &w.ProjectID, &w.Name, &w.Description, &w.CurrentVersion, &w.CreatedAt, &w.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		out = append(out, w)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflows: %w", err)
	}
	return out, nil
}

func (r *PostgresRepository) UpdateWorkflow(ctx context.Context, workflow domain.Workflow) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE workflows SET name=$2, description=$3, current_version=$4, updated_at=NOW()
		WHERE tenant_id=$5 AND project_id=$6 AND id=$1`,
		workflow.ID, workflow.Name, workflow.Description, workflow.CurrentVersion, workflow.TenantID, workflow.ProjectID)
	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}
	return nil
}

func (r *PostgresRepository) DeleteWorkflow(ctx context.Context, tenantID, projectID, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM workflows WHERE tenant_id=$1 AND project_id=$2 AND id=$3`, tenantID, projectID, id)
	if err != nil {
		return fmt.Errorf("delete workflow: %w", err)
	}
	return nil
}

func (r *PostgresRepository) CreateVersion(ctx context.Context, version domain.WorkflowVersion) error {
	if version.ID == "" {
		version.ID = uuid.NewString()
	}
	raw, err := json.Marshal(version.Definition)
	if err != nil {
		return fmt.Errorf("marshal workflow definition: %w", err)
	}
	if version.Checksum == "" {
		version.Checksum = version.Definition.Checksum()
	}
	if version.CreatedAt.IsZero() {
		version.CreatedAt = time.Now().UTC()
	}
	_, err = r.db.ExecContext(ctx, `
		INSERT INTO workflow_versions (id, workflow_id, version, definition, checksum, status, created_by, created_at)
		VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8)`,
		version.ID, version.WorkflowID, version.Version, string(raw), version.Checksum, version.Status,
		version.CreatedBy, version.CreatedAt)
	if err != nil {
		if isUniqueViolation(err) {
			return fmt.Errorf("workflow version %d already exists: %w", version.Version, err)
		}
		return fmt.Errorf("create workflow version: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetVersion(ctx context.Context, workflowID string, number int) (domain.WorkflowVersion, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, workflow_id, version, definition, checksum, status, COALESCE(created_by,''), created_at
		FROM workflow_versions WHERE workflow_id=$1 AND version=$2`, workflowID, number)
	return scanVersion(row)
}

func (r *PostgresRepository) ListVersions(ctx context.Context, workflowID string) ([]domain.WorkflowVersion, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, workflow_id, version, definition, checksum, status, COALESCE(created_by,''), created_at
		FROM workflow_versions WHERE workflow_id=$1 ORDER BY version DESC`, workflowID)
	if err != nil {
		return nil, fmt.Errorf("list workflow versions: %w", err)
	}
	defer rows.Close()
	var out []domain.WorkflowVersion
	for rows.Next() {
		version, err := scanVersion(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate workflow versions: %w", err)
	}
	return out, nil
}

func (r *PostgresRepository) GetLatestVersion(ctx context.Context, workflowID string) (domain.WorkflowVersion, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, workflow_id, version, definition, checksum, status, COALESCE(created_by,''), created_at
		FROM workflow_versions WHERE workflow_id=$1 ORDER BY version DESC LIMIT 1`, workflowID)
	return scanVersion(row)
}

type scanner interface {
	Scan(dest ...any) error
}

func scanVersion(row scanner) (domain.WorkflowVersion, error) {
	var v domain.WorkflowVersion
	var raw []byte
	if err := row.Scan(&v.ID, &v.WorkflowID, &v.Version, &raw, &v.Checksum, &v.Status, &v.CreatedBy, &v.CreatedAt); err != nil {
		return v, fmt.Errorf("scan workflow version: %w", err)
	}
	if err := json.Unmarshal(raw, &v.Definition); err != nil {
		return v, fmt.Errorf("unmarshal workflow definition: %w", err)
	}
	return v, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
