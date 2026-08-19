package infrastructure

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/acme/distributed-workflow-engine/internal/scheduler/domain"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateTrigger(ctx context.Context, trigger domain.Trigger) error {
	if trigger.ID == "" {
		trigger.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if trigger.CreatedAt.IsZero() {
		trigger.CreatedAt = now
	}
	if trigger.UpdatedAt.IsZero() {
		trigger.UpdatedAt = now
	}
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO workflow_triggers
			(id, tenant_id, project_id, workflow_id, version, type, cron, event, enabled, next_run_at, last_run_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`,
		trigger.ID, trigger.TenantID, trigger.ProjectID, trigger.WorkflowID, trigger.Version, string(trigger.Type),
		trigger.Cron, trigger.Event, trigger.Enabled, trigger.NextRunAt, trigger.LastRunAt, trigger.CreatedAt, trigger.UpdatedAt)
	if err != nil {
		return fmt.Errorf("create trigger: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetTrigger(ctx context.Context, id string) (domain.Trigger, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, type, cron, event, enabled, next_run_at, last_run_at, created_at, updated_at
		FROM workflow_triggers WHERE id=$1`, id)
	return scanTrigger(row)
}

func (r *PostgresRepository) ListTriggers(ctx context.Context, tenantID, projectID string) ([]domain.Trigger, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, type, cron, event, enabled, next_run_at, last_run_at, created_at, updated_at
		FROM workflow_triggers WHERE tenant_id=$1 AND project_id=$2 ORDER BY created_at DESC`, tenantID, projectID)
	if err != nil {
		return nil, fmt.Errorf("list triggers: %w", err)
	}
	defer rows.Close()
	var out []domain.Trigger
	for rows.Next() {
		trigger, err := scanTrigger(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, trigger)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) ListDue(ctx context.Context, now time.Time, limit int) ([]domain.Trigger, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, type, cron, event, enabled, next_run_at, last_run_at, created_at, updated_at
		FROM workflow_triggers
		WHERE enabled=TRUE AND (next_run_at IS NULL OR next_run_at <= $1)
		ORDER BY next_run_at NULLS FIRST, created_at ASC
		LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list due triggers: %w", err)
	}
	defer rows.Close()
	var out []domain.Trigger
	for rows.Next() {
		trigger, err := scanTrigger(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, trigger)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) UpdateTrigger(ctx context.Context, trigger domain.Trigger) error {
	_, err := r.db.ExecContext(ctx, `
		UPDATE workflow_triggers SET type=$2, cron=$3, event=$4, enabled=$5, next_run_at=$6, last_run_at=$7, updated_at=NOW()
		WHERE id=$1`,
		trigger.ID, string(trigger.Type), trigger.Cron, trigger.Event, trigger.Enabled, trigger.NextRunAt, trigger.LastRunAt)
	if err != nil {
		return fmt.Errorf("update trigger: %w", err)
	}
	return nil
}

func (r *PostgresRepository) DeleteTrigger(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, `DELETE FROM workflow_triggers WHERE id=$1`, id)
	return err
}

type scanner interface {
	Scan(dest ...any) error
}

func scanTrigger(row scanner) (domain.Trigger, error) {
	var t domain.Trigger
	if err := row.Scan(&t.ID, &t.TenantID, &t.ProjectID, &t.WorkflowID, &t.Version, &t.Type, &t.Cron, &t.Event,
		&t.Enabled, &t.NextRunAt, &t.LastRunAt, &t.CreatedAt, &t.UpdatedAt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return t, fmt.Errorf("trigger not found: %w", err)
		}
		return t, fmt.Errorf("scan trigger: %w", err)
	}
	return t, nil
}
