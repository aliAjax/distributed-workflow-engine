package infrastructure

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/acme/distributed-workflow-engine/internal/execution/application"
	"github.com/acme/distributed-workflow-engine/internal/execution/domain"
)

type PostgresRepository struct {
	db *sql.DB
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db}
}

func (r *PostgresRepository) CreateExecution(ctx context.Context, execution domain.Execution) error {
	if execution.ID == "" {
		execution.ID = uuid.NewString()
	}
	input, _ := json.Marshal(execution.Input)
	contextRaw, _ := json.Marshal(execution.Context)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO executions
			(id, tenant_id, project_id, workflow_id, version, status, input, context, trigger_source, trigger_ref, priority, idempotency_key, last_heartbeat_at, created_at, updated_at, started_at, finished_at, error)
		VALUES ($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)`,
		execution.ID, execution.TenantID, execution.ProjectID, execution.WorkflowID, execution.Version,
		string(execution.Status), string(input), string(contextRaw), string(execution.TriggerSource),
		execution.TriggerRef, execution.Priority, execution.IdempotencyKey, execution.LastHeartbeatAt,
		execution.CreatedAt, execution.UpdatedAt, execution.StartedAt, execution.FinishedAt, execution.Error)
	if err != nil {
		return fmt.Errorf("create execution: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetExecution(ctx context.Context, tenantID, id string) (domain.Execution, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, status, input, context, trigger_source, COALESCE(trigger_ref,''), priority, COALESCE(idempotency_key,''), last_heartbeat_at, created_at, updated_at, started_at, finished_at, error
		FROM executions WHERE tenant_id=$1 AND id=$2`, tenantID, id)
	return scanExecution(row)
}

func (r *PostgresRepository) GetExecutionByID(ctx context.Context, id string) (domain.Execution, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, status, input, context, trigger_source, COALESCE(trigger_ref,''), priority, COALESCE(idempotency_key,''), last_heartbeat_at, created_at, updated_at, started_at, finished_at, error
		FROM executions WHERE id=$1`, id)
	return scanExecution(row)
}

func (r *PostgresRepository) GetExecutionByIdempotency(ctx context.Context, tenantID, key string) (domain.Execution, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, status, input, context, trigger_source, COALESCE(trigger_ref,''), priority, COALESCE(idempotency_key,''), last_heartbeat_at, created_at, updated_at, started_at, finished_at, error
		FROM executions WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key)
	return scanExecution(row)
}

func (r *PostgresRepository) ListExecutions(ctx context.Context, tenantID, projectID string, limit, offset int) ([]domain.Execution, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, project_id, workflow_id, version, status, input, context, trigger_source, COALESCE(trigger_ref,''), priority, COALESCE(idempotency_key,''), last_heartbeat_at, created_at, updated_at, started_at, finished_at, error
		FROM executions WHERE tenant_id=$1 AND project_id=$2 ORDER BY created_at DESC LIMIT $3 OFFSET $4`, tenantID, projectID, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("list executions: %w", err)
	}
	defer rows.Close()
	var out []domain.Execution
	for rows.Next() {
		execution, err := scanExecution(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, execution)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) UpdateExecution(ctx context.Context, execution domain.Execution) error {
	input, _ := json.Marshal(execution.Input)
	contextRaw, _ := json.Marshal(execution.Context)
	_, err := r.db.ExecContext(ctx, `
		UPDATE executions SET status=$2, input=$3::jsonb, context=$4::jsonb, trigger_source=$5, trigger_ref=$6, priority=$7, last_heartbeat_at=$8, started_at=$9, finished_at=$10, error=$11, updated_at=NOW()
		WHERE id=$1`,
		execution.ID, string(execution.Status), string(input), string(contextRaw), string(execution.TriggerSource),
		execution.TriggerRef, execution.Priority, execution.LastHeartbeatAt, execution.StartedAt, execution.FinishedAt, execution.Error)
	if err != nil {
		return fmt.Errorf("update execution: %w", err)
	}
	return nil
}

func (r *PostgresRepository) TransitionExecution(ctx context.Context, id string, from, to domain.ExecutionStatus) error {
	result, err := r.db.ExecContext(ctx, `UPDATE executions SET status=$2, updated_at=NOW() WHERE id=$1 AND status=$3`, id, string(to), string(from))
	if err != nil {
		return fmt.Errorf("transition execution: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return fmt.Errorf("execution transition %s -> %s failed: state changed concurrently", from, to)
	}
	return nil
}

func (r *PostgresRepository) CreateNode(ctx context.Context, node domain.NodeExecution) error {
	if node.ID == "" {
		node.ID = uuid.NewString()
	}
	input, _ := json.Marshal(node.Input)
	output, _ := json.Marshal(node.Output)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO node_executions
			(id, execution_id, node_id, attempt, status, input, output, error, retry_reason, lease_owner, lease_expires_at, started_at, finished_at, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11,$12,$13,$14,$15)`,
		node.ID, node.ExecutionID, node.NodeID, node.Attempt, string(node.Status), string(input), string(output),
		node.Error, node.RetryReason, node.LeaseOwner, node.LeaseExpiresAt, node.StartedAt, node.FinishedAt,
		node.CreatedAt, node.UpdatedAt)
	if err != nil {
		return fmt.Errorf("create node execution: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetNode(ctx context.Context, executionID, nodeID string, attempt int) (domain.NodeExecution, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, execution_id, node_id, attempt, status, input, output, error, retry_reason, COALESCE(lease_owner,''), lease_expires_at, started_at, finished_at, created_at, updated_at
		FROM node_executions WHERE execution_id=$1 AND node_id=$2 AND attempt=$3`, executionID, nodeID, attempt)
	return scanNode(row)
}

func (r *PostgresRepository) GetLatestNode(ctx context.Context, executionID, nodeID string) (domain.NodeExecution, error) {
	row := r.db.QueryRowContext(ctx, `
		SELECT id, execution_id, node_id, attempt, status, input, output, error, retry_reason, COALESCE(lease_owner,''), lease_expires_at, started_at, finished_at, created_at, updated_at
		FROM node_executions WHERE execution_id=$1 AND node_id=$2 ORDER BY attempt DESC LIMIT 1`, executionID, nodeID)
	return scanNode(row)
}

func (r *PostgresRepository) ListNodes(ctx context.Context, executionID string) ([]domain.NodeExecution, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, execution_id, node_id, attempt, status, input, output, error, retry_reason, COALESCE(lease_owner,''), lease_expires_at, started_at, finished_at, created_at, updated_at
		FROM node_executions WHERE execution_id=$1 ORDER BY created_at, attempt`, executionID)
	if err != nil {
		return nil, fmt.Errorf("list node executions: %w", err)
	}
	defer rows.Close()
	var out []domain.NodeExecution
	for rows.Next() {
		node, err := scanNode(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, node)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) UpdateNode(ctx context.Context, node domain.NodeExecution) error {
	input, _ := json.Marshal(node.Input)
	output, _ := json.Marshal(node.Output)
	_, err := r.db.ExecContext(ctx, `
		UPDATE node_executions SET status=$2, input=$3::jsonb, output=$4::jsonb, error=$5, retry_reason=$6, lease_owner=$7, lease_expires_at=$8, started_at=$9, finished_at=$10, updated_at=NOW()
		WHERE id=$1`,
		node.ID, string(node.Status), string(input), string(output), node.Error, node.RetryReason,
		node.LeaseOwner, node.LeaseExpiresAt, node.StartedAt, node.FinishedAt)
	if err != nil {
		return fmt.Errorf("update node execution: %w", err)
	}
	return nil
}

func (r *PostgresRepository) MergeContext(ctx context.Context, executionID string, patch map[string]any) error {
	raw, _ := json.Marshal(patch)
	_, err := r.db.ExecContext(ctx, `UPDATE executions SET context = COALESCE(context,'{}'::jsonb) || $2::jsonb, updated_at=NOW() WHERE id=$1`, executionID, string(raw))
	if err != nil {
		return fmt.Errorf("merge execution context: %w", err)
	}
	return nil
}

func (r *PostgresRepository) AppendEvent(ctx context.Context, event domain.Event) error {
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	payload, _ := json.Marshal(event.Payload)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO execution_events (execution_id, node_id, type, payload) VALUES ($1,$2,$3,$4::jsonb)`,
		event.ExecutionID, event.NodeID, event.Type, string(payload))
	if err != nil {
		return fmt.Errorf("append execution event: %w", err)
	}
	return nil
}

func (r *PostgresRepository) ListEvents(ctx context.Context, executionID string) ([]domain.Event, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id::text, execution_id, COALESCE(node_id,''), type, payload, created_at
		FROM execution_events WHERE execution_id=$1 ORDER BY created_at, id`, executionID)
	if err != nil {
		return nil, fmt.Errorf("list execution events: %w", err)
	}
	defer rows.Close()
	var out []domain.Event
	for rows.Next() {
		var event domain.Event
		var raw []byte
		if err := rows.Scan(&event.ID, &event.ExecutionID, &event.NodeID, &event.Type, &raw, &event.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan execution event: %w", err)
		}
		_ = json.Unmarshal(raw, &event.Payload)
		out = append(out, event)
	}
	return out, rows.Err()
}

func (r *PostgresRepository) CreateWaitEvent(ctx context.Context, wait application.WaitEvent) error {
	if wait.ID == "" {
		wait.ID = uuid.NewString()
	}
	payload, _ := json.Marshal(wait.Payload)
	_, err := r.db.ExecContext(ctx, `
		INSERT INTO wait_events (id, tenant_id, project_id, execution_id, node_id, event_name, status, payload, created_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
		ON CONFLICT (tenant_id, execution_id, node_id, event_name) DO UPDATE SET status='waiting', payload=EXCLUDED.payload`,
		wait.ID, wait.TenantID, wait.ProjectID, wait.ExecutionID, wait.NodeID, wait.EventName, "waiting", string(payload), time.Now().UTC())
	if err != nil {
		return fmt.Errorf("create wait event: %w", err)
	}
	return nil
}

func (r *PostgresRepository) ResolveWaitEvent(ctx context.Context, tenantID, executionID, nodeID, eventName string, payload map[string]any) (bool, error) {
	raw, _ := json.Marshal(payload)
	result, err := r.db.ExecContext(ctx, `
		UPDATE wait_events SET status='resolved', payload=$5::jsonb, resolved_at=NOW()
		WHERE tenant_id=$1 AND execution_id=$2 AND node_id=$3 AND event_name=$4 AND status='waiting'`,
		tenantID, executionID, nodeID, eventName, string(raw))
	if err != nil {
		return false, fmt.Errorf("resolve wait event: %w", err)
	}
	affected, _ := result.RowsAffected()
	return affected > 0, nil
}

func (r *PostgresRepository) ListOpenWaitEvents(ctx context.Context, tenantID, projectID string) ([]application.WaitEvent, error) {
	rows, err := r.db.QueryContext(ctx, `
		SELECT id, tenant_id, project_id, execution_id, node_id, event_name, status, payload, created_at, resolved_at
		FROM wait_events WHERE tenant_id=$1 AND project_id=$2 AND status='waiting' ORDER BY created_at`, tenantID, projectID)
	if err != nil {
		return nil, fmt.Errorf("list open wait events: %w", err)
	}
	defer rows.Close()
	var out []application.WaitEvent
	for rows.Next() {
		var wait application.WaitEvent
		var raw []byte
		if err := rows.Scan(&wait.ID, &wait.TenantID, &wait.ProjectID, &wait.ExecutionID, &wait.NodeID, &wait.EventName, &wait.Status, &raw, &wait.CreatedAt, &wait.ResolvedAt); err != nil {
			return nil, fmt.Errorf("scan wait event: %w", err)
		}
		_ = json.Unmarshal(raw, &wait.Payload)
		out = append(out, wait)
	}
	return out, rows.Err()
}

type scanner interface {
	Scan(dest ...any) error
}

func scanExecution(row scanner) (domain.Execution, error) {
	var e domain.Execution
	var inputRaw, contextRaw []byte
	if err := row.Scan(&e.ID, &e.TenantID, &e.ProjectID, &e.WorkflowID, &e.Version, &e.Status, &inputRaw, &contextRaw,
		&e.TriggerSource, &e.TriggerRef, &e.Priority, &e.IdempotencyKey, &e.LastHeartbeatAt, &e.CreatedAt, &e.UpdatedAt,
		&e.StartedAt, &e.FinishedAt, &e.Error); err != nil {
		return e, wrapNotFound("execution", err)
	}
	_ = json.Unmarshal(inputRaw, &e.Input)
	_ = json.Unmarshal(contextRaw, &e.Context)
	return e, nil
}

func scanNode(row scanner) (domain.NodeExecution, error) {
	var n domain.NodeExecution
	var inputRaw, outputRaw []byte
	if err := row.Scan(&n.ID, &n.ExecutionID, &n.NodeID, &n.Attempt, &n.Status, &inputRaw, &outputRaw, &n.Error,
		&n.RetryReason, &n.LeaseOwner, &n.LeaseExpiresAt, &n.StartedAt, &n.FinishedAt, &n.CreatedAt, &n.UpdatedAt); err != nil {
		return n, wrapNotFound("node execution", err)
	}
	_ = json.Unmarshal(inputRaw, &n.Input)
	_ = json.Unmarshal(outputRaw, &n.Output)
	return n, nil
}

func wrapNotFound(name string, err error) error {
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("%s not found: %w", name, err)
	}
	return fmt.Errorf("get %s: %w", name, err)
}
