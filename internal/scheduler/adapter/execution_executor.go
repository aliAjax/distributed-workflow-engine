package adapter

import (
	"context"
	"fmt"
	"time"

	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	schedulerdomain "github.com/acme/distributed-workflow-engine/internal/scheduler/domain"
)

type ExecutionExecutor struct {
	executions *executionapplication.Service
}

func NewExecutionExecutor(executions *executionapplication.Service) *ExecutionExecutor {
	return &ExecutionExecutor{executions: executions}
}

func (e *ExecutionExecutor) Trigger(ctx context.Context, trigger schedulerdomain.Trigger) error {
	source := executiondomain.TriggerCron
	if trigger.Type == schedulerdomain.TriggerEvent {
		source = executiondomain.TriggerEvent
	}
	execution, err := e.executions.Create(ctx, executionapplication.CreateInput{
		TenantID:       trigger.TenantID,
		ProjectID:      trigger.ProjectID,
		WorkflowID:     trigger.WorkflowID,
		Version:        trigger.Version,
		Input:          map[string]any{"trigger_id": trigger.ID, "event": trigger.Event},
		Source:         source,
		TriggerRef:     trigger.ID,
		IdempotencyKey: fmt.Sprintf("trigger:%s:%d", trigger.ID, time.Now().UnixNano()),
	})
	if err != nil {
		return fmt.Errorf("trigger execution: %w", err)
	}
	_ = execution
	return nil
}
