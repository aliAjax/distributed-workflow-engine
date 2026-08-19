package adapter

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
)

type ExecutionEventRepository interface {
	AppendEvent(ctx context.Context, event executiondomain.Event) error
}

type ExecutionPersister struct {
	repo ExecutionEventRepository
}

func NewExecutionPersister(repo ExecutionEventRepository) *ExecutionPersister {
	return &ExecutionPersister{repo: repo}
}

func (p *ExecutionPersister) AppendEvent(ctx context.Context, event domain.Event) error {
	nodeID, _ := event.Payload["node_id"].(string)
	executionEvent := executiondomain.Event{
		ID:          event.ID,
		ExecutionID: event.SubjectID,
		NodeID:      nodeID,
		Type:        event.Type,
		Payload:     event.Payload,
		CreatedAt:   event.CreatedAt,
	}
	if executionEvent.CreatedAt.IsZero() {
		executionEvent.CreatedAt = time.Now().UTC()
	}
	if executionEvent.ID == "" {
		executionEvent.ID = uuid.NewString()
	}
	if executionEvent.ExecutionID == "" {
		return fmt.Errorf("cannot persist event without subject id")
	}
	if err := p.repo.AppendEvent(ctx, executionEvent); err != nil {
		return fmt.Errorf("append execution event: %w", err)
	}
	return nil
}
