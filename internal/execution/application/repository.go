package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/acme/distributed-workflow-engine/internal/execution/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

var ErrQueueEmpty = errors.New("queue is empty")

type Repository interface {
	CreateExecution(ctx context.Context, execution domain.Execution) error
	GetExecution(ctx context.Context, tenantID, id string) (domain.Execution, error)
	GetExecutionByID(ctx context.Context, id string) (domain.Execution, error)
	GetExecutionByIdempotency(ctx context.Context, tenantID, key string) (domain.Execution, error)
	ListExecutions(ctx context.Context, tenantID, projectID string, limit, offset int) ([]domain.Execution, error)
	UpdateExecution(ctx context.Context, execution domain.Execution) error
	TransitionExecution(ctx context.Context, id string, from, to domain.ExecutionStatus) error
	CreateNode(ctx context.Context, node domain.NodeExecution) error
	GetNode(ctx context.Context, executionID, nodeID string, attempt int) (domain.NodeExecution, error)
	GetLatestNode(ctx context.Context, executionID, nodeID string) (domain.NodeExecution, error)
	ListNodes(ctx context.Context, executionID string) ([]domain.NodeExecution, error)
	UpdateNode(ctx context.Context, node domain.NodeExecution) error
	MergeContext(ctx context.Context, executionID string, patch map[string]any) error
	AppendEvent(ctx context.Context, event domain.Event) error
	ListEvents(ctx context.Context, executionID string) ([]domain.Event, error)
	CreateWaitEvent(ctx context.Context, wait WaitEvent) error
	ResolveWaitEvent(ctx context.Context, tenantID, executionID, nodeID, eventName string, payload map[string]any) (bool, error)
	ListOpenWaitEvents(ctx context.Context, tenantID, projectID string) ([]WaitEvent, error)
}

type Queue interface {
	EnqueueNode(ctx context.Context, tenantID, executionID, nodeID string, priority int, availableAt time.Time) error
	DequeueNode(ctx context.Context, tenantID string, now time.Time) (QueueNode, error)
	AckNode(ctx context.Context, tenantID, executionID, nodeID string) error
}

type QueueNode struct {
	ExecutionID string    `json:"execution_id"`
	NodeID      string    `json:"node_id"`
	AvailableAt time.Time `json:"available_at"`
	TenantID    string    `json:"tenant_id"`
	Priority    int       `json:"priority"`
}

type WaitEvent struct {
	ID          string         `json:"id"`
	TenantID    string         `json:"tenant_id"`
	ProjectID   string         `json:"project_id"`
	ExecutionID string         `json:"execution_id"`
	NodeID      string         `json:"node_id"`
	EventName   string         `json:"event_name"`
	Status      string         `json:"status"`
	Payload     map[string]any `json:"payload"`
	CreatedAt   time.Time      `json:"created_at"`
	ResolvedAt  *time.Time     `json:"resolved_at,omitempty"`
}

type Service struct {
	repo      Repository
	queue     Queue
	workflows workflowRepository
}

type workflowRepository interface {
	GetVersion(ctx context.Context, workflowID string, number int) (workflowdomain.WorkflowVersion, error)
	GetLatestVersion(ctx context.Context, workflowID string) (workflowdomain.WorkflowVersion, error)
}

func NewService(repo Repository, queue Queue, workflows workflowRepository) *Service {
	return &Service{repo: repo, queue: queue, workflows: workflows}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (domain.Execution, error) {
	if input.TenantID == "" || input.WorkflowID == "" {
		return domain.Execution{}, fmt.Errorf("tenant and workflow are required")
	}
	if input.Version <= 0 {
		latest, err := s.workflows.GetLatestVersion(ctx, input.WorkflowID)
		if err != nil {
			return domain.Execution{}, fmt.Errorf("resolve workflow version: %w", err)
		}
		input.Version = latest.Version
	}
	definition, err := s.workflows.GetVersion(ctx, input.WorkflowID, input.Version)
	if err != nil {
		return domain.Execution{}, fmt.Errorf("get workflow version: %w", err)
	}
	if input.IdempotencyKey != "" {
		existing, err := s.repo.GetExecutionByIdempotency(ctx, input.TenantID, input.IdempotencyKey)
		if err == nil {
			return existing, nil
		}
	}
	if err := domain.ValidateExecutionInput(input.Input); err != nil {
		return domain.Execution{}, err
	}
	now := time.Now().UTC()
	execution := domain.Execution{
		ID:             uuid.NewString(),
		TenantID:       input.TenantID,
		ProjectID:      input.ProjectID,
		WorkflowID:     input.WorkflowID,
		Version:        input.Version,
		Status:         domain.ExecutionPending,
		Input:          input.Input,
		Context:        map[string]any{},
		TriggerSource:  input.Source,
		TriggerRef:     input.TriggerRef,
		Priority:       input.Priority,
		IdempotencyKey: input.IdempotencyKey,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
	if err := s.repo.CreateExecution(ctx, execution); err != nil {
		return domain.Execution{}, err
	}
	for _, start := range definition.Definition.StartNodes() {
		node := domain.NodeExecution{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      start,
			Attempt:     1,
			Status:      domain.NodeScheduled,
			Input:       input.Input,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		if err := s.repo.CreateNode(ctx, node); err != nil {
			return domain.Execution{}, err
		}
		if err := s.queue.EnqueueNode(ctx, input.TenantID, execution.ID, start, input.Priority, now); err != nil {
			return domain.Execution{}, fmt.Errorf("enqueue start node %s: %w", start, err)
		}
	}
	return execution, nil
}

func (s *Service) Get(ctx context.Context, tenantID, id string) (domain.Execution, error) {
	return s.repo.GetExecution(ctx, tenantID, id)
}

func (s *Service) GetByID(ctx context.Context, id string) (domain.Execution, error) {
	return s.repo.GetExecutionByID(ctx, id)
}

func (s *Service) List(ctx context.Context, tenantID, projectID string, limit, offset int) ([]domain.Execution, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	return s.repo.ListExecutions(ctx, tenantID, projectID, limit, offset)
}

func (s *Service) Pause(ctx context.Context, tenantID, id string) error {
	return s.transition(ctx, tenantID, id, domain.ExecutionRunning, domain.ExecutionPaused)
}

func (s *Service) Resume(ctx context.Context, tenantID, id string) error {
	return s.transition(ctx, tenantID, id, domain.ExecutionPaused, domain.ExecutionRunning)
}

func (s *Service) Cancel(ctx context.Context, tenantID, id string) error {
	execution, err := s.repo.GetExecution(ctx, tenantID, id)
	if err != nil {
		return err
	}
	if domain.TerminalStatus(execution.Status) {
		return fmt.Errorf("execution %s is already terminal", id)
	}
	return s.transition(ctx, tenantID, id, execution.Status, domain.ExecutionCanceled)
}

func (s *Service) Terminate(ctx context.Context, tenantID, id string) error {
	execution, err := s.repo.GetExecution(ctx, tenantID, id)
	if err != nil {
		return err
	}
	if domain.TerminalStatus(execution.Status) {
		return fmt.Errorf("execution %s is already terminal", id)
	}
	return s.repo.TransitionExecution(ctx, id, execution.Status, domain.ExecutionTerminated)
}

func (s *Service) transition(ctx context.Context, tenantID, id string, from, to domain.ExecutionStatus) error {
	if !domain.CanTransition(from, to) {
		return fmt.Errorf("invalid execution transition %s -> %s", from, to)
	}
	return s.repo.TransitionExecution(ctx, id, from, to)
}

func (s *Service) ListNodes(ctx context.Context, executionID string) ([]domain.NodeExecution, error) {
	return s.repo.ListNodes(ctx, executionID)
}

func (s *Service) ListEvents(ctx context.Context, executionID string) ([]domain.Event, error) {
	return s.repo.ListEvents(ctx, executionID)
}

func (s *Service) RegisterWait(ctx context.Context, wait WaitEvent) error {
	return s.repo.CreateWaitEvent(ctx, wait)
}

func (s *Service) ResolveWait(ctx context.Context, tenantID, executionID, nodeID, eventName string, payload map[string]any) (bool, error) {
	return s.repo.ResolveWaitEvent(ctx, tenantID, executionID, nodeID, eventName, payload)
}

type CreateInput struct {
	TenantID       string
	ProjectID      string
	WorkflowID     string
	Version        int
	Input          map[string]any
	Source         domain.TriggerSource
	TriggerRef     string
	Priority       int
	IdempotencyKey string
}
