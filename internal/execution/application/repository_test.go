package application

import (
	"context"
	"testing"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/execution/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

type executionFakeRepo struct {
	created      domain.Execution
	transitioned [][2]domain.ExecutionStatus
}

func (f *executionFakeRepo) CreateExecution(_ context.Context, e domain.Execution) error {
	f.created = e
	return nil
}
func (f *executionFakeRepo) GetExecution(_ context.Context, _, _ string) (domain.Execution, error) {
	return f.created, nil
}
func (f *executionFakeRepo) GetExecutionByID(_ context.Context, _ string) (domain.Execution, error) {
	return f.created, nil
}
func (f *executionFakeRepo) GetExecutionByIdempotency(context.Context, string, string) (domain.Execution, error) {
	return domain.Execution{}, errNotFound
}
func (f *executionFakeRepo) ListExecutions(context.Context, string, string, int, int) ([]domain.Execution, error) {
	return nil, nil
}
func (f *executionFakeRepo) UpdateExecution(context.Context, domain.Execution) error { return nil }
func (f *executionFakeRepo) TransitionExecution(_ context.Context, _ string, from, to domain.ExecutionStatus) error {
	f.transitioned = append(f.transitioned, [2]domain.ExecutionStatus{from, to})
	return nil
}
func (f *executionFakeRepo) CreateNode(context.Context, domain.NodeExecution) error { return nil }
func (f *executionFakeRepo) GetNode(context.Context, string, string, int) (domain.NodeExecution, error) {
	return domain.NodeExecution{}, errNotFound
}
func (f *executionFakeRepo) GetLatestNode(context.Context, string, string) (domain.NodeExecution, error) {
	return domain.NodeExecution{}, errNotFound
}
func (f *executionFakeRepo) ListNodes(context.Context, string) ([]domain.NodeExecution, error) {
	return nil, nil
}
func (f *executionFakeRepo) UpdateNode(context.Context, domain.NodeExecution) error { return nil }
func (f *executionFakeRepo) MergeContext(context.Context, string, map[string]any) error {
	return nil
}
func (f *executionFakeRepo) AppendEvent(context.Context, domain.Event) error { return nil }
func (f *executionFakeRepo) ListEvents(context.Context, string) ([]domain.Event, error) {
	return nil, nil
}
func (f *executionFakeRepo) CreateWaitEvent(context.Context, WaitEvent) error { return nil }
func (f *executionFakeRepo) ResolveWaitEvent(context.Context, string, string, string, string, map[string]any) (bool, error) {
	return true, nil
}
func (f *executionFakeRepo) ListOpenWaitEvents(context.Context, string, string) ([]WaitEvent, error) {
	return nil, nil
}

type executionFakeQueue struct{}

func (executionFakeQueue) EnqueueNode(context.Context, string, string, string, int, time.Time) error {
	return nil
}
func (executionFakeQueue) DequeueNode(context.Context, string, time.Time) (QueueNode, error) {
	return QueueNode{}, errNotFound
}
func (executionFakeQueue) AckNode(context.Context, string, string, string) error { return nil }

type executionFakeWorkflows struct{}

func (executionFakeWorkflows) GetVersion(context.Context, string, int) (workflowdomain.WorkflowVersion, error) {
	return workflowdomain.WorkflowVersion{
		Definition: workflowdomain.Definition{
			Name:  "simple",
			Nodes: map[string]workflowdomain.Node{"start": {ID: "start", Type: workflowdomain.NodeTypeNoop}},
			Edges: []workflowdomain.Edge{},
		},
	}, nil
}
func (executionFakeWorkflows) GetLatestVersion(context.Context, string) (workflowdomain.WorkflowVersion, error) {
	return executionFakeWorkflows{}.GetVersion(context.Background(), "", 1)
}

var errNotFound = context.DeadlineExceeded

func TestExecutionStateMachineTransitions(t *testing.T) {
	if !domain.CanTransition(domain.ExecutionWaiting, domain.ExecutionRunning) {
		t.Fatal("waiting execution must be able to transition to running")
	}
	if !domain.TerminalStatus(domain.ExecutionTerminated) {
		t.Fatal("terminated must be terminal")
	}
	if !domain.CanTransitionNode(domain.NodeWaiting, domain.NodeScheduled) {
		t.Fatal("waiting node must be able to transition to scheduled")
	}
}

func TestCreateExecutionContextAndInputValidation(t *testing.T) {
	repo := &executionFakeRepo{}
	svc := NewService(repo, executionFakeQueue{}, executionFakeWorkflows{})
	_, err := svc.Create(context.Background(), CreateInput{
		TenantID:   "tenant-a",
		WorkflowID: "wf-a",
		Version:    1,
		Input:      map[string]any{"": "bad"},
	})
	if err == nil {
		t.Fatal("expected invalid execution input to be rejected")
	}

	repo.created = domain.Execution{}
	_, err = svc.Create(context.Background(), CreateInput{
		TenantID:   "tenant-a",
		WorkflowID: "wf-a",
		Version:    1,
		Input:      map[string]any{"name": "ok"},
	})
	if err != nil {
		t.Fatalf("create execution: %v", err)
	}
	if repo.created.Context == nil {
		t.Fatal("execution context must be initialized")
	}
}

func TestCancelUsesExecutionCurrentState(t *testing.T) {
	repo := &executionFakeRepo{created: domain.Execution{Status: domain.ExecutionPaused}}
	svc := NewService(repo, executionFakeQueue{}, executionFakeWorkflows{})
	if err := svc.Cancel(context.Background(), "tenant-a", "execution-a"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if len(repo.transitioned) != 1 {
		t.Fatalf("expected one transition, got %d", len(repo.transitioned))
	}
	got := repo.transitioned[0]
	if got[0] != domain.ExecutionPaused || got[1] != domain.ExecutionCanceled {
		t.Fatalf("unexpected transition: %s -> %s", got[0], got[1])
	}
}
