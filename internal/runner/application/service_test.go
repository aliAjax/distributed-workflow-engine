package application

import (
	"context"
	"sync"
	"testing"
	"time"

	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

type runnerTenantProvider struct{}

func (runnerTenantProvider) ListTenants(context.Context, int, int) ([]tenantdomain.Tenant, error) {
	return nil, nil
}

type runnerQueue struct{}

func (runnerQueue) EnqueueNode(context.Context, string, string, string, int, time.Time) error {
	return nil
}
func (runnerQueue) DequeueNode(context.Context, string, time.Time) (executionapplication.QueueNode, error) {
	return executionapplication.QueueNode{}, executionapplication.ErrQueueEmpty
}
func (runnerQueue) AckNode(context.Context, string, string, string) error { return nil }

type runnerExecutionRepo struct {
	nodes       []executiondomain.NodeExecution
	transitions [][2]executiondomain.ExecutionStatus
}

func (r *runnerExecutionRepo) CreateExecution(context.Context, executiondomain.Execution) error { return nil }
func (r *runnerExecutionRepo) GetExecution(context.Context, string, string) (executiondomain.Execution, error) {
	return executiondomain.Execution{}, nil
}
func (r *runnerExecutionRepo) GetExecutionByID(context.Context, string) (executiondomain.Execution, error) {
	return executiondomain.Execution{}, nil
}
func (r *runnerExecutionRepo) GetExecutionByIdempotency(context.Context, string, string) (executiondomain.Execution, error) {
	return executiondomain.Execution{}, context.DeadlineExceeded
}
func (r *runnerExecutionRepo) ListExecutions(context.Context, string, string, int, int) ([]executiondomain.Execution, error) {
	return nil, nil
}
func (r *runnerExecutionRepo) UpdateExecution(context.Context, executiondomain.Execution) error { return nil }
func (r *runnerExecutionRepo) TransitionExecution(_ context.Context, _ string, from, to executiondomain.ExecutionStatus) error {
	r.transitions = append(r.transitions, [2]executiondomain.ExecutionStatus{from, to})
	return nil
}
func (r *runnerExecutionRepo) CreateNode(context.Context, executiondomain.NodeExecution) error { return nil }
func (r *runnerExecutionRepo) GetNode(context.Context, string, string, int) (executiondomain.NodeExecution, error) {
	return executiondomain.NodeExecution{}, context.DeadlineExceeded
}
func (r *runnerExecutionRepo) GetLatestNode(context.Context, string, string) (executiondomain.NodeExecution, error) {
	return executiondomain.NodeExecution{}, context.DeadlineExceeded
}
func (r *runnerExecutionRepo) ListNodes(_ context.Context, _ string) ([]executiondomain.NodeExecution, error) {
	return r.nodes, nil
}
func (r *runnerExecutionRepo) UpdateNode(context.Context, executiondomain.NodeExecution) error { return nil }
func (r *runnerExecutionRepo) MergeContext(context.Context, string, map[string]any) error { return nil }
func (r *runnerExecutionRepo) AppendEvent(context.Context, executiondomain.Event) error { return nil }
func (r *runnerExecutionRepo) ListEvents(context.Context, string) ([]executiondomain.Event, error) { return nil, nil }
func (r *runnerExecutionRepo) CreateWaitEvent(context.Context, executionapplication.WaitEvent) error {
	return nil
}
func (r *runnerExecutionRepo) ResolveWaitEvent(context.Context, string, string, string, string, map[string]any) (bool, error) {
	return true, nil
}
func (r *runnerExecutionRepo) ListOpenWaitEvents(context.Context, string, string) ([]executionapplication.WaitEvent, error) {
	return nil, nil
}

func TestRunnerWorkerChannelLifecycle(t *testing.T) {
	svc := NewService(
		executionapplication.Service{},
		&runnerExecutionRepo{},
		runnerQueue{},
		runnerWorkflowReader{},
		runnerTenantProvider{},
		nil,
		nil,
		nil,
		Config{PollInterval: time.Millisecond, LeaseDuration: time.Second, HeartbeatEvery: time.Millisecond},
	)
	ctx, cancel := context.WithCancel(context.Background())
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		cancel()
	}()
	go func() {
		defer wg.Done()
		<-start
		if err := svc.Run(ctx, 2); err == nil {
			t.Errorf("expected context cancellation error")
		}
	}()
	close(start)
	wg.Wait()
}

type runnerWorkflowReader struct{}

func (runnerWorkflowReader) GetVersion(context.Context, string, int) (workflowdomain.WorkflowVersion, error) {
	return workflowdomain.WorkflowVersion{}, context.DeadlineExceeded
}

func TestRunnerReconcileFailedState(t *testing.T) {
	repo := &runnerExecutionRepo{nodes: []executiondomain.NodeExecution{
		{ID: "node-1", ExecutionID: "exec-1", NodeID: "step-1", Attempt: 1, Status: executiondomain.NodeFailed},
	}}
	svc := NewService(
		executionapplication.Service{},
		repo,
		runnerQueue{},
		runnerWorkflowReader{},
		runnerTenantProvider{},
		nil,
		nil,
		nil,
		Config{PollInterval: time.Millisecond, LeaseDuration: time.Second, HeartbeatEvery: time.Millisecond},
	)
	started := time.Now().UTC()
	err := svc.reconcileExecution(context.Background(), executiondomain.Execution{
		ID:        "exec-1",
		Status:    executiondomain.ExecutionRunning,
		StartedAt: &started,
	})
	if err != nil {
		t.Fatalf("reconcile execution: %v", err)
	}
	if len(repo.transitions) != 1 {
		t.Fatalf("expected one transition, got %d", len(repo.transitions))
	}
	got := repo.transitions[0]
	if got[1] != executiondomain.ExecutionFailed {
		t.Fatalf("expected transition to failed, got %s", got[1])
	}
}

func TestRunnerReconcileCompensatingStaysRunning(t *testing.T) {
	repo := &runnerExecutionRepo{nodes: []executiondomain.NodeExecution{
		{ID: "node-1", ExecutionID: "exec-1", NodeID: "step-1", Attempt: 1, Status: executiondomain.NodeCompensating},
	}}
	svc := NewService(
		executionapplication.Service{},
		repo,
		runnerQueue{},
		runnerWorkflowReader{},
		runnerTenantProvider{},
		nil,
		nil,
		nil,
		Config{PollInterval: time.Millisecond, LeaseDuration: time.Second, HeartbeatEvery: time.Millisecond},
	)
	started := time.Now().UTC()
	if err := svc.reconcileExecution(context.Background(), executiondomain.Execution{
		ID:        "exec-1",
		Status:    executiondomain.ExecutionRunning,
		StartedAt: &started,
	}); err != nil {
		t.Fatalf("reconcile execution: %v", err)
	}
	if len(repo.transitions) != 0 {
		t.Fatalf("expected no transition while compensating, got %d", len(repo.transitions))
	}
}

func TestRunnerSleepStopsOnCancel(t *testing.T) {
	svc := &Service{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := svc.executeNode(ctx, workflowdomain.Node{Type: workflowdomain.NodeTypeSleep, Command: "50ms"}, nil, nil)
	if err == nil {
		t.Fatal("expected sleep to stop when context is canceled")
	}
}
