package application

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	eventbusapplication "github.com/acme/distributed-workflow-engine/internal/eventbus/application"
	eventbusdomain "github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	policyapplication "github.com/acme/distributed-workflow-engine/internal/policy/application"
	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

type WorkflowReader interface {
	GetVersion(ctx context.Context, workflowID string, number int) (workflowdomain.WorkflowVersion, error)
}

type TenantProvider interface {
	ListTenants(ctx context.Context, limit, offset int) ([]tenantdomain.Tenant, error)
}

type MetricsRecorder interface {
	IncNode(tenantID, workflowID, nodeID, status string)
	SetQueueDepth(tenantID, queue string, depth float64)
	SetActiveLeases(count float64)
	IncEvent(tenantID, eventType string)
}

type Config struct {
	WorkerID       string
	PollInterval   time.Duration
	LeaseDuration  time.Duration
	HeartbeatEvery time.Duration
}

type Service struct {
	execution executionapplication.Service
	execRepo  executionapplication.Repository
	queue     executionapplication.Queue
	workflows WorkflowReader
	tenants   TenantProvider
	policy    *policyapplication.Service
	events    *eventbusapplication.Service
	metrics   MetricsRecorder
	config    Config
}

func NewService(
	execution executionapplication.Service,
	execRepo executionapplication.Repository,
	queue executionapplication.Queue,
	workflows WorkflowReader,
	tenants TenantProvider,
	policy *policyapplication.Service,
	events *eventbusapplication.Service,
	metrics MetricsRecorder,
	config Config,
) *Service {
	if config.WorkerID == "" {
		config.WorkerID = uuid.NewString()
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 500 * time.Millisecond
	}
	if config.LeaseDuration <= 0 {
		config.LeaseDuration = 30 * time.Second
	}
	if config.HeartbeatEvery <= 0 {
		config.HeartbeatEvery = 5 * time.Second
	}
	return &Service{
		execution: execution,
		execRepo:  execRepo,
		queue:     queue,
		workflows: workflows,
		tenants:   tenants,
		policy:    policy,
		events:    events,
		metrics:   metrics,
		config:    config,
	}
}

func (s *Service) Run(ctx context.Context, count int) error {
	if count <= 0 {
		count = 1
	}
	var wg sync.WaitGroup
	errCh := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			if err := s.runWorker(ctx, worker); err != nil {
				errCh <- err
			}
		}(i)
	}
	<-ctx.Done()
	// Wait for workers to finish before closing errCh: a worker that observed
	// ctx.Done() returns ctx.Err() and sends it here. Closing first would turn
	// that send into a "send on closed channel" panic.
	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return ctx.Err()
}

func (s *Service) runWorker(ctx context.Context, worker int) error {
	ticker := time.NewTicker(s.config.PollInterval)
	defer ticker.Stop()
	workerName := fmt.Sprintf("%s:%d", s.config.WorkerID, worker)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.pollOnce(ctx, workerName); err != nil {
				return err
			}
		}
	}
}

func (s *Service) pollOnce(ctx context.Context, workerName string) error {
	tenants, err := s.tenants.ListTenants(ctx, 100, 0)
	if err != nil {
		return fmt.Errorf("runner list tenants: %w", err)
	}
	for _, tenant := range tenants {
		for {
			item, err := s.queue.DequeueNode(ctx, tenant.ID, time.Now().UTC())
			if err == executionapplication.ErrQueueEmpty {
				break
			}
			if err != nil {
				return fmt.Errorf("runner dequeue tenant %s: %w", tenant.ID, err)
			}
			if err := s.processNode(ctx, workerName, item); err != nil {
				// A single node failure is recorded in the execution and must not
				// stop the worker; continuing lets other nodes progress.
				continue
			}
		}
	}
	return nil
}

func (s *Service) processNode(ctx context.Context, workerName string, item executionapplication.QueueNode) error {
	execution, err := s.execution.GetByID(ctx, item.ExecutionID)
	if err != nil {
		return err
	}
	version, err := s.workflows.GetVersion(ctx, execution.WorkflowID, execution.Version)
	if err != nil {
		return err
	}
	nodeDef, ok := version.Definition.Nodes[item.NodeID]
	if !ok {
		return fmt.Errorf("node %s not found in workflow version %d", item.NodeID, execution.Version)
	}
	node, err := s.execRepo.GetLatestNode(ctx, execution.ID, item.NodeID)
	if err != nil {
		return err
	}
	if nodeIsTerminal(node.Status) {
		_ = s.queue.AckNode(ctx, item.TenantID, item.ExecutionID, item.NodeID)
		return nil
	}
	if !s.executionCanRun(execution.Status) {
		node.Status = executiondomain.NodeCanceled
		now := time.Now().UTC()
		node.FinishedAt = &now
		if err := s.execRepo.UpdateNode(ctx, node); err != nil {
			return err
		}
		_ = s.queue.AckNode(ctx, item.TenantID, item.ExecutionID, item.NodeID)
		return nil
	}

	leaseKey := fmt.Sprintf("workflow:node-lease:%s:%s:%d", execution.ID, item.NodeID, node.Attempt)
	ok, err = s.policy.AcquireNodeLease(ctx, leaseKey, workerName, s.config.LeaseDuration)
	if err != nil {
		return err
	}
	if !ok {
		_ = s.queue.EnqueueNode(ctx, item.TenantID, item.ExecutionID, item.NodeID, execution.Priority, time.Now().UTC().Add(time.Second))
		return nil
	}
	s.metrics.SetActiveLeases(1)
	leaseCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go s.heartbeatLease(leaseCtx, leaseKey, workerName, s.config.LeaseDuration, s.config.HeartbeatEvery)
	defer func() {
		_ = s.policy.ReleaseNodeLease(ctx, leaseKey, workerName)
		s.metrics.SetActiveLeases(-1)
	}()

	now := time.Now().UTC()
	node.Status = executiondomain.NodeRunning
	node.LeaseOwner = workerName
	node.LeaseExpiresAt = timePtr(now.Add(s.config.LeaseDuration))
	node.StartedAt = &now
	if err := s.execRepo.UpdateNode(ctx, node); err != nil {
		return err
	}
	_ = s.appendEvent(ctx, execution, item.NodeID, "node.running", map[string]any{"attempt": node.Attempt, "worker": workerName})

	output, execErr := s.executeNode(ctx, nodeDef, node.Input, execution.Context)
	if execErr == nil {
		if err := s.completeNode(ctx, execution, version.Definition, node, output); err != nil {
			return err
		}
	} else {
		if err := s.failNode(ctx, execution, version.Definition, node, execErr); err != nil {
			return err
		}
	}
	_ = s.queue.AckNode(ctx, item.TenantID, item.ExecutionID, item.NodeID)
	return nil
}

func (s *Service) heartbeatLease(ctx context.Context, key, owner string, ttl, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := s.policy.RenewNodeLease(ctx, key, owner, ttl)
			if err != nil || !ok {
				return
			}
		}
	}
}

func (s *Service) appendEvent(ctx context.Context, execution executiondomain.Execution, nodeID, eventType string, payload map[string]any) error {
	if payload == nil {
		payload = map[string]any{}
	}
	payload["node_id"] = nodeID
	event := eventbusdomain.Event{
		TenantID:  execution.TenantID,
		ProjectID: execution.ProjectID,
		Type:      eventType,
		Source:    "runner",
		SubjectID: execution.ID,
		Payload:   payload,
	}
	return s.events.Publish(ctx, event)
}

func (s *Service) executionCanRun(status executiondomain.ExecutionStatus) bool {
	switch status {
	case executiondomain.ExecutionPending, executiondomain.ExecutionRunning, executiondomain.ExecutionWaiting:
		return true
	default:
		return false
	}
}

func nodeIsTerminal(status executiondomain.NodeStatus) bool {
	switch status {
	case executiondomain.NodeSucceeded, executiondomain.NodeFailed, executiondomain.NodeSkipped, executiondomain.NodeCanceled, executiondomain.NodeCompensated:
		return true
	default:
		return false
	}
}

func timePtr(value time.Time) *time.Time {
	return &value
}
