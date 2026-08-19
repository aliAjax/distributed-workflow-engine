package application

import (
	"context"
	"time"

	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
)

func (s *Service) reconcileExecution(ctx context.Context, execution executiondomain.Execution) error {
	nodes, err := s.execRepo.ListNodes(ctx, execution.ID)
	if err != nil {
		return err
	}
	latest := make(map[string]executiondomain.NodeExecution)
	for _, node := range nodes {
		current, ok := latest[node.NodeID]
		if !ok || node.Attempt > current.Attempt {
			latest[node.NodeID] = node
		}
	}

	hasFailure := false
	hasRunning := false
	hasPending := false
	allDone := true
	for _, node := range latest {
		switch node.Status {
		case executiondomain.NodeRunning, executiondomain.NodeScheduled, executiondomain.NodePending, executiondomain.NodeCompensating:
			// Active or in-flight work keeps the execution running; compensating
			// is still active cleanup, so it must not transition the execution.
			hasRunning = true
			allDone = false
		case executiondomain.NodeWaiting:
			allDone = false
		case executiondomain.NodeFailed:
			// Failed is terminal for the node; it only flags the execution as
			// failed once no other work remains in flight.
			hasFailure = true
		case executiondomain.NodeCanceled, executiondomain.NodeSkipped, executiondomain.NodeSucceeded, executiondomain.NodeCompensated:
			// Terminal and successful for execution completion purposes.
		}
	}

	next := execution.Status
	now := time.Now().UTC()
	if hasRunning || hasPending {
		next = executiondomain.ExecutionRunning
	} else if !allDone {
		next = executiondomain.ExecutionWaiting
	} else if hasFailure {
		next = executiondomain.ExecutionFailed
	} else {
		next = executiondomain.ExecutionSucceeded
	}

	if next == execution.Status {
		return nil
	}
	if !executiondomain.CanTransition(execution.Status, next) {
		next = execution.Status
	}
	if err := s.execRepo.TransitionExecution(ctx, execution.ID, execution.Status, next); err != nil {
		return err
	}
	if next == executiondomain.ExecutionRunning && execution.StartedAt == nil {
		if _, err := s.execution.GetByID(ctx, execution.ID); err != nil {
			return err
		}
	}
	if executiondomain.TerminalStatus(next) {
		finished := &now
		updated := execution
		updated.Status = next
		updated.FinishedAt = finished
		updated.UpdatedAt = now
		if next == executiondomain.ExecutionFailed {
			updated.Error = "one or more nodes failed"
		}
		_ = s.execRepo.UpdateExecution(ctx, updated)
	}
	return nil
}
