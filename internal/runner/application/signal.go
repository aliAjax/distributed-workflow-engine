package application

import (
	"context"
	"time"

	eventbusdomain "github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

func (s *Service) ResolveExternal(ctx context.Context, signal eventbusdomain.ExternalSignal) error {
	if signal.ExecutionID != "" {
		return s.resolveForExecution(ctx, signal)
	}
	waits, err := s.execRepo.ListOpenWaitEvents(ctx, signal.TenantID, signal.ProjectID)
	if err != nil {
		return err
	}
	for _, wait := range waits {
		if wait.EventName != signal.Event {
			continue
		}
		candidate := signal
		candidate.ExecutionID = wait.ExecutionID
		if err := s.resolveForExecution(ctx, candidate); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (s *Service) resolveForExecution(ctx context.Context, signal eventbusdomain.ExternalSignal) error {
	waits, err := s.execRepo.ListOpenWaitEvents(ctx, signal.TenantID, signal.ProjectID)
	if err != nil {
		return err
	}
	var matched *executionapplication.WaitEvent
	for _, wait := range waits {
		if wait.ExecutionID == signal.ExecutionID && wait.EventName == signal.Event {
			copy := wait
			matched = &copy
			break
		}
	}
	if matched == nil {
		return nil
	}
	resolved, err := s.execRepo.ResolveWaitEvent(ctx, signal.TenantID, signal.ExecutionID, matched.NodeID, signal.Event, signal.Payload)
	if err != nil {
		return err
	}
	if !resolved {
		return nil
	}
	execution, err := s.execution.GetByID(ctx, signal.ExecutionID)
	if err != nil {
		return err
	}
	version, err := s.workflows.GetVersion(ctx, execution.WorkflowID, execution.Version)
	if err != nil {
		return err
	}
	for nodeID, nodeDef := range version.Definition.Nodes {
		if nodeDef.Type != workflowdomain.NodeTypeWaitEvent {
			continue
		}
		eventName := resolveText(nodeDef.Command, execution.Input, execution.Context)
		if eventName != signal.Event {
			continue
		}
		node, err := s.execRepo.GetLatestNode(ctx, execution.ID, nodeID)
		if err != nil {
			return err
		}
		if node.Status != executiondomain.NodeWaiting {
			continue
		}
		output := map[string]any{"waiting_for": eventName, "signal": signal.Payload}
		return s.resumeWaitingNode(ctx, execution, version.Definition, node, output)
	}
	return nil
}

func (s *Service) resumeWaitingNode(ctx context.Context, execution executiondomain.Execution, definition workflowdomain.Definition, node executiondomain.NodeExecution, output map[string]any) error {
	now := time.Now().UTC()
	node.Status = executiondomain.NodeSucceeded
	node.Output = output
	node.FinishedAt = &now
	node.LeaseOwner = ""
	node.LeaseExpiresAt = nil
	if err := s.execRepo.UpdateNode(ctx, node); err != nil {
		return err
	}
	s.metrics.IncNode(execution.TenantID, execution.WorkflowID, node.NodeID, string(executiondomain.NodeSucceeded))
	if err := s.execRepo.MergeContext(ctx, execution.ID, map[string]any{node.NodeID: output}); err != nil {
		return err
	}
	_ = s.appendEvent(ctx, execution, node.NodeID, "node.succeeded", map[string]any{"output": output, "signal": output["signal"]})
	if err := s.enqueueDependents(ctx, execution, definition, node); err != nil {
		return err
	}
	return s.reconcileExecution(ctx, execution)
}

func executiondomainNodeTypeWaitEvent() string {
	return workflowdomain.NodeTypeWaitEvent
}
