package application

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"

	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

func (s *Service) executeNode(ctx context.Context, node workflowdomain.Node, input, executionContext map[string]any) (map[string]any, error) {
	if node.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, node.Timeout)
		defer cancel()
	}
	switch node.Type {
	case workflowdomain.NodeTypeNoop, workflowdomain.NodeTypeJoin:
		return map[string]any{"ok": true}, nil
	case workflowdomain.NodeTypeLog:
		value := resolveText(node.Command, input, executionContext)
		return map[string]any{"logged": value}, nil
	case workflowdomain.NodeTypeEcho:
		return map[string]any{"value": resolveText(node.Command, input, executionContext)}, nil
	case workflowdomain.NodeTypeSleep:
		duration, err := time.ParseDuration(resolveText(node.Command, input, executionContext))
		if err != nil {
			return nil, fmt.Errorf("parse sleep duration: %w", err)
		}
		if err := sleepContext(ctx, duration); err != nil {
			return nil, err
		}
		return map[string]any{"slept": duration.String()}, nil
	case workflowdomain.NodeTypeFail:
		return nil, fmt.Errorf("%s", resolveText(node.Command, input, executionContext))
	case workflowdomain.NodeTypeHTTP:
		return s.executeHTTP(ctx, node, input, executionContext)
	case workflowdomain.NodeTypeDecision:
		result, err := evaluateCondition(node.Condition, input, executionContext)
		if err != nil {
			return nil, err
		}
		branch := "else"
		if result {
			branch = "then"
		}
		return map[string]any{"decision": branch, "matched": result}, nil
	case workflowdomain.NodeTypeWaitEvent:
		eventName := resolveText(node.Command, input, executionContext)
		return map[string]any{"waiting_for": eventName}, nil
	case workflowdomain.NodeTypeCompensate:
		return map[string]any{"compensated_for": node.CompensateFor}, nil
	default:
		return nil, fmt.Errorf("unsupported node type %q", node.Type)
	}
}

func (s *Service) executeHTTP(ctx context.Context, node workflowdomain.Node, input, executionContext map[string]any) (map[string]any, error) {
	method := strings.ToUpper(strings.TrimSpace(node.Inputs["method"]))
	if method == "" {
		method = http.MethodGet
	}
	url := resolveText(node.Command, input, executionContext)
	if url == "" {
		url = node.Inputs["url"]
	}
	var body io.Reader
	if rawBody := node.Inputs["body"]; rawBody != "" {
		body = strings.NewReader(rawBody)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("build http request: %w", err)
	}
	for key, value := range node.Inputs {
		if strings.HasPrefix(key, "header.") {
			req.Header.Set(strings.TrimPrefix(key, "header."), value)
		}
	}
	client := &http.Client{Timeout: node.Timeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read http response: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("http status %d: %s", resp.StatusCode, string(raw))
	}
	return map[string]any{"status_code": resp.StatusCode, "body": string(raw)}, nil
}

func (s *Service) completeNode(ctx context.Context, execution executiondomain.Execution, definition workflowdomain.Definition, node executiondomain.NodeExecution, output map[string]any) error {
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
	patch := map[string]any{node.NodeID: output}
	if err := s.execRepo.MergeContext(ctx, execution.ID, patch); err != nil {
		return err
	}
	_ = s.appendEvent(ctx, execution, node.NodeID, "node.succeeded", map[string]any{"output": output})

	nodeDef := definition.Nodes[node.NodeID]
	if nodeDef.Type == workflowdomain.NodeTypeWaitEvent {
		eventName, _ := output["waiting_for"].(string)
		if eventName == "" {
			eventName = resolveText(nodeDef.Command, node.Input, execution.Context)
		}
		node.Status = executiondomain.NodeWaiting
		node.Output = output
		node.FinishedAt = nil
		node.LeaseOwner = ""
		node.LeaseExpiresAt = nil
		if err := s.execRepo.UpdateNode(ctx, node); err != nil {
			return err
		}
		wait := executionapplication.WaitEvent{
			TenantID:    execution.TenantID,
			ProjectID:   execution.ProjectID,
			ExecutionID: execution.ID,
			NodeID:      node.NodeID,
			EventName:   eventName,
			Status:      "waiting",
		}
		if err := s.execution.RegisterWait(ctx, wait); err != nil {
			return err
		}
		_ = s.appendEvent(ctx, execution, node.NodeID, "node.waiting", map[string]any{"event": eventName})
		return s.reconcileExecution(ctx, execution)
	}
	if err := s.enqueueDependents(ctx, execution, definition, node); err != nil {
		return err
	}
	return s.reconcileExecution(ctx, execution)
}

func (s *Service) failNode(ctx context.Context, execution executiondomain.Execution, definition workflowdomain.Definition, node executiondomain.NodeExecution, execErr error) error {
	now := time.Now().UTC()
	node.Error = execErr.Error()
	node.FinishedAt = &now
	node.LeaseOwner = ""
	node.LeaseExpiresAt = nil
	nodeDef := definition.Nodes[node.NodeID]
	retry := nodeDef.Retry
	if retry.MaxAttempts == 0 {
		retry.MaxAttempts = 1
	}
	if node.Attempt < retry.MaxAttempts {
		next := executiondomain.NodeExecution{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      node.NodeID,
			Attempt:     node.Attempt + 1,
			Status:      executiondomain.NodeScheduled,
			Input:       node.Input,
			CreatedAt:   now,
			UpdatedAt:   now,
		}
		node.Status = executiondomain.NodeFailed
		node.RetryReason = execErr.Error()
		if err := s.execRepo.UpdateNode(ctx, node); err != nil {
			return err
		}
		if err := s.execRepo.CreateNode(ctx, next); err != nil {
			return err
		}
		backoff := time.Duration(0)
		if retry.InitialBackoff > 0 {
			backoff = retry.InitialBackoff
			if retry.Multiplier > 1 && node.Attempt > 1 {
				backoff = time.Duration(float64(backoff) * powFactor(retry.Multiplier, node.Attempt-1))
				if retry.MaxBackoff > 0 && backoff > retry.MaxBackoff {
					backoff = retry.MaxBackoff
				}
			}
		}
		_ = s.appendEvent(ctx, execution, node.NodeID, "node.retrying", map[string]any{
			"attempt": next.Attempt, "reason": execErr.Error(), "delay": backoff.String(),
		})
		return s.queue.EnqueueNode(ctx, execution.TenantID, execution.ID, node.NodeID, execution.Priority, now.Add(backoff))
	}

	node.Status = executiondomain.NodeFailed
	node.RetryReason = "attempts exhausted: " + execErr.Error()
	if err := s.execRepo.UpdateNode(ctx, node); err != nil {
		return err
	}
	s.metrics.IncNode(execution.TenantID, execution.WorkflowID, node.NodeID, string(executiondomain.NodeFailed))
	_ = s.appendEvent(ctx, execution, node.NodeID, "node.failed", map[string]any{"error": execErr.Error()})
	if err := s.enqueueCompensations(ctx, execution, definition, node.NodeID); err != nil {
		return err
	}
	return s.reconcileExecution(ctx, execution)
}

func (s *Service) enqueueDependents(ctx context.Context, execution executiondomain.Execution, definition workflowdomain.Definition, completed executiondomain.NodeExecution) error {
	completedDef := definition.Nodes[completed.NodeID]
	for _, dependentID := range definition.Dependents(completed.NodeID) {
		edge := edgeFor(definition, completed.NodeID, dependentID)
		if completedDef.Type == workflowdomain.NodeTypeDecision {
			decision, _ := completed.Output["decision"].(string)
			if edge.When != "" && edge.When != decision {
				continue
			}
		}
		if edge.When == "failure" && completed.Status != executiondomain.NodeFailed {
			continue
		}
		if edge.When == "success" && completed.Status != executiondomain.NodeSucceeded {
			continue
		}
		if !s.dependenciesSatisfied(ctx, execution.ID, definition, dependentID) {
			continue
		}
		latest, err := s.execRepo.GetLatestNode(ctx, execution.ID, dependentID)
		if err == nil && nodeIsTerminal(latest.Status) {
			continue
		}
		node := executiondomain.NodeExecution{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      dependentID,
			Attempt:     1,
			Status:      executiondomain.NodeScheduled,
			Input:       buildNodeInput(definition, execution.Input, execution.Context, dependentID),
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		if err := s.execRepo.CreateNode(ctx, node); err != nil {
			return err
		}
		if err := s.queue.EnqueueNode(ctx, execution.TenantID, execution.ID, dependentID, execution.Priority, time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) enqueueCompensations(ctx context.Context, execution executiondomain.Execution, definition workflowdomain.Definition, failedNodeID string) error {
	for _, nodeDef := range definition.Nodes {
		if nodeDef.CompensateFor != failedNodeID {
			continue
		}
		latest, err := s.execRepo.GetLatestNode(ctx, execution.ID, nodeDef.ID)
		if err == nil && nodeIsTerminal(latest.Status) {
			continue
		}
		node := executiondomain.NodeExecution{
			ID:          uuid.NewString(),
			ExecutionID: execution.ID,
			NodeID:      nodeDef.ID,
			Attempt:     1,
			Status:      executiondomain.NodeScheduled,
			Input:       buildNodeInput(definition, execution.Input, execution.Context, nodeDef.ID),
			CreatedAt:   time.Now().UTC(),
			UpdatedAt:   time.Now().UTC(),
		}
		if err := s.execRepo.CreateNode(ctx, node); err != nil {
			return err
		}
		if err := s.queue.EnqueueNode(ctx, execution.TenantID, execution.ID, nodeDef.ID, execution.Priority, time.Now().UTC()); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) dependenciesSatisfied(ctx context.Context, executionID string, definition workflowdomain.Definition, nodeID string) bool {
	for _, depID := range definition.Dependencies(nodeID) {
		latest, err := s.execRepo.GetLatestNode(ctx, executionID, depID)
		if err != nil {
			return false
		}
		if latest.Status != executiondomain.NodeSucceeded && latest.Status != executiondomain.NodeSkipped && latest.Status != executiondomain.NodeCompensated {
			return false
		}
	}
	return true
}

func edgeFor(definition workflowdomain.Definition, from, to string) workflowdomain.Edge {
	for _, edge := range definition.Edges {
		if edge.From == from && edge.To == to {
			return edge
		}
	}
	return workflowdomain.Edge{}
}

func buildNodeInput(definition workflowdomain.Definition, initialInput, executionContext map[string]any, nodeID string) map[string]any {
	merged := map[string]any{"execution": initialInput}
	for key, value := range executionContext {
		merged[key] = value
	}
	if node, ok := definition.Nodes[nodeID]; ok {
		for key, expr := range node.Inputs {
			merged[key] = resolveText(expr, initialInput, executionContext)
		}
	}
	return merged
}

func resolveText(value string, input, executionContext map[string]any) string {
	if !strings.Contains(value, "$") {
		return value
	}
	if strings.HasPrefix(value, "$context.") {
		key := strings.TrimPrefix(value, "$context.")
		if found, ok := lookupPath(executionContext, key); ok {
			if raw, err := json.Marshal(found); err == nil {
				return strings.Trim(string(raw), `"`)
			}
		}
		return ""
	}
	if strings.HasPrefix(value, "$input.") {
		key := strings.TrimPrefix(value, "$input.")
		if found, ok := lookupPath(input, key); ok {
			if raw, err := json.Marshal(found); err == nil {
				return strings.Trim(string(raw), `"`)
			}
		}
		return ""
	}
	return value
}

func lookupPath(data map[string]any, path string) (any, bool) {
	parts := strings.Split(path, ".")
	var current any = data
	for _, part := range parts {
		m, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = m[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func evaluateCondition(expr string, input, executionContext map[string]any) (bool, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return true, nil
	}
	if strings.Contains(expr, "==") {
		parts := strings.SplitN(expr, "==", 2)
		left := strings.TrimSpace(resolveText(parts[0], input, executionContext))
		right := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		return left == right, nil
	}
	if strings.Contains(expr, "!=") {
		parts := strings.SplitN(expr, "!=", 2)
		left := strings.TrimSpace(resolveText(parts[0], input, executionContext))
		right := strings.Trim(strings.TrimSpace(parts[1]), `"'`)
		return left != right, nil
	}
	value := strings.ToLower(strings.TrimSpace(resolveText(expr, input, executionContext)))
	switch value {
	case "true", "1", "yes", "success":
		return true, nil
	case "false", "0", "no", "failure", "":
		return false, nil
	default:
		return false, fmt.Errorf("unparseable condition %q", expr)
	}
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func powFactor(base float64, exp int) float64 {
	result := 1.0
	for i := 0; i < exp; i++ {
		result *= base
	}
	return result
}
