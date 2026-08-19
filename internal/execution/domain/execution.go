package domain

import (
	"errors"
	"fmt"
	"time"
)

type ExecutionStatus string

const (
	ExecutionScheduled  ExecutionStatus = "scheduled"
	ExecutionPending    ExecutionStatus = "pending"
	ExecutionRunning    ExecutionStatus = "running"
	ExecutionPaused     ExecutionStatus = "paused"
	ExecutionWaiting    ExecutionStatus = "waiting"
	ExecutionSucceeded  ExecutionStatus = "succeeded"
	ExecutionFailed     ExecutionStatus = "failed"
	ExecutionCanceled   ExecutionStatus = "canceled"
	ExecutionTerminated ExecutionStatus = "terminated"
)

type NodeStatus string

const (
	NodePending      NodeStatus = "pending"
	NodeScheduled    NodeStatus = "scheduled"
	NodeRunning      NodeStatus = "running"
	NodeSucceeded    NodeStatus = "succeeded"
	NodeFailed       NodeStatus = "failed"
	NodeWaiting      NodeStatus = "waiting"
	NodeSkipped      NodeStatus = "skipped"
	NodeCanceled     NodeStatus = "canceled"
	NodeCompensating NodeStatus = "compensating"
	NodeCompensated  NodeStatus = "compensated"
)

type TriggerSource string

const (
	TriggerManual TriggerSource = "manual"
	TriggerCron   TriggerSource = "cron"
	TriggerEvent  TriggerSource = "event"
)

type Execution struct {
	ID              string          `json:"id"`
	TenantID        string          `json:"tenant_id"`
	ProjectID       string          `json:"project_id"`
	WorkflowID      string          `json:"workflow_id"`
	Version         int             `json:"version"`
	Status          ExecutionStatus `json:"status"`
	Input           map[string]any  `json:"input,omitempty"`
	Context         map[string]any  `json:"context,omitempty"`
	TriggerSource   TriggerSource   `json:"trigger_source"`
	TriggerRef      string          `json:"trigger_ref,omitempty"`
	Priority        int             `json:"priority"`
	IdempotencyKey  string          `json:"idempotency_key,omitempty"`
	LastHeartbeatAt *time.Time      `json:"last_heartbeat_at,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	StartedAt       *time.Time      `json:"started_at,omitempty"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
	Error           string          `json:"error,omitempty"`
}

type NodeExecution struct {
	ID             string         `json:"id"`
	ExecutionID    string         `json:"execution_id"`
	NodeID         string         `json:"node_id"`
	Attempt        int            `json:"attempt"`
	Status         NodeStatus     `json:"status"`
	Input          map[string]any `json:"input,omitempty"`
	Output         map[string]any `json:"output,omitempty"`
	Error          string         `json:"error,omitempty"`
	RetryReason    string         `json:"retry_reason,omitempty"`
	LeaseOwner     string         `json:"lease_owner,omitempty"`
	LeaseExpiresAt *time.Time     `json:"lease_expires_at,omitempty"`
	StartedAt      *time.Time     `json:"started_at,omitempty"`
	FinishedAt     *time.Time     `json:"finished_at,omitempty"`
	CreatedAt      time.Time      `json:"created_at"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type Event struct {
	ID          string         `json:"id"`
	ExecutionID string         `json:"execution_id"`
	NodeID      string         `json:"node_id,omitempty"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
}

type Transition struct {
	From ExecutionStatus `json:"from"`
	To   ExecutionStatus `json:"to"`
}

var executionTransitions = map[ExecutionStatus]map[ExecutionStatus]bool{
	ExecutionScheduled:  {ExecutionPending: true, ExecutionCanceled: true, ExecutionTerminated: true},
	ExecutionPending:    {ExecutionRunning: true, ExecutionCanceled: true, ExecutionTerminated: true},
	ExecutionRunning:    {ExecutionPaused: true, ExecutionWaiting: true, ExecutionSucceeded: true, ExecutionFailed: true, ExecutionCanceled: true, ExecutionTerminated: true},
	ExecutionPaused:     {ExecutionRunning: true, ExecutionCanceled: true, ExecutionTerminated: true},
	ExecutionWaiting:    {ExecutionRunning: true, ExecutionCanceled: true, ExecutionTerminated: true},
	ExecutionSucceeded:  {},
	ExecutionFailed:     {ExecutionRunning: true, ExecutionTerminated: true},
	ExecutionCanceled:   {},
	ExecutionTerminated: {},
}

var nodeTransitions = map[NodeStatus]map[NodeStatus]bool{
	NodePending:      {NodeScheduled: true, NodeSkipped: true, NodeCanceled: true},
	NodeScheduled:    {NodeRunning: true, NodeCanceled: true, NodeSkipped: true},
	NodeRunning:      {NodeSucceeded: true, NodeFailed: true, NodeWaiting: true, NodeCanceled: true, NodeCompensating: true},
	NodeFailed:       {NodeScheduled: true, NodeSucceeded: true, NodeCompensating: true, NodeCanceled: true},
	NodeWaiting:      {NodeScheduled: true, NodeCanceled: true},
	NodeCompensating: {NodeCompensated: true, NodeFailed: true},
	NodeCompensated:  {},
	NodeSucceeded:    {NodeCompensating: true},
	NodeSkipped:      {},
	NodeCanceled:     {},
}

func CanTransition(from, to ExecutionStatus) bool {
	if from == to {
		return true
	}
	return executionTransitions[from][to]
}

func CanTransitionNode(from, to NodeStatus) bool {
	if from == to {
		return true
	}
	return nodeTransitions[from][to]
}

func TerminalStatus(status ExecutionStatus) bool {
	switch status {
	case ExecutionSucceeded, ExecutionFailed, ExecutionCanceled, ExecutionTerminated:
		return true
	default:
		return false
	}
}

func ValidateExecutionInput(input map[string]any) error {
	if len(input) > 100 {
		return fmt.Errorf("execution input contains too many keys: %d", len(input))
	}
	for key := range input {
		if key == "" {
			return errors.New("execution input contains empty key")
		}
	}
	return nil
}
