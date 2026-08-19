package domain

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type Workflow struct {
	ID             string    `json:"id" yaml:"id"`
	TenantID       string    `json:"tenant_id" yaml:"tenant_id"`
	ProjectID      string    `json:"project_id" yaml:"project_id"`
	Name           string    `json:"name" yaml:"name"`
	Description    string    `json:"description,omitempty" yaml:"description,omitempty"`
	CurrentVersion int       `json:"current_version" yaml:"current_version"`
	CreatedAt      time.Time `json:"created_at" yaml:"created_at"`
	UpdatedAt      time.Time `json:"updated_at" yaml:"updated_at"`
}

type WorkflowVersion struct {
	ID         string     `json:"id" yaml:"id"`
	WorkflowID string     `json:"workflow_id" yaml:"workflow_id"`
	Version    int        `json:"version" yaml:"version"`
	Definition Definition `json:"definition" yaml:"definition"`
	Checksum   string     `json:"checksum" yaml:"checksum"`
	Status     string     `json:"status" yaml:"status"`
	CreatedBy  string     `json:"created_by,omitempty" yaml:"created_by,omitempty"`
	CreatedAt  time.Time  `json:"created_at" yaml:"created_at"`
}

type Definition struct {
	Name        string            `json:"name" yaml:"name"`
	Description string            `json:"description,omitempty" yaml:"description,omitempty"`
	Version     int               `json:"version" yaml:"version"`
	Nodes       map[string]Node   `json:"nodes" yaml:"nodes"`
	Edges       []Edge            `json:"edges" yaml:"edges"`
	Triggers    []Trigger         `json:"triggers,omitempty" yaml:"triggers,omitempty"`
	Inputs      map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type Node struct {
	ID            string            `json:"id" yaml:"id"`
	Type          string            `json:"type" yaml:"type"`
	Name          string            `json:"name" yaml:"name"`
	Description   string            `json:"description,omitempty" yaml:"description,omitempty"`
	Command       string            `json:"command,omitempty" yaml:"command,omitempty"`
	Inputs        map[string]string `json:"inputs,omitempty" yaml:"inputs,omitempty"`
	Outputs       map[string]string `json:"outputs,omitempty" yaml:"outputs,omitempty"`
	Condition     string            `json:"condition,omitempty" yaml:"condition,omitempty"`
	Timeout       time.Duration     `json:"timeout,omitempty" yaml:"timeout,omitempty"`
	Retry         RetryPolicy       `json:"retry,omitempty" yaml:"retry,omitempty"`
	Loop          *Loop             `json:"loop,omitempty" yaml:"loop,omitempty"`
	CompensateFor string            `json:"compensate_for,omitempty" yaml:"compensate_for,omitempty"`
	Delay         string            `json:"delay,omitempty" yaml:"delay,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type Edge struct {
	From      string `json:"from" yaml:"from"`
	To        string `json:"to" yaml:"to"`
	Condition string `json:"condition,omitempty" yaml:"condition,omitempty"`
	When      string `json:"when,omitempty" yaml:"when,omitempty"`
}

type Trigger struct {
	Type     string            `json:"type" yaml:"type"`
	Cron     string            `json:"cron,omitempty" yaml:"cron,omitempty"`
	Event    string            `json:"event,omitempty" yaml:"event,omitempty"`
	Schedule string            `json:"schedule,omitempty" yaml:"schedule,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty" yaml:"metadata,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts    int           `json:"max_attempts" yaml:"max_attempts"`
	InitialBackoff time.Duration `json:"initial_backoff,omitempty" yaml:"initial_backoff,omitempty"`
	MaxBackoff     time.Duration `json:"max_backoff,omitempty" yaml:"max_backoff,omitempty"`
	Multiplier     float64       `json:"multiplier,omitempty" yaml:"multiplier,omitempty"`
	Retryable      []string      `json:"retryable,omitempty" yaml:"retryable,omitempty"`
}

type Loop struct {
	Mode      string `json:"mode" yaml:"mode"`
	Condition string `json:"condition,omitempty" yaml:"condition,omitempty"`
	Count     int    `json:"count,omitempty" yaml:"count,omitempty"`
	MaxCount  int    `json:"max_count,omitempty" yaml:"max_count,omitempty"`
}

const (
	NodeTypeNoop       = "noop"
	NodeTypeLog        = "log"
	NodeTypeSleep      = "sleep"
	NodeTypeFail       = "fail"
	NodeTypeHTTP       = "http"
	NodeTypeEcho       = "echo"
	NodeTypeDecision   = "decision"
	NodeTypeJoin       = "join"
	NodeTypeWaitEvent  = "wait_event"
	NodeTypeCompensate = "compensate"
)

var validNodeTypes = map[string]bool{
	NodeTypeNoop:       true,
	NodeTypeLog:        true,
	NodeTypeSleep:      true,
	NodeTypeFail:       true,
	NodeTypeHTTP:       true,
	NodeTypeEcho:       true,
	NodeTypeDecision:   true,
	NodeTypeJoin:       true,
	NodeTypeWaitEvent:  true,
	NodeTypeCompensate: true,
}

func (d Definition) Validate() error {
	if strings.TrimSpace(d.Name) == "" {
		return errors.New("workflow name is required")
	}
	if len(d.Nodes) == 0 {
		return errors.New("workflow must contain at least one node")
	}
	for id, node := range d.Nodes {
		if strings.TrimSpace(id) == "" {
			return errors.New("node id cannot be empty")
		}
		if !validNodeTypes[node.Type] {
			return fmt.Errorf("node %s has unsupported type %q", id, node.Type)
		}
		if node.Retry.MaxAttempts < 0 {
			return fmt.Errorf("node %s retry max_attempts cannot be negative", id)
		}
		if node.Loop != nil {
			switch node.Loop.Mode {
			case "count", "condition":
			default:
				return fmt.Errorf("node %s loop mode must be count or condition", id)
			}
			if node.Loop.Mode == "count" && node.Loop.Count <= 0 {
				return fmt.Errorf("node %s count loop requires positive count", id)
			}
		}
	}
	for i, edge := range d.Edges {
		if _, ok := d.Nodes[edge.From]; !ok {
			return fmt.Errorf("edge %d references unknown source node %q", i, edge.From)
		}
		if _, ok := d.Nodes[edge.To]; !ok {
			return fmt.Errorf("edge %d references unknown target node %q", i, edge.To)
		}
		if edge.From == edge.To {
			return fmt.Errorf("edge %d cannot connect node %q to itself", i, edge.From)
		}
	}
	for _, trigger := range d.Triggers {
		switch trigger.Type {
		case "cron", "manual", "event":
		default:
			return fmt.Errorf("unsupported trigger type %q", trigger.Type)
		}
	}
	return nil
}

func (d Definition) Checksum() string {
	raw, _ := json.Marshal(d)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (d Definition) StartNodes() []string {
	hasIncoming := make(map[string]bool, len(d.Nodes))
	for _, edge := range d.Edges {
		hasIncoming[edge.To] = true
	}
	var starts []string
	for id := range d.Nodes {
		if !hasIncoming[id] {
			starts = append(starts, id)
		}
	}
	return starts
}

func (d Definition) Dependents(nodeID string) []string {
	var out []string
	for _, edge := range d.Edges {
		if edge.From == nodeID {
			out = append(out, edge.To)
		}
	}
	return out
}

func (d Definition) Dependencies(nodeID string) []string {
	var out []string
	for _, edge := range d.Edges {
		if edge.To == nodeID {
			out = append(out, edge.From)
		}
	}
	return out
}
