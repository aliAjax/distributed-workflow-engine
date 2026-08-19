package domain

import "testing"

func TestWorkflowStartNodesStableCopy(t *testing.T) {
	definition := Definition{
		Name: "flow",
		Nodes: map[string]Node{
			"start": {ID: "start", Type: NodeTypeNoop},
			"done":  {ID: "done", Type: NodeTypeNoop},
		},
		Edges: []Edge{{From: "start", To: "done"}},
	}
	first := definition.StartNodes()
	if len(first) != 1 {
		t.Fatalf("expected one start node, got %d", len(first))
	}
	_ = definition.Dependents("start")
	if len(first) != 1 || first[0] != "start" {
		t.Fatalf("StartNodes result was mutated by Dependents: %v", first)
	}
}

func TestWorkflowDefinitionRejectsDuplicateEdges(t *testing.T) {
	definition := Definition{
		Name: "flow",
		Nodes: map[string]Node{
			"start": {ID: "start", Type: NodeTypeNoop},
			"done":  {ID: "done", Type: NodeTypeNoop},
		},
		Edges: []Edge{{From: "start", To: "done"}, {From: "start", To: "done"}},
	}
	if err := definition.Validate(); err == nil {
		t.Fatal("expected duplicate edges to be rejected")
	}
}
