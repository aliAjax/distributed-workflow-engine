package application

import (
	"context"
	"testing"

	"github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

type workflowFakeRepo struct {
	current domain.Workflow
	updated domain.Workflow
}

func (r *workflowFakeRepo) CreateWorkflow(context.Context, domain.Workflow) error { return nil }
func (r *workflowFakeRepo) GetWorkflow(_ context.Context, _, _, _ string) (domain.Workflow, error) {
	return r.current, nil
}
func (r *workflowFakeRepo) ListWorkflows(context.Context, string, string, int, int) ([]domain.Workflow, error) {
	return nil, nil
}
func (r *workflowFakeRepo) UpdateWorkflow(_ context.Context, workflow domain.Workflow) error {
	r.updated = workflow
	return nil
}
func (r *workflowFakeRepo) DeleteWorkflow(context.Context, string, string, string) error { return nil }
func (r *workflowFakeRepo) CreateVersion(context.Context, domain.WorkflowVersion) error { return nil }
func (r *workflowFakeRepo) GetVersion(context.Context, string, int) (domain.WorkflowVersion, error) {
	return domain.WorkflowVersion{}, context.DeadlineExceeded
}
func (r *workflowFakeRepo) ListVersions(context.Context, string) ([]domain.WorkflowVersion, error) {
	return nil, nil
}
func (r *workflowFakeRepo) GetLatestVersion(context.Context, string) (domain.WorkflowVersion, error) {
	return domain.WorkflowVersion{}, context.DeadlineExceeded
}

func TestWorkflowPublishVersionAdvances(t *testing.T) {
	repo := &workflowFakeRepo{current: domain.Workflow{ID: "wf-a", CurrentVersion: 1}}
	svc := NewService(repo)
	_, err := svc.PublishVersion(context.Background(), "tenant-a", "project-a", "wf-a", domain.Definition{
		Name: "flow",
		Nodes: map[string]domain.Node{
			"start": {ID: "start", Type: domain.NodeTypeNoop},
		},
	}, "creator-a")
	if err != nil {
		t.Fatalf("publish version: %v", err)
	}
	if repo.updated.CurrentVersion != 2 {
		t.Fatalf("expected current version to advance to 2, got %d", repo.updated.CurrentVersion)
	}
}
