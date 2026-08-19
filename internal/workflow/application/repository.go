package application

import (
	"context"
	"fmt"
	"time"

	"github.com/acme/distributed-workflow-engine/internal/workflow/domain"
	"github.com/google/uuid"
)

type Repository interface {
	CreateWorkflow(ctx context.Context, workflow domain.Workflow) error
	GetWorkflow(ctx context.Context, tenantID, projectID, id string) (domain.Workflow, error)
	ListWorkflows(ctx context.Context, tenantID, projectID string, limit, offset int) ([]domain.Workflow, error)
	UpdateWorkflow(ctx context.Context, workflow domain.Workflow) error
	DeleteWorkflow(ctx context.Context, tenantID, projectID, id string) error
	CreateVersion(ctx context.Context, version domain.WorkflowVersion) error
	GetVersion(ctx context.Context, workflowID string, number int) (domain.WorkflowVersion, error)
	ListVersions(ctx context.Context, workflowID string) ([]domain.WorkflowVersion, error)
	GetLatestVersion(ctx context.Context, workflowID string) (domain.WorkflowVersion, error)
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) Create(ctx context.Context, input CreateInput) (domain.Workflow, error) {
	workflow := input.Workflow
	if err := s.ValidateDefinition(input.Definition); err != nil {
		return domain.Workflow{}, err
	}
	if workflow.ID == "" {
		workflow.ID = uuid.NewString()
	}
	now := time.Now().UTC()
	if workflow.CreatedAt.IsZero() {
		workflow.CreatedAt = now
	}
	if workflow.UpdatedAt.IsZero() {
		workflow.UpdatedAt = now
	}
	if err := s.repo.CreateWorkflow(ctx, workflow); err != nil {
		return domain.Workflow{}, err
	}
	version := domain.WorkflowVersion{
		ID:         uuid.NewString(),
		WorkflowID: workflow.ID,
		Version:    1,
		Definition: input.Definition,
		Checksum:   input.Definition.Checksum(),
		Status:     "published",
	}
	if err := s.repo.CreateVersion(ctx, version); err != nil {
		return domain.Workflow{}, err
	}
	workflow.CurrentVersion = 1
	if err := s.repo.UpdateWorkflow(ctx, workflow); err != nil {
		return domain.Workflow{}, err
	}
	return workflow, nil
}

func (s *Service) PublishVersion(ctx context.Context, tenantID, projectID, workflowID string, definition domain.Definition, createdBy string) (domain.WorkflowVersion, error) {
	if err := s.ValidateDefinition(definition); err != nil {
		return domain.WorkflowVersion{}, err
	}
	current, err := s.repo.GetWorkflow(ctx, tenantID, projectID, workflowID)
	if err != nil {
		return domain.WorkflowVersion{}, err
	}
	next := current.CurrentVersion + 1
	version := domain.WorkflowVersion{
		ID:         uuid.NewString(),
		WorkflowID: workflowID,
		Version:    next,
		Definition: definition,
		Checksum:   definition.Checksum(),
		Status:     "published",
		CreatedBy:  createdBy,
	}
	if err := s.repo.CreateVersion(ctx, version); err != nil {
		return domain.WorkflowVersion{}, err
	}
	current.CurrentVersion = next
	if err := s.repo.UpdateWorkflow(ctx, current); err != nil {
		return domain.WorkflowVersion{}, err
	}
	return version, nil
}

func (s *Service) Get(ctx context.Context, tenantID, projectID, id string) (domain.Workflow, error) {
	return s.repo.GetWorkflow(ctx, tenantID, projectID, id)
}

func (s *Service) List(ctx context.Context, tenantID, projectID string, limit, offset int) ([]domain.Workflow, error) {
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	return s.repo.ListWorkflows(ctx, tenantID, projectID, limit, offset)
}

func (s *Service) GetVersion(ctx context.Context, workflowID string, number int) (domain.WorkflowVersion, error) {
	return s.repo.GetVersion(ctx, workflowID, number)
}

func (s *Service) GetLatestVersion(ctx context.Context, workflowID string) (domain.WorkflowVersion, error) {
	return s.repo.GetLatestVersion(ctx, workflowID)
}

func (s *Service) ListVersions(ctx context.Context, workflowID string) ([]domain.WorkflowVersion, error) {
	return s.repo.ListVersions(ctx, workflowID)
}

func (s *Service) ValidateDefinition(definition domain.Definition) error {
	return definition.Validate()
}

type CreateInput struct {
	Workflow   domain.Workflow
	Definition domain.Definition
}

var (
	errMissingScope = fmt.Errorf("workflow tenant and project are required")
	errMissingName  = fmt.Errorf("workflow name is required")
)
