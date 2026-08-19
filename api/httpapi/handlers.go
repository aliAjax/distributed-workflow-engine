package httpapi

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/acme/distributed-workflow-engine/internal/eventbus/domain"
	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	schedulerdomain "github.com/acme/distributed-workflow-engine/internal/scheduler/domain"
	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

type createWorkflowRequest struct {
	ProjectID   string                    `json:"project_id,omitempty"`
	Name        string                    `json:"name"`
	Description string                    `json:"description,omitempty"`
	Definition  workflowdomain.Definition `json:"definition"`
}

func (s *Server) handleCreateWorkflow(w http.ResponseWriter, r *http.Request) {
	var input createWorkflowRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	principal := principalFrom(r.Context())
	projectID := input.ProjectID
	if principal.ProjectID != "" {
		projectID = principal.ProjectID
	}
	workflow := workflowdomain.Workflow{
		TenantID:    principal.TenantID,
		ProjectID:   projectID,
		Name:        input.Name,
		Description: input.Description,
	}
	created, err := s.workflows.Create(r.Context(), struct {
		Workflow   workflowdomain.Workflow
		Definition workflowdomain.Definition
	}{Workflow: workflow, Definition: input.Definition})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.registerDefinitionTriggers(r.Context(), principal.TenantID, projectID, created.ID, created.CurrentVersion, input.Definition.Triggers); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *Server) handleListWorkflows(w http.ResponseWriter, r *http.Request) {
	principal := principalFrom(r.Context())
	limit, offset := pagination(r)
	items, err := s.workflows.List(r.Context(), principal.TenantID, principal.ProjectID, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
}

func (s *Server) handleGetWorkflow(w http.ResponseWriter, r *http.Request) {
	principal := principalFrom(r.Context())
	item, err := s.workflows.Get(r.Context(), principal.TenantID, principal.ProjectID, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) handleCreateWorkflowVersion(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Definition workflowdomain.Definition `json:"definition"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	principal := principalFrom(r.Context())
	version, err := s.workflows.PublishVersion(r.Context(), principal.TenantID, principal.ProjectID, r.PathValue("id"), input.Definition, principal.KeyID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := s.registerDefinitionTriggers(r.Context(), principal.TenantID, principal.ProjectID, r.PathValue("id"), version.Version, input.Definition.Triggers); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, version)
}

type createExecutionRequest struct {
	WorkflowID     string         `json:"workflow_id"`
	Version        int            `json:"version,omitempty"`
	Input          map[string]any `json:"input,omitempty"`
	Source         string         `json:"source,omitempty"`
	TriggerRef     string         `json:"trigger_ref,omitempty"`
	Priority       int            `json:"priority,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
}

func (s *Server) handleCreateExecution(w http.ResponseWriter, r *http.Request) {
	var input createExecutionRequest
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if input.Input == nil {
		input.Input = map[string]any{}
	}
	principal := principalFrom(r.Context())
	source := executiondomain.TriggerManual
	if input.Source != "" {
		source = executiondomain.TriggerSource(input.Source)
	}
	execution, err := s.executions.Create(r.Context(), executionapplication.CreateInput{
		TenantID:       principal.TenantID,
		ProjectID:      principal.ProjectID,
		WorkflowID:     input.WorkflowID,
		Version:        input.Version,
		Input:          input.Input,
		Source:         source,
		TriggerRef:     input.TriggerRef,
		Priority:       input.Priority,
		IdempotencyKey: input.IdempotencyKey,
	})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, execution)
}

func (s *Server) handleListExecutions(w http.ResponseWriter, r *http.Request) {
	principal := principalFrom(r.Context())
	limit, offset := pagination(r)
	items, err := s.executions.List(r.Context(), principal.TenantID, principal.ProjectID, limit, offset)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "limit": limit, "offset": offset})
}

func (s *Server) handleGetExecution(w http.ResponseWriter, r *http.Request) {
	principal := principalFrom(r.Context())
	item, err := s.executions.Get(r.Context(), principal.TenantID, r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	writeJSON(w, http.StatusOK, item)
}

func (s *Server) handleExecutionAction(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Action string `json:"action"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	principal := principalFrom(r.Context())
	id := r.PathValue("id")
	var err error
	switch strings.ToLower(input.Action) {
	case "pause":
		err = s.executions.Pause(r.Context(), principal.TenantID, id)
	case "resume":
		err = s.executions.Resume(r.Context(), principal.TenantID, id)
	case "cancel":
		err = s.executions.Cancel(r.Context(), principal.TenantID, id)
	case "terminate":
		err = s.executions.Terminate(r.Context(), principal.TenantID, id)
	default:
		writeError(w, http.StatusBadRequest, fmt.Errorf("unsupported action %q", input.Action))
		return
	}
	if err != nil {
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"action": input.Action, "execution_id": id})
}

func (s *Server) handleListNodes(w http.ResponseWriter, r *http.Request) {
	items, err := s.executions.ListNodes(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleListEvents(w http.ResponseWriter, r *http.Request) {
	items, err := s.executions.ListEvents(r.Context(), r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

func (s *Server) handleExternalSignal(w http.ResponseWriter, r *http.Request) {
	var input struct {
		ExecutionID string         `json:"execution_id,omitempty"`
		Event       string         `json:"event"`
		Payload     map[string]any `json:"payload,omitempty"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	principal := principalFrom(r.Context())
	signal := domain.ExternalSignal{
		TenantID:    principal.TenantID,
		ProjectID:   principal.ProjectID,
		ExecutionID: input.ExecutionID,
		Event:       input.Event,
		Payload:     input.Payload,
	}
	if err := s.eventbus.PublishExternalSignal(r.Context(), signal); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"event": input.Event, "status": "accepted"})
}

func (s *Server) handleCreateTenant(w http.ResponseWriter, r *http.Request) {
	var input struct {
		Name   string                `json:"name"`
		Slug   string                `json:"slug,omitempty"`
		Quotas tenantdomain.QuotaSet `json:"quotas,omitempty"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	tenant, err := s.tenants.CreateTenant(r.Context(), tenantdomain.Tenant{Name: input.Name, Slug: input.Slug, Status: "active", Quotas: input.Quotas})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, tenant)
}

func (s *Server) handleCreateProject(w http.ResponseWriter, r *http.Request) {
	var input struct {
		TenantID string `json:"tenant_id"`
		Name     string `json:"name"`
		Slug     string `json:"slug,omitempty"`
	}
	if err := decodeJSON(r, &input); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if input.TenantID == "" {
		input.TenantID = principalFrom(r.Context()).TenantID
	}
	project, err := s.tenants.CreateProject(r.Context(), tenantdomain.Project{TenantID: input.TenantID, Name: input.Name, Slug: input.Slug})
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, project)
}

func pagination(r *http.Request) (int, int) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if limit > 100 {
		limit = 50
	}
	return limit, offset
}

func (s *Server) registerDefinitionTriggers(ctx context.Context, tenantID, projectID, workflowID string, version int, triggers []workflowdomain.Trigger) error {
	for _, definition := range triggers {
		if definition.Type != "cron" && definition.Type != "event" {
			continue
		}
		triggerType := schedulerdomain.TriggerCron
		event := ""
		cron := ""
		if definition.Type == "event" {
			triggerType = schedulerdomain.TriggerEvent
			event = definition.Event
		} else {
			cron = definition.Cron
		}
		trigger := schedulerdomain.Trigger{
			TenantID:   tenantID,
			ProjectID:  projectID,
			WorkflowID: workflowID,
			Version:    version,
			Type:       triggerType,
			Cron:       cron,
			Event:      event,
			Enabled:    true,
		}
		if _, err := s.scheduler.Register(ctx, trigger); err != nil {
			return err
		}
	}
	return nil
}
