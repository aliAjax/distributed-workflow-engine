package httpapi

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/acme/distributed-workflow-engine/internal/config"
	eventbusapplication "github.com/acme/distributed-workflow-engine/internal/eventbus/application"
	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	"github.com/acme/distributed-workflow-engine/internal/logger"
	observabilityapplication "github.com/acme/distributed-workflow-engine/internal/observability/application"
	"github.com/acme/distributed-workflow-engine/internal/platform"
	policyapplication "github.com/acme/distributed-workflow-engine/internal/policy/application"
	schedulerapplication "github.com/acme/distributed-workflow-engine/internal/scheduler/application"
	tenantapplication "github.com/acme/distributed-workflow-engine/internal/tenant/application"
	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
	workflowapplication "github.com/acme/distributed-workflow-engine/internal/workflow/application"
)

type Server struct {
	cfg                config.Config
	log                logger.Logger
	metrics            *platform.Metrics
	workflows          *workflowapplication.Service
	executions         *executionapplication.Service
	tenants            *tenantapplication.Service
	scheduler          *schedulerapplication.Service
	eventbus           *eventbusapplication.Service
	policy             *policyapplication.Service
	health             *observabilityapplication.Service
	bootstrapPrincipal tenantdomain.Principal
	httpServer         *http.Server
}

func New(
	cfg config.Config,
	log logger.Logger,
	metrics *platform.Metrics,
	workflows *workflowapplication.Service,
	executions *executionapplication.Service,
	tenants *tenantapplication.Service,
	scheduler *schedulerapplication.Service,
	eventbus *eventbusapplication.Service,
	policy *policyapplication.Service,
	health *observabilityapplication.Service,
) *Server {
	return &Server{
		cfg:        cfg,
		log:        log,
		metrics:    metrics,
		workflows:  workflows,
		executions: executions,
		tenants:    tenants,
		scheduler:  scheduler,
		eventbus:   eventbus,
		policy:     policy,
		health:     health,
	}
}

func (s *Server) SetBootstrapPrincipal(principal tenantdomain.Principal) {
	s.bootstrapPrincipal = principal
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", s.handleHealth)
	mux.HandleFunc("GET /readyz", s.handleReady)
	mux.Handle("GET /metrics", promhttp.HandlerFor(s.metrics.Registry(), promhttp.HandlerOpts{}))

	workflowCreate := s.withAuth("workflow", "create", http.HandlerFunc(s.handleCreateWorkflow))
	workflowList := s.withAuth("workflow", "read", http.HandlerFunc(s.handleListWorkflows))
	workflowGet := s.withAuth("workflow", "read", http.HandlerFunc(s.handleGetWorkflow))
	workflowVersion := s.withAuth("workflow", "update", http.HandlerFunc(s.handleCreateWorkflowVersion))
	executionCreate := s.withAuth("execution", "create", http.HandlerFunc(s.handleCreateExecution))
	executionList := s.withAuth("execution", "read", http.HandlerFunc(s.handleListExecutions))
	executionGet := s.withAuth("execution", "read", http.HandlerFunc(s.handleGetExecution))
	executionAction := s.withAuth("execution", "update", http.HandlerFunc(s.handleExecutionAction))
	executionNodes := s.withAuth("execution", "read", http.HandlerFunc(s.handleListNodes))
	executionEvents := s.withAuth("execution", "read", http.HandlerFunc(s.handleListEvents))
	externalSignal := s.withAuth("event", "create", http.HandlerFunc(s.handleExternalSignal))
	tenantCreate := s.withAuth("tenant", "create", http.HandlerFunc(s.handleCreateTenant))
	projectCreate := s.withAuth("project", "create", http.HandlerFunc(s.handleCreateProject))

	mux.Handle("POST /api/v1/workflows", workflowCreate)
	mux.Handle("GET /api/v1/workflows", workflowList)
	mux.Handle("GET /api/v1/workflows/{id}", workflowGet)
	mux.Handle("POST /api/v1/workflows/{id}/versions", workflowVersion)

	mux.Handle("POST /api/v1/executions", executionCreate)
	mux.Handle("GET /api/v1/executions", executionList)
	mux.Handle("GET /api/v1/executions/{id}", executionGet)
	mux.Handle("POST /api/v1/executions/{id}/action", executionAction)
	mux.Handle("GET /api/v1/executions/{id}/nodes", executionNodes)
	mux.Handle("GET /api/v1/executions/{id}/events", executionEvents)

	mux.Handle("POST /api/v1/events/external", externalSignal)
	mux.Handle("POST /api/v1/tenants", tenantCreate)
	mux.Handle("POST /api/v1/projects", projectCreate)

	var handler http.Handler = mux
	handler = s.withRequestContext(handler)
	handler = s.withRecovery(handler)
	handler = s.withTimeout(s.cfg.HTTP.WriteTimeout)(handler)
	handler = s.withLogging(handler)
	handler = s.withMetrics(handler)
	handler = s.withGlobalRateLimit(handler)
	return handler
}

func (s *Server) Start(ctx context.Context) error {
	s.httpServer = &http.Server{
		Addr:         s.cfg.HTTP.Addr,
		Handler:      s.Handler(),
		ReadTimeout:  s.cfg.HTTP.ReadTimeout,
		WriteTimeout: s.cfg.HTTP.WriteTimeout,
	}
	errCh := make(chan error, 1)
	go func() {
		s.log.Info(ctx, "http server listening", "addr", s.cfg.HTTP.Addr)
		errCh <- s.httpServer.ListenAndServe()
	}()
	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("http serve: %w", err)
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.HTTP.ShutdownTimeout)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer == nil {
		return nil
	}
	return s.httpServer.Shutdown(ctx)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "time": time.Now().UTC()})
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	report := s.health.Ready(r.Context())
	status := http.StatusOK
	if report.Status != "ok" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, report)
}
