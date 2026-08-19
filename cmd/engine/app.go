package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/acme/distributed-workflow-engine/api/grpcapi"
	"github.com/acme/distributed-workflow-engine/api/httpapi"
	"github.com/acme/distributed-workflow-engine/internal/config"
	eventbusadapter "github.com/acme/distributed-workflow-engine/internal/eventbus/adapter"
	eventbusapplication "github.com/acme/distributed-workflow-engine/internal/eventbus/application"
	eventbusinfra "github.com/acme/distributed-workflow-engine/internal/eventbus/infrastructure"
	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executioninfra "github.com/acme/distributed-workflow-engine/internal/execution/infrastructure"
	"github.com/acme/distributed-workflow-engine/internal/logger"
	observabilityapplication "github.com/acme/distributed-workflow-engine/internal/observability/application"
	observabilityinfra "github.com/acme/distributed-workflow-engine/internal/observability/infrastructure"
	"github.com/acme/distributed-workflow-engine/internal/platform"
	policyapplication "github.com/acme/distributed-workflow-engine/internal/policy/application"
	policyinfra "github.com/acme/distributed-workflow-engine/internal/policy/infrastructure"
	runneradapter "github.com/acme/distributed-workflow-engine/internal/runner/adapter"
	runnerapplication "github.com/acme/distributed-workflow-engine/internal/runner/application"
	scheduleradapter "github.com/acme/distributed-workflow-engine/internal/scheduler/adapter"
	schedulerapplication "github.com/acme/distributed-workflow-engine/internal/scheduler/application"
	schedulerinfra "github.com/acme/distributed-workflow-engine/internal/scheduler/infrastructure"
	tenantapplication "github.com/acme/distributed-workflow-engine/internal/tenant/application"
	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
	tenantinfra "github.com/acme/distributed-workflow-engine/internal/tenant/infrastructure"
	workflowapplication "github.com/acme/distributed-workflow-engine/internal/workflow/application"
	workflowinfra "github.com/acme/distributed-workflow-engine/internal/workflow/infrastructure"
)

type app struct {
	cfg     config.Config
	log     logger.Logger
	metrics *platform.Metrics
	db      *sql.DB
	redis   *redis.Client

	workflows  *workflowapplication.Service
	executions *executionapplication.Service
	tenants    *tenantapplication.Service
	scheduler  *schedulerapplication.Service
	eventbus   *eventbusapplication.Service
	policy     *policyapplication.Service
	health     *observabilityapplication.Service
	runner     *runnerapplication.Service
	http       *httpapi.Server
	grpc       *grpcapi.Server
}

func newApp(cfg config.Config, log logger.Logger) *app {
	return &app{cfg: cfg, log: log}
}

func (a *app) openInfra(ctx context.Context) error {
	postgresCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	db, err := platform.OpenPostgres(postgresCtx, platform.NormalizeDSN(a.cfg.Postgres.DSN), a.cfg.Postgres.MaxOpenConns, a.cfg.Postgres.MaxIdleConns, a.cfg.Postgres.ConnMaxLifetime)
	if err != nil {
		return err
	}
	a.db = db

	redisCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	redisClient, err := platform.OpenRedis(redisCtx, a.cfg.Redis.Addr, a.cfg.Redis.Password, a.cfg.Redis.DB, a.cfg.Redis.PoolSize)
	if err != nil {
		_ = platform.ClosePostgres(db)
		return err
	}
	a.redis = redisClient
	a.metrics = platform.NewMetrics()
	return nil
}

func (a *app) migrate(ctx context.Context) error {
	return platform.NewMigrator(a.db, a.cfg.Migrations.Dir).Up(ctx)
}

func (a *app) seed(ctx context.Context) error {
	tenantRepo := tenantinfra.NewPostgresRepository(a.db)
	projectRepo := tenantRepo
	tenant := tenantdomain.Tenant{
		Name:   "Default Tenant",
		Slug:   "default",
		Status: "active",
		Quotas: tenantdomain.QuotaSet{MaxWorkflows: 100, MaxExecutions: 10000, MaxConcurrent: 20, MaxProjects: 20},
	}
	created, err := tenantapplication.NewService(tenantRepo).CreateTenant(ctx, tenant)
	if err != nil {
		created, err = tenantapplication.NewService(tenantRepo).GetTenantBySlug(ctx, tenant.Slug)
		if err != nil {
			return err
		}
	} else {
		created, err = tenantapplication.NewService(tenantRepo).GetTenantBySlug(ctx, tenant.Slug)
		if err != nil {
			return err
		}
	}
	project := tenantdomain.Project{TenantID: created.ID, Name: "Default Project", Slug: "default"}
	projectCreated, err := tenantapplication.NewService(projectRepo).CreateProject(ctx, project)
	if err != nil {
		projects, listErr := tenantapplication.NewService(projectRepo).ListProjects(ctx, created.ID)
		if listErr != nil || len(projects) == 0 {
			return err
		}
		projectCreated = projects[0]
	} else {
		projects, listErr := tenantapplication.NewService(projectRepo).ListProjects(ctx, created.ID)
		if listErr != nil || len(projects) == 0 {
			return err
		}
		projectCreated = projects[0]
	}
	a.http.SetBootstrapPrincipal(tenantdomain.Principal{
		TenantID:  created.ID,
		ProjectID: projectCreated.ID,
		KeyID:     "bootstrap",
		Roles:     []string{string(tenantdomain.RoleAdmin)},
		Scopes:    []string{"workflow:*", "execution:*", "event:*", "tenant:*", "project:*"},
	})
	a.grpc.SetBootstrapPrincipal(tenantdomain.Principal{
		TenantID:  created.ID,
		ProjectID: projectCreated.ID,
		KeyID:     "bootstrap",
		Roles:     []string{string(tenantdomain.RoleAdmin)},
		Scopes:    []string{"workflow:*", "execution:*", "event:*", "tenant:*", "project:*"},
	})
	_ = projectCreated
	return nil
}

func (a *app) buildServices(ctx context.Context) {
	workflowRepo := workflowinfra.NewPostgresRepository(a.db)
	executionRepo := executioninfra.NewPostgresRepository(a.db)
	queue := executioninfra.NewRedisQueue(a.redis)
	tenantRepo := tenantinfra.NewPostgresRepository(a.db)
	schedulerRepo := schedulerinfra.NewPostgresRepository(a.db)
	policyStore := policyinfra.NewRedisStore(a.redis)
	eventPublisher := eventbusinfra.NewRedisPublisher(a.redis)
	eventPersister := eventbusadapter.NewExecutionPersister(executionRepo)
	runnerMetrics := runneradapter.NewMetricsRecorder(a.metrics)

	a.workflows = workflowapplication.NewService(workflowRepo)
	a.tenants = tenantapplication.NewService(tenantRepo)
	a.executions = executionapplication.NewService(executionRepo, queue, a.workflows)
	a.eventbus = eventbusapplication.NewService(eventPublisher, eventPersister)
	a.policy = policyapplication.NewService(policyStore)
	a.health = observabilityapplication.NewService(
		observabilityinfra.NewPostgresChecker(a.db),
		observabilityinfra.NewRedisChecker(a.redis),
	)
	executor := scheduleradapter.NewExecutionExecutor(a.executions)
	a.scheduler = schedulerapplication.NewService(schedulerRepo, executor)
	a.runner = runnerapplication.NewService(
		*a.executions,
		executionRepo,
		queue,
		a.workflows,
		a.tenants,
		a.policy,
		a.eventbus,
		runnerMetrics,
		runnerapplication.Config{
			PollInterval:   a.cfg.Worker.PollInterval,
			LeaseDuration:  a.cfg.Worker.LeaseDuration,
			HeartbeatEvery: a.cfg.Worker.HeartbeatEvery,
		},
	)
	a.eventbus.SetSignalResolver(a.runner)
	a.http = httpapi.New(a.cfg, a.log, a.metrics, a.workflows, a.executions, a.tenants, a.scheduler, a.eventbus, a.policy, a.health)
	a.grpc = grpcapi.New(a.workflows, a.executions, a.tenants, a.cfg.Auth.BootstrapToken)
}

func (a *app) run(ctx context.Context, runWorkers bool) error {
	appCtx, cancel := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	errCh := make(chan error, 3)
	go func() {
		if err := a.grpc.Start(a.cfg.GRPC.Addr); err != nil {
			errCh <- fmt.Errorf("grpc serve: %w", err)
		}
	}()
	go func() {
		if err := a.http.Start(appCtx); err != nil {
			errCh <- fmt.Errorf("http serve: %w", err)
		}
	}()
	if runWorkers {
		go func() {
			if err := a.runner.Run(appCtx, a.cfg.Worker.Count); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- fmt.Errorf("runner: %w", err)
			}
		}()
		go func() {
			ticker := time.NewTicker(a.cfg.Scheduler.Interval)
			defer ticker.Stop()
			for {
				select {
				case <-appCtx.Done():
					return
				case <-ticker.C:
					if _, err := a.scheduler.Tick(appCtx, 20); err != nil {
						a.log.Error(appCtx, "scheduler tick failed", "error", err)
					}
				}
			}
		}()
	}

	select {
	case <-appCtx.Done():
		a.log.Info(ctx, "shutdown requested")
		a.grpc.Stop()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), a.cfg.HTTP.ShutdownTimeout)
		defer shutdownCancel()
		if err := a.http.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("http shutdown: %w", err)
		}
		return nil
	case err := <-errCh:
		a.grpc.Stop()
		return err
	}
}

func (a *app) close() {
	_ = platform.ClosePostgres(a.db)
	_ = platform.CloseRedis(a.redis)
}

func loadConfig(path string) (config.Config, error) {
	return config.Load(path)
}

func flagConfig() string {
	var path string
	flags := flag.NewFlagSet("engine", flag.ContinueOnError)
	flags.StringVar(&path, "config", os.Getenv("WORKFLOW_CONFIG"), "path to YAML config file")
	_ = flags.Parse(os.Args[1:])
	if path == "" {
		path = "configs/config.yaml"
	}
	return path
}

func main() {
	cfg, err := config.Load(flagConfig())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	log := logger.New(cfg.Log.Level, cfg.Log.Format)
	ctx := context.Background()
	engine := newApp(cfg, log)
	if err := engine.openInfra(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer engine.close()
	if err := engine.migrate(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	engine.buildServices(ctx)
	if err := engine.seed(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := engine.run(ctx, true); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
