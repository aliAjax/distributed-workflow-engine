package grpcapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"

	executionapplication "github.com/acme/distributed-workflow-engine/internal/execution/application"
	executiondomain "github.com/acme/distributed-workflow-engine/internal/execution/domain"
	tenantapplication "github.com/acme/distributed-workflow-engine/internal/tenant/application"
	tenantdomain "github.com/acme/distributed-workflow-engine/internal/tenant/domain"
	workflowapplication "github.com/acme/distributed-workflow-engine/internal/workflow/application"
	workflowdomain "github.com/acme/distributed-workflow-engine/internal/workflow/domain"
)

const serviceName = "workflow.v1.Management"

type managementService interface {
	managementServerMarker()
}

type jsonCodec struct{}

func (jsonCodec) Name() string { return "json" }

func (jsonCodec) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (jsonCodec) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}

type Server struct {
	server         *grpc.Server
	listener       net.Listener
	workflows      *workflowapplication.Service
	executions     *executionapplication.Service
	tenants        *tenantapplication.Service
	bootstrap      tenantdomain.Principal
	bootstrapToken string
}

func New(workflows *workflowapplication.Service, executions *executionapplication.Service, tenants *tenantapplication.Service, bootstrapToken string) *Server {
	s := &Server{workflows: workflows, executions: executions, tenants: tenants, bootstrapToken: bootstrapToken}
	server := grpc.NewServer(
		grpc.ForceServerCodec(jsonCodec{}),
		grpc.UnaryInterceptor(s.authInterceptor),
	)
	server.RegisterService(managementDesc(), s)
	s.server = server
	return s
}

func (s *Server) managementServerMarker() {}

func (s *Server) SetBootstrapPrincipal(principal tenantdomain.Principal) {
	s.bootstrap = principal
}

func (s *Server) Start(addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("grpc listen: %w", err)
	}
	s.listener = listener
	return s.server.Serve(listener)
}

func (s *Server) Stop() {
	if s.server != nil {
		s.server.GracefulStop()
	}
}

func (s *Server) authInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	principal, err := s.authenticate(ctx)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, principalKey{}, principal)
	return handler(ctx, req)
}

func (s *Server) authenticate(ctx context.Context) (tenantdomain.Principal, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return tenantdomain.Principal{}, fmt.Errorf("missing grpc metadata")
	}
	values := md.Get("authorization")
	if len(values) == 0 || !strings.HasPrefix(values[0], "Bearer ") {
		return tenantdomain.Principal{}, fmt.Errorf("authorization bearer token required")
	}
	token := strings.TrimSpace(strings.TrimPrefix(values[0], "Bearer "))
	if token == s.bootstrapToken && s.bootstrap.TenantID != "" {
		return s.bootstrap, nil
	}
	principal, err := s.tenants.Authenticate(ctx, token)
	if err != nil {
		return tenantdomain.Principal{}, fmt.Errorf("authenticate api key: %w", err)
	}
	return principal, nil
}

type principalKey struct{}

func principalFrom(ctx context.Context) tenantdomain.Principal {
	value, _ := ctx.Value(principalKey{}).(tenantdomain.Principal)
	return value
}

func managementDesc() *grpc.ServiceDesc {
	return &grpc.ServiceDesc{
		ServiceName: serviceName,
		HandlerType: (*managementService)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Health",
				Handler: unaryMethod("Health", func(ctx context.Context, srv *Server, input map[string]any) (map[string]any, error) {
					return map[string]any{"status": "ok", "time": time.Now().UTC().Format(time.RFC3339)}, nil
				}),
			},
			{
				MethodName: "CreateWorkflow",
				Handler: unaryMethod("CreateWorkflow", func(ctx context.Context, srv *Server, input map[string]any) (map[string]any, error) {
					principal := principalFrom(ctx)
					name, _ := input["name"].(string)
					description, _ := input["description"].(string)
					projectID := principal.ProjectID
					if raw, ok := input["project_id"].(string); ok && raw != "" {
						projectID = raw
					}
					definition := workflowdomain.Definition{}
					if raw, ok := input["definition"]; ok {
						encoded, _ := json.Marshal(raw)
						_ = json.Unmarshal(encoded, &definition)
					}
					workflow := workflowdomain.Workflow{
						TenantID: principal.TenantID, ProjectID: projectID, Name: name, Description: description,
					}
					created, err := srv.workflows.Create(ctx, workflowapplication.CreateInput{Workflow: workflow, Definition: definition})
					if err != nil {
						return nil, err
					}
					return map[string]any{"id": created.ID, "name": created.Name, "current_version": created.CurrentVersion}, nil
				}),
			},
			{
				MethodName: "GetExecution",
				Handler: unaryMethod("GetExecution", func(ctx context.Context, srv *Server, input map[string]any) (map[string]any, error) {
					principal := principalFrom(ctx)
					id, _ := input["id"].(string)
					execution, err := srv.executions.Get(ctx, principal.TenantID, id)
					if err != nil {
						return nil, err
					}
					return map[string]any{"id": execution.ID, "workflow_id": execution.WorkflowID, "version": execution.Version, "status": string(execution.Status)}, nil
				}),
			},
			{
				MethodName: "TriggerExecution",
				Handler: unaryMethod("TriggerExecution", func(ctx context.Context, srv *Server, input map[string]any) (map[string]any, error) {
					principal := principalFrom(ctx)
					workflowID, _ := input["workflow_id"].(string)
					version := intField(input, "version")
					inputMap := mapField(input, "input")
					execution, err := srv.executions.Create(ctx, executionapplication.CreateInput{
						TenantID: principal.TenantID, ProjectID: principal.ProjectID, WorkflowID: workflowID,
						Version: version, Input: inputMap, Source: executiondomain.TriggerManual,
					})
					if err != nil {
						return nil, err
					}
					return map[string]any{"id": execution.ID, "status": string(execution.Status)}, nil
				}),
			},
			{
				MethodName: "ListExecutions",
				Handler: unaryMethod("ListExecutions", func(ctx context.Context, srv *Server, input map[string]any) (map[string]any, error) {
					principal := principalFrom(ctx)
					items, err := srv.executions.List(ctx, principal.TenantID, principal.ProjectID, 50, 0)
					if err != nil {
						return nil, err
					}
					encoded := make([]map[string]any, 0, len(items))
					for _, item := range items {
						encoded = append(encoded, map[string]any{"id": item.ID, "workflow_id": item.WorkflowID, "version": item.Version, "status": string(item.Status)})
					}
					return map[string]any{"items": encoded}, nil
				}),
			},
		},
	}
}

func unaryMethod(name string, method func(context.Context, *Server, map[string]any) (map[string]any, error)) grpc.MethodHandler {
	return func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error) {
		input := map[string]any{}
		if err := dec(&input); err != nil {
			return nil, fmt.Errorf("decode %s request: %w", name, err)
		}
		info := &grpc.UnaryServerInfo{Server: srv, FullMethod: fmt.Sprintf("/%s/%s", serviceName, name)}
		handler := func(ctx context.Context, req any) (any, error) {
			return method(ctx, srv.(*Server), req.(map[string]any))
		}
		if interceptor == nil {
			return handler(ctx, input)
		}
		return interceptor(ctx, input, info, handler)
	}
}

func intField(input map[string]any, key string) int {
	switch value := input[key].(type) {
	case float64:
		return int(value)
	case int:
		return value
	case string:
		var parsed int
		_, _ = fmt.Sscanf(value, "%d", &parsed)
		return parsed
	default:
		return 0
	}
}

func mapField(input map[string]any, key string) map[string]any {
	if value, ok := input[key].(map[string]any); ok {
		return value
	}
	return map[string]any{}
}

var _ = encoding.GetCodec
