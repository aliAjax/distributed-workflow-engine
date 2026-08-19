# Distributed Workflow Engine

纯 Go 实现的分布式 DAG 工作流编排与执行引擎。系统提供 CLI、REST API 和 gRPC 管理接口，使用 PostgreSQL 持久化定义、实例、节点状态和事件日志，使用 Redis 实现队列、租约、限流和短期状态。多副本部署时不在进程内共享执行状态。

## 功能

- YAML/JSON 工作流定义：节点、输入输出、依赖、条件分支、循环、并行、超时、重试、补偿、校验和版本管理。
- 调度执行：手动、Cron、外部事件触发；持久化 Redis 队列、多 worker 并发消费、幂等、心跳、续租、失败转移、暂停/恢复/取消/终止。
- 状态机：实例与节点状态持久化，事件日志落库，重启后从数据库继续，已成功节点不会重复执行。
- 事件总线：Redis Pub/Sub 加 PostgreSQL `wait_events`，外部信号不依赖进程内存。
- 策略与限流：工作流/租户并发上限、优先级、队列延迟、退避策略、HTTP 限流。
- 可观测性：执行轨迹、节点耗时、重试原因、错误信息、实例查询、Prometheus 指标、`/healthz`、`/readyz`。
- 多租户与权限：租户、项目、API Key、HMAC 摘要认证、基于资源和动作的访问控制。

## 目录结构

```text
cmd/engine              服务入口与依赖装配
cmd/grpcping            gRPC JSON-codec 管理接口探针
internal/workflow       工作流领域
internal/execution      执行实例领域
internal/scheduler      调度领域
internal/eventbus       事件总线领域
internal/runner         执行器领域
internal/policy         策略领域
internal/tenant         租户领域
internal/observability  可观测性领域
api/httpapi             REST API
api/grpcapi             gRPC 管理接口
configs                 YAML 配置
migrations              PostgreSQL migrations
deploy                  Dockerfile 与部署资源
examples/workflows      示例工作流
```

每个领域按 `domain`、`application`、`adapter`、`infrastructure` 分层，通过接口构造注入；所有跨模块调用传递 `context.Context`，错误使用 `fmt.Errorf` 包裹，日志使用结构化 `slog`。

## 快速启动

要求 Go 1.22、Docker Compose。

```bash
docker compose up -d postgres redis
go run ./cmd/engine -config configs/config.yaml
```

默认监听 `:8080` HTTP、`:9090` gRPC，bootstrap token 为 `dev-secret-token`。若端口被占用，可通过环境变量覆盖：

```bash
HTTP_ADDR=:18080 GRPC_ADDR=:19090 go run ./cmd/engine -config configs/config.yaml
```

## 配置

主要配置位于 `configs/config.yaml`，支持以下环境变量覆盖：

| 环境变量 | 说明 |
| --- | --- |
| `HTTP_ADDR` | HTTP 监听地址 |
| `GRPC_ADDR` | gRPC 监听地址 |
| `POSTGRES_DSN` | PostgreSQL DSN |
| `REDIS_ADDR` | Redis 地址 |
| `REDIS_PASSWORD` | Redis 密码 |
| `AUTH_BOOTSTRAP_TOKEN` | 启动时 bootstrap token |
| `LOG_LEVEL` | `debug` / `info` / `warn` / `error` |
| `WORKER_COUNT` | 内嵌 worker 数量 |
| `SCHEDULER_INTERVAL` | 调度轮询间隔，如 `2s` |

## REST API 示例

### 健康与指标

```bash
curl http://localhost:8080/healthz
curl http://localhost:8080/readyz
curl http://localhost:8080/metrics
```

### 创建并触发工作流

```bash
curl -X POST http://localhost:8080/api/v1/workflows \
  -H 'Authorization: Bearer dev-secret-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "name":"hello",
    "definition":{
      "name":"hello",
      "version":1,
      "nodes":{
        "start":{"id":"start","type":"echo","name":"start","command":"hello"},
        "done":{"id":"done","type":"log","name":"done","command":"done"}
      },
      "edges":[{"from":"start","to":"done"}],
      "triggers":[{"type":"manual"}]
    }
  }'
```

```bash
curl -X POST http://localhost:8080/api/v1/executions \
  -H 'Authorization: Bearer dev-secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"workflow_id":"<workflow-id>","input":{"greeting":"hi"}}'
```

### 查询执行、节点和事件

```bash
curl http://localhost:8080/api/v1/executions/<execution-id> \
  -H 'Authorization: Bearer dev-secret-token'
curl http://localhost:8080/api/v1/executions/<execution-id>/nodes \
  -H 'Authorization: Bearer dev-secret-token'
curl http://localhost:8080/api/v1/executions/<execution-id>/events \
  -H 'Authorization: Bearer dev-secret-token'
```

### 外部事件等待

创建包含 `wait_event` 节点的定义并触发后，实例会进入 `waiting` 状态。发送外部信号：

```bash
curl -X POST http://localhost:8080/api/v1/events/external \
  -H 'Authorization: Bearer dev-secret-token' \
  -H 'Content-Type: application/json' \
  -d '{"execution_id":"<execution-id>","event":"approval","payload":{"approved":true}}'
```

## gRPC 验证

本项目 gRPC 管理接口使用 JSON codec，避免仓库依赖生成代码。可使用内置探针：

```bash
go run ./cmd/grpcping -addr localhost:19090 -token dev-secret-token -method Health
go run ./cmd/grpcping -addr localhost:19090 -token dev-secret-token -method CreateWorkflow
```

## 验证方法

```bash
go build ./...
curl -fsS http://localhost:18080/healthz
curl -fsS http://localhost:18080/readyz
curl -fsS http://localhost:18080/metrics | head
go run ./cmd/grpcping -addr localhost:19090 -token dev-secret-token -method Health
```

## 关闭

```bash
docker compose down
```
