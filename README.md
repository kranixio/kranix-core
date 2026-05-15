# kranix-core

> The orchestration engine — state, scheduling, and reconciliation for the Kranix platform.

`kranix-core` is the brain of the Kranix ecosystem. It owns all business logic: reconciliation loops, workload scheduling, state management, event routing, and policy enforcement. Every other Kranix repo either sends work *into* core or gets driven *by* core. Nothing touches infrastructure directly except through it.

---

## What it does

- Maintains desired vs actual state for all managed workloads
- Runs continuous reconciliation loops (Git intent → runtime state)
- Schedules and coordinates deployments across backends
- Routes events between the API layer and runtime drivers
- Enforces infra policies (resource limits, namespace isolation, rollout rules)
- Provides the plugin interface for extending Kranix with custom controllers

---

## Architecture position

```
kranix-api  ──►  kranix-core  ──►  kranix-runtime
                    │
                    ├──►  kranix-operator
                    └──►  kranix-packages (imported)
```

`kranix-core` sits between the API surface and the infra drivers. It never exposes HTTP endpoints and never talks to Docker or Kubernetes directly — those concerns belong to `kranix-runtime` and `kranix-operator`.

---

## Core concepts

### Reconciliation loop

Kranix-core runs a continuous control loop:

```
Observe current state  →  Compare to desired state  →  Compute diff  →  Apply actions  →  repeat
```

Desired state comes from three sources, merged by priority:

| Source       | Examples                              |
|--------------|---------------------------------------|
| Git manifests | `KranixApp` CRDs committed to a repo  |
| API intent   | `POST /deploy` from CLI or MCP agent  |
| AI intent    | Agent-issued actions via kranix-mcp    |

### Workload model

Every managed unit is a `Workload` object with:

- `spec` — desired configuration (image, replicas, env, resources)
- `status` — current observed state (running, degraded, crashed)
- `history` — immutable log of all state transitions

### Event bus

Internal components communicate via a typed event bus. Events flow:

```
API receives request
  → publishes WorkloadDeployRequested
    → Scheduler picks it up
      → publishes WorkloadScheduled
        → Runtime driver executes
          → publishes WorkloadRunning / WorkloadFailed
```

---

## Project structure

```
kranix-core/
├── cmd/                  # Entry point (if running standalone)
├── internal/
│   ├── reconciler/       # Main reconciliation loop
│   ├── scheduler/        # Workload placement logic
│   ├── state/            # State store interface + implementations
│   ├── eventbus/         # Typed internal event system
│   ├── policy/           # Policy engine (limits, rules)
│   └── plugin/           # Plugin/controller extension interface
├── pkg/
│   └── types/            # Shared domain types (re-exported from kranix-packages)
├── config/               # Default configuration schemas
└── tests/
    ├── unit/
    └── integration/
```

---

## Getting started

### Prerequisites

- Go 1.22+
- `kranix-packages` (auto-resolved via Go modules)

### Run locally

```bash
git clone https://github.com/kranix-io/kranix-core
cd kranix-core
go mod download
go run ./cmd/core --config ./config/local.yaml
```

### Run tests

```bash
go test ./...
go test ./internal/reconciler/... -v   # reconciler unit tests
go test ./tests/integration/... -tags integration
```

---

## Configuration

`kranix-core` is configured via YAML:

```yaml
core:
  reconcile_interval: 15s
  max_concurrent_reconciles: 10

state:
  backend: memory          # memory | postgres | etcd
  postgres_dsn: ""

policy:
  default_cpu_limit: "500m"
  default_memory_limit: "512Mi"
  enforce_namespace_isolation: true

eventbus:
  buffer_size: 1024
```

---

## Extending with custom controllers

Implement the `Controller` interface and register it on startup:

```go
type Controller interface {
    Name() string
    Reconcile(ctx context.Context, workload *types.Workload) error
    ShouldHandle(workload *types.Workload) bool
}

// Register in cmd/core/main.go
engine.RegisterController(&MyCustomController{})
```

---

## Connectivity

| Repo | Relationship |
|---|---|
| `kranix-api` | Calls core via internal Go interface |
| `kranix-runtime` | Core drives runtime via the `RuntimeDriver` interface |
| `kranix-operator` | Core drives operator reconciliation loops |
| `kranix-packages` | Core imports shared types and utilities |

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md). All reconciliation logic must have unit tests. Integration tests require a running Docker daemon or a local `kind` cluster.

## License

Apache 2.0 — see [LICENSE](./LICENSE).
