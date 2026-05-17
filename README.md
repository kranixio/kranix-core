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

drift_detection:
  enabled: true
  check_interval: 30s

event_sourcing:
  enabled: true
  storage_backend: memory  # memory | postgres | etcd
  max_event_age: 720h      # 30 days
  compression: false

autoscaler:
  check_interval: 30s
  metrics_provider: "prometheus"  # prometheus, custom

scheduler:
  cost_provider: "aws"           # aws, gcp, azure, custom
  node_registry: "kubernetes"    # kubernetes, custom

dependency:
  enabled: true
  max_depth: 10

prediction:
  model_type: "simple"          # simple, ml, custom
  check_interval: 60s

multitenancy:
  enabled: true
  default_isolation: true
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
```

---

## New Features

### Smart Auto-scaling

The auto-scaler automatically adjusts replica counts based on CPU, memory, and custom metrics:

```yaml
auto_scaling:
  enabled: true
  min_replicas: 2
  max_replicas: 10
  target_cpu_utilization: 70        # Scale up when CPU > 70%
  target_memory_utilization: 80     # Scale up when memory > 80%
  custom_metrics:
    - name: requests_per_second
      type: pods
      metric_name: http_requests_total
      target:
        type: average
        average_value: "1000"
  scale_down_cooldown_seconds: 300
  scale_up_cooldown_seconds: 60
```

### Cost-aware Scheduling

Route workloads to the cheapest available nodes/regions:

```yaml
scheduling:
  cost_aware: true
  preferred_regions:
    - us-east-1
    - us-west-2
  preferred_zones:
    - us-east-1a
  node_selectors:
    node.kubernetes.io/instance-type: "t3.medium"
  max_cost_per_hour: "0.50"
```

### Advanced Rollout Strategies

Deploy workloads using canary, blue-green, or A/B testing strategies:

```yaml
rollout_strategy:
  type: canary              # rolling, recreate, bluegreen, canary, abtest
  max_unavailable: 1
  canary_config:
    replicas: 2
    percentage: 10
    analysis_duration: "10m"
    success_threshold: 99
    metrics:
      - error_rate
      - latency_p99
    auto_promote: true
```

For A/B testing:

```yaml
rollout_strategy:
  type: abtest
  ab_test_config:
    variant_a: "myapp:v1.0"
    variant_b: "myapp:v2.0"
    traffic_split: 20           # 20% to variant B
    analysis_duration: "30m"
    metrics:
      - conversion_rate
      - user_engagement
    auto_select_winner: true
```

---

## New Features (v2.0)

### Dependency Graph

Automatically deploy services in the correct order based on dependencies:

```yaml
dependencies:
  - workloadId: "database"
    type: "depends_on"
    condition: "healthy"
    timeout: "5m"
  - workloadId: "cache"
    type: "waits_for"
    condition: "running"
```

The dependency resolver:
- Performs topological sort to determine deployment order
- Detects circular dependencies
- Waits for dependencies to reach specified conditions
- Supports conditions: `running`, `healthy`, `ready`

### Failure Prediction

ML-based failure prediction using historical crash/OOM data:

```yaml
failure_prediction:
  enabled: true
  modelType: "ml"              # simple, ml, custom
  predictionWindow: "15m"
  threshold: 0.75              # probability threshold (0-1)
  features:
    - cpu_usage
    - memory_usage
    - request_rate
    - error_rate
  mitigationActions:
    - scale_up
    - restart
    - migrate
```

The prediction engine:
- Extracts features from workload metrics
- Uses configurable ML models (simple heuristic or custom)
- Triggers mitigation actions when failure probability exceeds threshold
- Collects historical data for model training

### Multi-tenancy Engine

Hard isolation between organizations with resource quotas:

```yaml
tenant:
  id: "org-123"
  name: "Acme Corp"
  namespace: "acme-prod"
  labels:
    environment: "production"
  quota:
    maxCPU: "16"
    maxMemory: "64Gi"
    maxWorkloads: 50
    maxReplicas: 200
    maxStorage: "1Ti"
    maxCustomMetrics: 20
  isolation:
    networkPolicy: true
    resourceQuota: true
    limitRange: true
    podSecurityPolicy: true
    storageClass: "tenant-storage"
```

The multi-tenancy engine:
- Enforces resource quotas per tenant
- Applies hard isolation policies (network, resource limits)
- Tracks resource usage per tenant
- Validates workloads against tenant constraints
- Supports dedicated storage classes per tenant

---

## New Features (v3.0)

### Drift Detection

Automatically detect when runtime state diverges from declared specifications:

```yaml
drift_detection:
  enabled: true
  check_interval: 30s
  alert_on_drift: true
  auto_reconcile: true
  monitored_fields:
    - replicas
    - env
  tolerance:
    replica_variance: 1
    resource_variance_pct: 10.0
    env_var_drift_allowed: false
    label_drift_allowed: true
  notification_hooks:
    - type: webhook
      url: "https://hooks.example.com/drift"
      headers:
        Authorization: "Bearer secret-token"
    - type: slack
      url: "https://hooks.slack.com/services/..."
```

The drift detection engine:
- Compares desired spec with actual runtime state at configurable intervals
- Detects replica count drift, resource drift, and configuration drift
- Supports configurable tolerance thresholds for acceptable variance
- Sends alerts via webhooks, Slack, email, or PagerDuty
- Optionally auto-reconciles drift by triggering reconciliation
- Provides detailed drift reports with severity levels (low, medium, high, critical)

### Event Sourcing

Full immutable log of every state transition for audit and debugging:

```yaml
event_sourcing:
  enabled: true
  storage_backend: memory  # memory | postgres | etcd
  max_event_age: 720h      # 30 days
  compression: false
```

The event sourcing system:
- Records every state transition as an immutable domain event
- Stores events with versioning for each workload aggregate
- Supports event replay to reconstruct historical state
- Provides event subscription for real-time monitoring
- Includes automatic cleanup of old events based on age
- Exposes event history via API endpoints in kranix-api

Event types recorded:
- `WorkloadCreated` - Initial workload creation
- `WorkloadUpdated` - Spec updates with old/new values
- `WorkloadDeleted` - Workload deletion
- `WorkloadPhaseTransition` - Phase changes with reason
- `WorkloadDriftDetected` - Drift detection events
- `WorkloadDriftReconciled` - Auto-reconciliation events
- `WorkloadScaled` - Scaling events with reason

API Endpoints (via kranix-api):
- `GET /api/v1/workloads/{id}/events` - Retrieve event history for a workload
- `GET /api/v1/events/{id}` - Retrieve a single event by ID
- `GET /api/v1/workloads/{id}/drift` - Retrieve drift detection reports

---

## Configuration

`kranix-core` is configured via YAML:
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
