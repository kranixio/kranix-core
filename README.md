# kranix-core

> The orchestration engine — state, scheduling, and reconciliation for the Kranix platform.

`kranix-core` is the brain of the Kranix ecosystem. It owns all business logic: reconciliation loops, workload scheduling, state management, event routing, and policy enforcement. Every other Kranix repo either sends work *into* core or gets driven *by* core. Nothing touches infrastructure directly except through it.

---

## What it does

- Maintains desired vs actual state for all managed workloads
- Runs continuous reconciliation loops (Git intent → runtime state)
- Schedules and coordinates deployments across backends
- Routes events between the API layer and runtime drivers
- Enforces infra policies (resource limits, namespace isolation, rollout rules, **workload priority** tiers, optional **cron** gates, **aggregate resource quotas** per namespace/team)
- Carries **cross-namespace traffic** and **spot / preemption** hints on the workload model for Kubernetes runtimes
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

Optional **`spec.cron_schedule`** enables **cron-style scheduling** inside the reconciler (standard five-field cron, optional IANA `time_zone`, optional `concurrency_policy` aligned with Kubernetes: allow / forbid / replace). When a schedule is active and due, core may emit **`WorkloadCronTriggered`** before **`WorkloadScheduled`**. With **`concurrency_policy: forbid`**, the controller does not trigger another schedule tick while the workload phase is **`Running`** or **`Degraded`**.

Core can enforce **hard aggregate quotas** over workloads in scope: **`resource_quota.hard_limits`** caps total CPU/memory *requests*, workload count, and replica count **per Kubernetes namespace** or **per team** (label `kranix.io/team`, or tenant id when keyed by team). The multitenancy engine also enforces tenant **`quota.maxCPU` / `maxMemory`** against summed requests across workloads in that tenant when those fields are set.

**Scheduling (priority & preemption):** **`spec.scheduling.workload_priority`** must be one of **`critical`**, **`high`**, **`normal`**, **`low`** (validated in policy). The scheduler uses **`WorkloadSchedulingRank`** so higher tiers reconcile first. **`preemption_enabled`** and **`priority_class_name`** are carried on the workload for **`kranix-runtime`** to map to Kubernetes **PriorityClasses** (clusters must install classes such as `kranix-critical` / `kranix-critical-np` as used by the driver).

**Spot / preemptible:** **`spec.scheduling.spot`** (**`enabled`**, **`reschedule_on_node_termination`**) is passed through for the Kubernetes backend to merge spot **tolerations** and tighter eviction behavior.

**Multi-arch & drain-aware scheduling:** **`spec.scheduling.architecture`** (`amd64` | `arm64`) and **`avoid_draining_nodes`** filter the node registry before cost-aware placement. Runtime applies **`kubernetes.io/arch`** node selection on deploy.

**Node health & drain API:** **`GET /api/v1/nodes/health`** returns per-node scores (0–100). **`POST /api/v1/nodes/{name}/drain`** delegates to runtime **`NodeOperations`** when wired via **`Server.SetNodeOperations`**.

**Checkpoint, restore & runtime plugins:** **`POST /api/v1/workloads/{id}/checkpoint`**, **`POST /api/v1/workloads/{id}/restore`**, and **`GET /api/v1/workloads/{id}/checkpoints`** delegate to runtime **`RuntimeExtendedOperations`** when wired via **`Server.SetRuntimeOperations`**. **`GET /api/v1/runtime/plugins`** lists backend plugins when **`Server.SetRuntimePluginLister`** is configured.

**Volumes & bandwidth on deploy:** **`spec.volumes`** and **`spec.networkBandwidth`** on workload specs are passed through to **`kranix-runtime`** during reconcile (PVC creation, pod annotations, Docker volume binds).

**Cross-namespace traffic:** **`spec.cross_namespace_traffic`** records which peer namespaces may exchange traffic when the runtime applies **NetworkPolicy** (ingress/egress allow lists, DNS, optional internet egress).

**Rollback history:** With **`rollback_history.enabled`**, a **`VersionedStore`** retains the last **`max_versions`** snapshots of each workload’s spec (and tags) in **`rollback_versions`** (newest first). Use **`rollouthistory.Revert`** / **`rollouthistory.ListRevisions`** for instant revert; emits **`WorkloadRolledBack`** on revert.

**Workload tags:** Structured **`tags`** (**`team`**, **`environment`**, **`cost_center`**, optional **`custom`**) are mirrored to labels **`kranix.io/team`**, **`kranix.io/environment`**, **`kranix.io/cost-center`** for filtering, billing exports, and team quotas. Optional policy flags under **`workload_tags`** require tags at admission.

**Circuit breaker:** **`spec.circuit_breaker`** (or global **`circuit_breaker.enabled`**) tracks per-workload state in **`status.circuit_breaker`** (`closed` → `open` → `half-open`). While **open**, the reconciler skips scheduling/routing and emits **`WorkloadCircuitOpen`**; recovery emits **`WorkloadCircuitClosed`**. Dependency resolution treats peers with an open circuit as unsatisfied.

**Warm standby:** **`spec.warm_standby`** provisions a linked cold workload (`{id}-standby`, **0 replicas**) labeled **`kranix.io/role=standby`**. **`auto_promote`** (or **`warm_standby.default_auto_promote`**) scales the standby when the primary circuit opens, emitting **`WorkloadStandbyPromoted`**. Configure via **`warm_standby`** in [`config/local.yaml`](./config/local.yaml).

**HTTP API** (when `http.enabled`): workload CRUD, **bulk** ops, **diff**, **cursor-paginated filtered list** (`limit`, `cursor`), **namespace quotas**, audit history, and secret rotation notify.

**Secret rotation awareness:** Workloads declare **`spec.secret_rotation.secret_refs`**. When an external controller (e.g. **`KranixSecret`**) reports a new version via **`POST /api/v1/secrets/rotated`**, core marks dependents **`pending_restart`** and the reconciler issues a **rolling restart** (`WorkloadRestartRequested`). Enable with **`secret_rotation.enabled`** and the core HTTP API (`http.addr`, default **`:8081`**).

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
│   ├── reconciler/       # Main reconciliation loop (policy, quota, cron gates)
│   ├── cronsched/        # Cron schedule evaluation for workloads
│   ├── resourcequota/    # Hard limits per namespace or team label
│   ├── quotaaggregate/   # CPU/memory request aggregates for quotas
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

# Optional: hard aggregate limits per namespace OR per team (label kranix.io/team / tenant id).
resource_quota:
  hard_limits:
    # - namespace: team-a-ns
    #   max_cpu_requests: "8"
    #   max_memory_requests: "16Gi"
    #   max_workloads: 50
    #   max_replicas_total: 200
    # - team_id: platform
    #   max_workloads: 100
```

The reconciler loads **policy**, **cron evaluation**, and (when `hard_limits` is non-empty) the **quota** engine from [`cmd/core/main.go`](./cmd/core/main.go).

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
- `WorkloadCronTriggered` - Cron schedule fired before a scheduled rollout tick

API Endpoints (via kranix-api):
- `GET /api/v1/workloads/{id}/events` - Retrieve event history for a workload
- `GET /api/v1/events/{id}` - Retrieve a single event by ID
- `GET /api/v1/workloads/{id}/drift` - Retrieve drift detection reports

---

## New Features (v4.0)

### Persistent State Backends

Production-grade persistent storage options for workload state:

```yaml
state:
  backend: memory          # memory | postgres | etcd
  postgres_dsn: ""         # e.g., "postgres://user:pass@localhost:5432/kranix"
  etcd_endpoints: []       # e.g., ["localhost:2379"]
```

**Memory Backend (Default):**
- In-memory storage for development and testing
- Fast but data is lost on restart
- Suitable for single-node deployments

**Postgres Backend:**
- Persistent relational database storage
- ACID transactions for data consistency
- Supports complex queries and joins
- Automatic backups via standard Postgres tools
- Recommended for production deployments

**etcd Backend:**
- Distributed key-value store
- Strong consistency guarantees
- Built-in watch capabilities for real-time updates
- Automatic leader election and failover
- Ideal for distributed systems and Kubernetes environments

### Health Gate Engine

Block rollouts until health checks pass to ensure safe deployments:

```yaml
health_gate:
  enabled: true
  default_timeout: 5m
  check_interval: 30s
```

Workload-level health gate configuration:

```yaml
spec:
  health_gate:
    enabled: true
    timeout: "5m"
    failure_mode: "block"  # block | warn | ignore
    checks:
      - name: "api-health"
        type: "http"
        config:
          url: "http://api-service:8080/health"
          method: "GET"
          expected_status: "200"
      - name: "database-ready"
        type: "tcp"
        config:
          host: "db-service"
          port: "5432"
      - name: "prometheus-metrics"
        type: "prometheus"
        config:
          query: "up{job=\"my-app\"}"
          prometheus_url: "http://prometheus:9090"
```

The health gate engine:
- Evaluates health checks before allowing rollouts to proceed
- Supports HTTP, TCP, command, and Prometheus query checks
- Configurable failure modes (block, warn, ignore)
- Timeout handling for long-running checks
- Individual check result tracking with status and metadata
- Real-time health status updates via event bus

Health check types supported:
- **HTTP** - Check HTTP endpoints with custom status codes
- **TCP** - Verify TCP connectivity to services
- **Command** - Execute custom health check commands
- **Prometheus** - Query Prometheus metrics for health assessment

API Endpoints (via kranix-api):
- `GET /api/v1/workloads/{id}/health` - Retrieve health gate status
- `POST /api/v1/workloads/{id}/health/evaluate` - Manually trigger health gate evaluation

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
