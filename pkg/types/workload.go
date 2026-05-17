package types

import (
	"context"
	"time"
)

// Workload represents a managed unit with desired and observed state.
type Workload struct {
	// Metadata
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`

	// Spec - desired configuration
	Spec WorkloadSpec `json:"spec"`

	// Status - current observed state
	Status WorkloadStatus `json:"status"`

	// History - immutable log of state transitions
	History []WorkloadStateTransition `json:"history,omitempty"`

	// Tenant - multi-tenancy information
	Tenant *TenantInfo `json:"tenant,omitempty"`
}

// WorkloadSpec defines the desired configuration of a workload.
type WorkloadSpec struct {
	Image             string                `json:"image"`
	Replicas          int32                 `json:"replicas"`
	Env               map[string]string     `json:"env,omitempty"`
	Resources         ResourceRequirements  `json:"resources,omitempty"`
	Backend           string                `json:"backend"` // docker, kubernetes, etc.
	RolloutStrategy   RolloutStrategy       `json:"rollout_strategy,omitempty"`
	AutoScaling       *AutoScalingConfig    `json:"auto_scaling,omitempty"`
	Scheduling        *SchedulingConfig     `json:"scheduling,omitempty"`
	Dependencies      []Dependency          `json:"dependencies,omitempty"`
	FailurePrediction *FailurePrediction    `json:"failure_prediction,omitempty"`
	DriftDetection    *DriftDetectionConfig `json:"drift_detection,omitempty"`
	HealthGate        *HealthGateConfig     `json:"health_gate,omitempty"`
}

// ResourceRequirements defines compute resource requests and limits.
type ResourceRequirements struct {
	CPURequest    string `json:"cpu_request,omitempty"`
	CPULimit      string `json:"cpu_limit,omitempty"`
	MemoryRequest string `json:"memory_request,omitempty"`
	MemoryLimit   string `json:"memory_limit,omitempty"`
}

// RolloutStrategy defines how the workload should be deployed.
type RolloutStrategy struct {
	Type           string        `json:"type"` // rolling, recreate, bluegreen, canary, abtest
	MaxUnavailable int32         `json:"max_unavailable,omitempty"`
	MaxSurge       int32         `json:"max_surge,omitempty"`
	CanaryConfig   *CanaryConfig `json:"canary_config,omitempty"`
	ABTestConfig   *ABTestConfig `json:"ab_test_config,omitempty"`
}

// WorkloadStatus represents the current observed state of a workload.
type WorkloadStatus struct {
	Phase             WorkloadPhase `json:"phase"`
	Replicas          int32         `json:"replicas"`
	AvailableReplicas int32         `json:"available_replicas"`
	ReadyReplicas     int32         `json:"ready_replicas"`
	Message           string        `json:"message,omitempty"`
	LastTransition    time.Time     `json:"last_transition"`
}

// WorkloadPhase represents the lifecycle phase of a workload.
type WorkloadPhase string

const (
	WorkloadPhasePending   WorkloadPhase = "Pending"
	WorkloadPhaseRunning   WorkloadPhase = "Running"
	WorkloadPhaseDegraded  WorkloadPhase = "Degraded"
	WorkloadPhaseFailed    WorkloadPhase = "Failed"
	WorkloadPhaseSucceeded WorkloadPhase = "Succeeded"
)

// WorkloadStateTransition records a state change in the workload's history.
type WorkloadStateTransition struct {
	FromPhase WorkloadPhase `json:"from_phase"`
	ToPhase   WorkloadPhase `json:"to_phase"`
	Timestamp time.Time     `json:"timestamp"`
	Reason    string        `json:"reason"`
	Message   string        `json:"message,omitempty"`
}

// AutoScalingConfig defines auto-scaling behavior.
type AutoScalingConfig struct {
	Enabled                  bool           `json:"enabled"`
	MinReplicas              int32          `json:"min_replicas"`
	MaxReplicas              int32          `json:"max_replicas"`
	TargetCPUUtilization     int32          `json:"target_cpu_utilization,omitempty"`    // percentage
	TargetMemoryUtilization  int32          `json:"target_memory_utilization,omitempty"` // percentage
	CustomMetrics            []CustomMetric `json:"custom_metrics,omitempty"`
	ScaleDownCooldownSeconds int32          `json:"scale_down_cooldown_seconds,omitempty"`
	ScaleUpCooldownSeconds   int32          `json:"scale_up_cooldown_seconds,omitempty"`
}

// CustomMetric defines a custom metric for auto-scaling.
type CustomMetric struct {
	Name       string       `json:"name"`
	Type       string       `json:"type"` // pods, object
	MetricName string       `json:"metric_name"`
	Target     MetricTarget `json:"target"`
}

// MetricTarget defines the target value for a metric.
type MetricTarget struct {
	Type         string `json:"type"` // average, value
	AverageValue string `json:"average_value,omitempty"`
	Value        string `json:"value,omitempty"`
}

// SchedulingConfig defines scheduling preferences.
type SchedulingConfig struct {
	CostAware        bool              `json:"cost_aware,omitempty"`
	PreferredRegions []string          `json:"preferred_regions,omitempty"`
	PreferredZones   []string          `json:"preferred_zones,omitempty"`
	NodeSelectors    map[string]string `json:"node_selectors,omitempty"`
	Affinity         *AffinityConfig   `json:"affinity,omitempty"`
	Tolerations      []Toleration      `json:"tolerations,omitempty"`
	MaxCostPerHour   string            `json:"max_cost_per_hour,omitempty"`
}

// AffinityConfig defines pod affinity/anti-affinity rules.
type AffinityConfig struct {
	NodeAffinity    *NodeAffinity `json:"node_affinity,omitempty"`
	PodAffinity     *PodAffinity  `json:"pod_affinity,omitempty"`
	PodAntiAffinity *PodAffinity  `json:"pod_anti_affinity,omitempty"`
}

// NodeAffinity defines node affinity rules.
type NodeAffinity struct {
	RequiredDuringScheduling  []NodeSelectorTerm        `json:"required_during_scheduling,omitempty"`
	PreferredDuringScheduling []PreferredSchedulingTerm `json:"preferred_during_scheduling,omitempty"`
}

// NodeSelectorTerm defines a node selector term.
type NodeSelectorTerm struct {
	MatchExpressions []NodeSelectorRequirement `json:"match_expressions,omitempty"`
	MatchFields      []NodeSelectorRequirement `json:"match_fields,omitempty"`
}

// NodeSelectorRequirement defines a node selector requirement.
type NodeSelectorRequirement struct {
	Key      string   `json:"key"`
	Operator string   `json:"operator"` // In, NotIn, Exists, DoesNotExist, Gt, Lt
	Values   []string `json:"values,omitempty"`
}

// PreferredSchedulingTerm defines a preferred scheduling term.
type PreferredSchedulingTerm struct {
	Weight     int32            `json:"weight"`
	Preference NodeSelectorTerm `json:"preference"`
}

// PodAffinity defines pod affinity rules.
type PodAffinity struct {
	RequiredDuringScheduling  []PodAffinityTerm         `json:"required_during_scheduling,omitempty"`
	PreferredDuringScheduling []WeightedPodAffinityTerm `json:"preferred_during_scheduling,omitempty"`
}

// PodAffinityTerm defines a pod affinity term.
type PodAffinityTerm struct {
	LabelSelector map[string]string `json:"label_selector,omitempty"`
	Namespaces    []string          `json:"namespaces,omitempty"`
	TopologyKey   string            `json:"topology_key"`
}

// WeightedPodAffinityTerm defines a weighted pod affinity term.
type WeightedPodAffinityTerm struct {
	Weight          int32           `json:"weight"`
	PodAffinityTerm PodAffinityTerm `json:"pod_affinity_term"`
}

// Toleration defines a toleration for taints.
type Toleration struct {
	Key               string `json:"key,omitempty"`
	Operator          string `json:"operator,omitempty"` // Exists, Equal
	Value             string `json:"value,omitempty"`
	Effect            string `json:"effect,omitempty"` // NoSchedule, PreferNoSchedule, NoExecute
	TolerationSeconds *int64 `json:"toleration_seconds,omitempty"`
}

// CanaryConfig defines canary deployment configuration.
type CanaryConfig struct {
	Replicas         int32    `json:"replicas"`
	Percentage       int32    `json:"percentage,omitempty"`
	AnalysisDuration string   `json:"analysis_duration,omitempty"`
	SuccessThreshold int32    `json:"success_threshold,omitempty"`
	Metrics          []string `json:"metrics,omitempty"`
	AutoPromote      bool     `json:"auto_promote,omitempty"`
}

// ABTestConfig defines A/B testing configuration.
type ABTestConfig struct {
	VariantA         string   `json:"variant_a"`
	VariantB         string   `json:"variant_b"`
	TrafficSplit     int32    `json:"traffic_split"` // percentage for variant B
	AnalysisDuration string   `json:"analysis_duration,omitempty"`
	Metrics          []string `json:"metrics,omitempty"`
	AutoSelectWinner bool     `json:"autoSelectWinner,omitempty"`
}

// Dependency defines a dependency relationship between workloads.
type Dependency struct {
	WorkloadID string `json:"workloadId"`
	Type       string `json:"type"`                // depends_on, waits_for, requires
	Condition  string `json:"condition,omitempty"` // running, healthy, ready
	Timeout    string `json:"timeout,omitempty"`   // duration string like "5m"
}

// FailurePrediction defines ML-based failure prediction configuration.
type FailurePrediction struct {
	Enabled           bool     `json:"enabled"`
	ModelType         string   `json:"modelType"`                   // simple, ml, custom
	PredictionWindow  string   `json:"predictionWindow,omitempty"`  // time window for prediction
	Threshold         float64  `json:"threshold"`                   // probability threshold (0-1)
	Features          []string `json:"features,omitempty"`          // cpu_usage, memory_usage, request_rate, error_rate
	MitigationActions []string `json:"mitigationActions,omitempty"` // scale_up, restart, migrate
}

// TenantInfo defines multi-tenancy information for a workload.
type TenantInfo struct {
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
	Quota     *TenantQuota      `json:"quota,omitempty"`
	Isolation *TenantIsolation  `json:"isolation,omitempty"`
}

// TenantQuota defines resource quotas for a tenant.
type TenantQuota struct {
	MaxCPU           string `json:"maxCPU,omitempty"`
	MaxMemory        string `json:"maxMemory,omitempty"`
	MaxWorkloads     int32  `json:"maxWorkloads,omitempty"`
	MaxReplicas      int32  `json:"maxReplicas,omitempty"`
	MaxStorage       string `json:"maxStorage,omitempty"`
	MaxCustomMetrics int32  `json:"maxCustomMetrics,omitempty"`
}

// TenantIsolation defines isolation mechanisms for a tenant.
type TenantIsolation struct {
	NetworkPolicy     bool   `json:"networkPolicy"`
	ResourceQuota     bool   `json:"resourceQuota"`
	LimitRange        bool   `json:"limitRange"`
	PodSecurityPolicy bool   `json:"podSecurityPolicy"`
	StorageClass      string `json:"storageClass,omitempty"`
}

// DriftDetectionConfig defines drift detection configuration for a workload.
type DriftDetectionConfig struct {
	Enabled           bool               `json:"enabled"`
	CheckInterval     string             `json:"check_interval,omitempty"` // e.g., "30s", "1m"
	AlertOnDrift      bool               `json:"alert_on_drift"`
	AutoReconcile     bool               `json:"auto_reconcile"`
	MonitoredFields   []string           `json:"monitored_fields,omitempty"` // fields to monitor for drift
	Tolerance         *DriftTolerance    `json:"tolerance,omitempty"`
	NotificationHooks []NotificationHook `json:"notification_hooks,omitempty"`
}

// DriftTolerance defines acceptable variance before triggering drift alerts.
type DriftTolerance struct {
	ReplicaVariance        int32   `json:"replica_variance,omitempty"`      // allowed replica count difference
	ResourceVariancePct    float64 `json:"resource_variance_pct,omitempty"` // allowed resource percentage difference
	EnvVarDriftAllowed     bool    `json:"env_var_drift_allowed"`           // allow env var changes
	LabelDriftAllowed      bool    `json:"label_drift_allowed"`             // allow label changes
	AnnotationDriftAllowed bool    `json:"annotation_drift_allowed"`        // allow annotation changes
}

// NotificationHook defines a webhook or callback for drift alerts.
type NotificationHook struct {
	Type    string            `json:"type"` // webhook, slack, email, pagerduty
	URL     string            `json:"url,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Payload string            `json:"payload,omitempty"` // template for payload
	Secret  string            `json:"secret,omitempty"`  // for webhook authentication
}

// DriftReport represents a detected drift between desired and actual state.
type DriftReport struct {
	WorkloadID     string         `json:"workload_id"`
	WorkloadName   string         `json:"workload_name"`
	Namespace      string         `json:"namespace"`
	Timestamp      time.Time      `json:"timestamp"`
	DetectedAt     time.Time      `json:"detected_at"`
	DriftedFields  []DriftedField `json:"drifted_fields"`
	Severity       DriftSeverity  `json:"severity"`
	AutoReconciled bool           `json:"auto_reconciled"`
	Message        string         `json:"message"`
}

// DriftedField represents a specific field that has drifted.
type DriftedField struct {
	FieldPath string      `json:"field_path"`
	Desired   interface{} `json:"desired"`
	Actual    interface{} `json:"actual"`
	DiffType  string      `json:"diff_type"` // added, removed, modified
}

// DriftSeverity represents the severity level of a drift.
type DriftSeverity string

const (
	DriftSeverityLow      DriftSeverity = "low"
	DriftSeverityMedium   DriftSeverity = "medium"
	DriftSeverityHigh     DriftSeverity = "high"
	DriftSeverityCritical DriftSeverity = "critical"
)

// DomainEvent represents a single state change event in the event sourcing log.
type DomainEvent struct {
	ID            string                 `json:"id"`
	Aggregate     string                 `json:"aggregate"`      // workload ID
	AggregateType string                 `json:"aggregate_type"` // "workload"
	Type          string                 `json:"type"`           // event type
	Version       int64                  `json:"version"`        // event version
	Timestamp     time.Time              `json:"timestamp"`
	Data          map[string]interface{} `json:"data"`
	Metadata      map[string]string      `json:"metadata,omitempty"`
}

// EventStore defines the interface for event sourcing storage.
type EventStore interface {
	// Append adds a new event to the store
	Append(ctx context.Context, event *DomainEvent) error
	// GetEvents retrieves events for an aggregate
	GetEvents(ctx context.Context, aggregateID string, fromVersion int64, limit int) ([]*DomainEvent, error)
	// GetEvent retrieves a single event by ID
	GetEvent(ctx context.Context, eventID string) (*DomainEvent, error)
	// Replay reconstructs state by replaying events
	Replay(ctx context.Context, aggregateID string) (interface{}, error)
	// Subscribe to events for an aggregate
	Subscribe(ctx context.Context, aggregateID string) (<-chan *DomainEvent, error)
}

// HealthGateConfig defines health gate configuration for blocking rollouts.
type HealthGateConfig struct {
	Enabled     bool          `json:"enabled"`
	Checks      []HealthCheck `json:"checks"`
	Timeout     string        `json:"timeout,omitempty"` // e.g., "5m"
	FailureMode string        `json:"failure_mode"`      // block, warn, ignore
}

// HealthCheck defines a single health check.
type HealthCheck struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"` // http, tcp, command, prometheus, custom
	Config    map[string]string `json:"config"`
	Interval  string            `json:"interval,omitempty"`  // e.g., "30s"
	Threshold int32             `json:"threshold,omitempty"` // number of consecutive successes/failures
}

// HealthCheckResult represents the result of a health check.
type HealthCheckResult struct {
	CheckName   string            `json:"check_name"`
	Status      string            `json:"status"` // passing, failing, unknown
	Message     string            `json:"message,omitempty"`
	LastChecked time.Time         `json:"last_checked"`
	Duration    time.Duration     `json:"duration"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// HealthGateStatus represents the overall status of health gates for a workload.
type HealthGateStatus struct {
	WorkloadID    string              `json:"workload_id"`
	OverallStatus string              `json:"overall_status"` // passing, failing, blocked
	Results       []HealthCheckResult `json:"results"`
	LastEvaluated time.Time           `json:"last_evaluated"`
	Blocked       bool                `json:"blocked"`
	BlockReason   string              `json:"block_reason,omitempty"`
}
