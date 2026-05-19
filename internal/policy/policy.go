package policy

import (
	"fmt"
	"strings"

	"github.com/kranix-io/kranix-core/internal/cronsched"
	"github.com/kranix-io/kranix-core/internal/workloadtags"
	"github.com/kranix-io/kranix-core/pkg/types"
)

var allowedWorkloadPriorities = map[string]struct{}{
	string(types.WorkloadPriorityCritical): {},
	string(types.WorkloadPriorityHigh):     {},
	string(types.WorkloadPriorityNormal):   {},
	string(types.WorkloadPriorityLow):      {},
	"":                                     {},
}

// Engine enforces infrastructure policies on workloads.
type Engine struct {
	config Config
}

// Config defines policy engine configuration.
type Config struct {
	DefaultCPULimit           string
	DefaultMemoryLimit        string
	EnforceNamespaceIsolation bool
	RequireTeamTag            bool
	RequireEnvironmentTag     bool
	RequireCostCenterTag      bool
}

// New creates a new policy engine with the given configuration.
func New(config Config) *Engine {
	return &Engine{
		config: config,
	}
}

// Validate checks if a workload complies with policies.
func (e *Engine) Validate(workload *types.Workload) error {
	// Enforce namespace isolation
	if e.config.EnforceNamespaceIsolation && workload.Namespace == "" {
		return fmt.Errorf("namespace is required when namespace isolation is enforced")
	}

	// Validate resource limits
	if workload.Spec.Resources.CPULimit == "" {
		workload.Spec.Resources.CPULimit = e.config.DefaultCPULimit
	}
	if workload.Spec.Resources.MemoryLimit == "" {
		workload.Spec.Resources.MemoryLimit = e.config.DefaultMemoryLimit
	}

	if workload.Spec.Scheduling != nil && workload.Spec.Scheduling.WorkloadPriority != "" {
		p := strings.ToLower(strings.TrimSpace(workload.Spec.Scheduling.WorkloadPriority))
		if _, ok := allowedWorkloadPriorities[p]; !ok {
			return fmt.Errorf("unsupported workload_priority %q (use critical|high|normal|low)", workload.Spec.Scheduling.WorkloadPriority)
		}
	}

	if workload.Spec.CronSchedule != nil {
		if err := cronsched.Validate(workload.Spec.CronSchedule); err != nil {
			return fmt.Errorf("cron_schedule: %w", err)
		}
	}

	if cb := workload.Spec.CircuitBreaker; cb != nil && cb.Enabled {
		if cb.FailureThreshold < 0 {
			return fmt.Errorf("circuit_breaker.failure_threshold must be non-negative")
		}
		if cb.SuccessThreshold < 0 {
			return fmt.Errorf("circuit_breaker.success_threshold must be non-negative")
		}
		if cb.OpenDurationSeconds < 0 {
			return fmt.Errorf("circuit_breaker.open_duration_seconds must be non-negative")
		}
		if cb.HalfOpenMaxRequests < 0 {
			return fmt.Errorf("circuit_breaker.half_open_max_requests must be non-negative")
		}
	}
	if ws := workload.Spec.WarmStandby; ws != nil && ws.Enabled {
		if ws.Replicas < 0 {
			return fmt.Errorf("warm_standby.replicas must be non-negative")
		}
	}
	if sr := workload.Spec.SecretRotation; sr != nil && sr.Enabled {
		if len(sr.SecretRefs) == 0 {
			return fmt.Errorf("secret_rotation.secret_refs required when enabled")
		}
		for _, ref := range sr.SecretRefs {
			if strings.TrimSpace(ref.Name) == "" {
				return fmt.Errorf("secret_rotation.secret_refs.name is required")
			}
		}
	}

	workloadtags.SyncFromLabels(workload)
	if e.config.RequireTeamTag && workloadtags.Team(workload) == "" {
		return fmt.Errorf("tags.team (or label %s) is required", types.LabelKeyTeam)
	}
	if e.config.RequireEnvironmentTag {
		env := ""
		if workload.Tags != nil {
			env = workload.Tags.Environment
		}
		if env == "" && workload.Labels != nil {
			env = workload.Labels[types.LabelKeyEnvironment]
		}
		if strings.TrimSpace(env) == "" {
			return fmt.Errorf("tags.environment (or label %s) is required", types.LabelKeyEnvironment)
		}
	}
	if e.config.RequireCostCenterTag {
		cc := ""
		if workload.Tags != nil {
			cc = workload.Tags.CostCenter
		}
		if cc == "" && workload.Labels != nil {
			cc = workload.Labels[types.LabelKeyCostCenter]
		}
		if strings.TrimSpace(cc) == "" {
			return fmt.Errorf("tags.cost_center (or label %s) is required", types.LabelKeyCostCenter)
		}
	}

	return nil
}

// Enforce applies policies to a workload and returns the modified workload.
func (e *Engine) Enforce(workload *types.Workload) (*types.Workload, error) {
	// Apply default limits if not set
	if workload.Spec.Resources.CPULimit == "" {
		workload.Spec.Resources.CPULimit = e.config.DefaultCPULimit
	}
	if workload.Spec.Resources.MemoryLimit == "" {
		workload.Spec.Resources.MemoryLimit = e.config.DefaultMemoryLimit
	}

	workloadtags.Apply(workload)
	return workload, nil
}
