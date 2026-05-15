package policy

import (
	"fmt"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// Engine enforces infrastructure policies on workloads.
type Engine struct {
	config Config
}

// Config defines policy engine configuration.
type Config struct {
	DefaultCPULimit           string
	DefaultMemoryLimit        string
	EnforceNamespaceIsolation bool
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

	return workload, nil
}
