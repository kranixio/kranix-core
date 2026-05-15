package plugin

import (
	"context"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// Controller defines the interface for custom workload controllers.
type Controller interface {
	// Name returns the unique name of this controller.
	Name() string
	// Reconcile handles reconciliation for a workload.
	Reconcile(ctx context.Context, workload *types.Workload) error
	// ShouldHandle returns true if this controller should handle the given workload.
	ShouldHandle(workload *types.Workload) bool
}

// Registry manages registered controllers.
type Registry struct {
	controllers []Controller
}

// NewRegistry creates a new controller registry.
func NewRegistry() *Registry {
	return &Registry{
		controllers: make([]Controller, 0),
	}
}

// Register adds a controller to the registry.
func (r *Registry) Register(controller Controller) {
	r.controllers = append(r.controllers, controller)
}

// GetControllers returns all registered controllers.
func (r *Registry) GetControllers() []Controller {
	return r.controllers
}

// GetControllersForWorkload returns controllers that should handle the given workload.
func (r *Registry) GetControllersForWorkload(workload *types.Workload) []Controller {
	var result []Controller
	for _, c := range r.controllers {
		if c.ShouldHandle(workload) {
			result = append(result, c)
		}
	}
	return result
}
