package scheduler

import (
	"context"
	"fmt"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// Scheduler handles workload placement and coordination.
type Scheduler struct {
	runtimeDrivers map[string]types.RuntimeDriver
}

// New creates a new scheduler.
func New() *Scheduler {
	return &Scheduler{
		runtimeDrivers: make(map[string]types.RuntimeDriver),
	}
}

// RegisterDriver adds a runtime driver to the scheduler.
func (s *Scheduler) RegisterDriver(driver types.RuntimeDriver) {
	s.runtimeDrivers[driver.Name()] = driver
}

// Schedule assigns a workload to an appropriate runtime backend.
func (s *Scheduler) Schedule(ctx context.Context, workload *types.Workload) error {
	driver, exists := s.runtimeDrivers[workload.Spec.Backend]
	if !exists {
		return fmt.Errorf("no runtime driver found for backend: %s", workload.Spec.Backend)
	}

	// Deploy the workload using the appropriate driver
	if err := driver.Deploy(ctx, workload); err != nil {
		return fmt.Errorf("failed to deploy workload: %w", err)
	}

	return nil
}

// GetDriver returns a runtime driver by name.
func (s *Scheduler) GetDriver(name string) (types.RuntimeDriver, bool) {
	driver, exists := s.runtimeDrivers[name]
	return driver, exists
}
