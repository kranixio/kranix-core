package scheduler

import (
	"context"
	"fmt"
	"time"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// RequestRollingRestart triggers a rolling restart for a running workload.
func (s *Scheduler) RequestRollingRestart(ctx context.Context, workload *types.Workload) error {
	if workload == nil {
		return fmt.Errorf("workload is nil")
	}
	driver, exists := s.runtimeDrivers[workload.Spec.Backend]
	if !exists {
		return fmt.Errorf("no runtime driver found for backend: %s", workload.Spec.Backend)
	}

	workload.Status.RestartGeneration++
	now := time.Now().UTC()
	workload.Status.LastTransition = now
	workload.Status.Message = "rolling restart triggered"

	if err := driver.Restart(ctx, workload); err != nil {
		// Drivers that do not implement restart may return error; fall back to redeploy.
		if err := driver.Deploy(ctx, workload); err != nil {
			return fmt.Errorf("rolling restart failed: %w", err)
		}
	}
	return nil
}
