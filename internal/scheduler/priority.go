package scheduler

import (
	"strings"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// WorkloadSchedulingRank returns a numeric rank for ordering reconciliation and placement;
// higher values are processed first when the engine has backpressure.
func WorkloadSchedulingRank(w *types.Workload) int {
	if w == nil {
		return 0
	}
	p := ""
	if w.Spec.Scheduling != nil {
		p = w.Spec.Scheduling.WorkloadPriority
		if w.Spec.Scheduling.PriorityClassName != "" {
			// Explicit classes usually indicate production-critical paths; bias above "low".
			return maxInt(WorkloadPriorityRank(p), 750)
		}
	}
	return WorkloadPriorityRank(p)
}

// WorkloadPriorityRank maps declared priorities to coarse numeric ranks (internal use only).
func WorkloadPriorityRank(priority string) int {
	switch strings.ToLower(strings.TrimSpace(priority)) {
	case string(types.WorkloadPriorityCritical), "crit":
		return 1000
	case string(types.WorkloadPriorityHigh):
		return 750
	case string(types.WorkloadPriorityLow):
		return 250
	case string(types.WorkloadPriorityNormal), "":
		fallthrough
	default:
		return 500
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
