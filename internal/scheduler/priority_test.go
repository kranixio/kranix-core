package scheduler

import (
	"testing"

	"github.com/kranix-io/kranix-core/pkg/types"
)

func TestWorkloadSchedulingRank(t *testing.T) {
	w := &types.Workload{
		Spec: types.WorkloadSpec{
			Scheduling: &types.SchedulingConfig{WorkloadPriority: string(types.WorkloadPriorityHigh)},
		},
	}
	if got, want := WorkloadSchedulingRank(w), 750; got != want {
		t.Fatalf("got %d want %d", got, want)
	}
}
