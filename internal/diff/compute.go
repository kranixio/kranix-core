package diff

import (
	"fmt"
	"strings"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// ComputeSpecVsLive compares desired workload spec fields to observed status (live state).
func ComputeSpecVsLive(w *types.Workload, proposed *types.WorkloadSpec) types.WorkloadDiffResult {
	if w == nil {
		return types.WorkloadDiffResult{}
	}
	desired := w.Spec
	if proposed != nil {
		desired = *proposed
	}

	desiredMap := specSnapshot(desired)
	liveMap := liveSnapshot(w)

	var changes []types.DiffChange
	compareField(&changes, "image", desiredMap["image"], liveMap["image"])
	compareField(&changes, "replicas", desiredMap["replicas"], liveMap["replicas"])
	compareField(&changes, "ready_replicas", desiredMap["replicas"], liveMap["ready_replicas"])
	compareField(&changes, "phase", desiredMap["expected_phase"], liveMap["phase"])
	compareField(&changes, "backend", desiredMap["backend"], liveMap["backend"])
	compareField(&changes, "cpu_request", desiredMap["cpu_request"], liveMap["cpu_request"])
	compareField(&changes, "memory_request", desiredMap["memory_request"], liveMap["memory_request"])
	compareField(&changes, "cpu_limit", desiredMap["cpu_limit"], liveMap["cpu_limit"])
	compareField(&changes, "memory_limit", desiredMap["memory_limit"], liveMap["memory_limit"])

	summary := types.DiffSummary{}
	for _, c := range changes {
		summary.TotalChanges++
		switch c.ChangeType {
		case "added":
			summary.Added++
		case "removed":
			summary.Removed++
		default:
			summary.Modified++
		}
	}
	summary.HasDrift = summary.TotalChanges > 0

	return types.WorkloadDiffResult{
		WorkloadID:   w.ID,
		WorkloadName: w.Name,
		Namespace:    w.Namespace,
		Desired:      desiredMap,
		Live:         liveMap,
		Changes:      changes,
		Summary:      summary,
	}
}

func specSnapshot(s types.WorkloadSpec) map[string]interface{} {
	expected := "Running"
	if s.Replicas == 0 {
		expected = "Pending"
	}
	return map[string]interface{}{
		"image":          s.Image,
		"replicas":       s.Replicas,
		"backend":        s.Backend,
		"cpu_request":    s.Resources.CPURequest,
		"cpu_limit":      s.Resources.CPULimit,
		"memory_request": s.Resources.MemoryRequest,
		"memory_limit":   s.Resources.MemoryLimit,
		"expected_phase": expected,
	}
}

func liveSnapshot(w *types.Workload) map[string]interface{} {
	st := w.Status
	return map[string]interface{}{
		"image":          w.Spec.Image,
		"replicas":       st.Replicas,
		"ready_replicas": st.ReadyReplicas,
		"phase":          string(st.Phase),
		"backend":        w.Spec.Backend,
		"cpu_request":    w.Spec.Resources.CPURequest,
		"cpu_limit":      w.Spec.Resources.CPULimit,
		"memory_request": w.Spec.Resources.MemoryRequest,
		"memory_limit":   w.Spec.Resources.MemoryLimit,
		"message":        st.Message,
	}
}

func compareField(changes *[]types.DiffChange, field string, desired, live interface{}) {
	ds := fmt.Sprint(desired)
	ls := fmt.Sprint(live)
	if strings.TrimSpace(ds) == strings.TrimSpace(ls) {
		return
	}
	ct := "modified"
	if ds == "" || ds == "<nil>" {
		ct = "removed"
	} else if ls == "" || ls == "<nil>" {
		ct = "added"
	}
	*changes = append(*changes, types.DiffChange{
		Field:      field,
		OldValue:   live,
		NewValue:   desired,
		ChangeType: ct,
	})
}
