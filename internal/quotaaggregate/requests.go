package quotaaggregate

import (
	"strconv"
	"strings"

	"github.com/kranix-io/kranix-core/pkg/types"
	core "k8s.io/apimachinery/pkg/api/resource"
)

// SumRequests accumulates replica count and parses CPU/memory requests across workloads excluding skipID when non-empty.
func SumRequests(workloads []*types.Workload, skipID string) (cpu core.Quantity, mem core.Quantity, replicas int32, count int, err error) {
	cpu = core.MustParse("0")
	mem = core.MustParse("0")
	for _, wl := range workloads {
		if wl == nil || (skipID != "" && wl.ID == skipID) {
			continue
		}
		count++
		replicas += wl.Spec.Replicas
		q, err := ParseCPU(wl.Spec.Resources.CPURequest)
		if err != nil {
			return cpu, mem, replicas, count, err
		}
		cpu.Add(q)
		m, err := ParseMemory(wl.Spec.Resources.MemoryRequest)
		if err != nil {
			return cpu, mem, replicas, count, err
		}
		mem.Add(m)
	}
	return cpu, mem, replicas, count, nil
}

// ParseCPU parses a Kubernetes CPU request string ("100m", "2", …).
func ParseCPU(s string) (core.Quantity, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return core.MustParse("0"), nil
	}
	return core.ParseQuantity(s)
}

// ParseMemory parses Kubernetes memory quantities ("512Mi", "1Gi").
func ParseMemory(s string) (core.Quantity, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return core.MustParse("0"), nil
	}
	return core.ParseQuantity(s)
}

const teamLabelKey = "kranix.io/team"

// TeamLabel returns workload team label value if set.
func TeamLabel(w *types.Workload) string {
	if w == nil || w.Labels == nil {
		return ""
	}
	return strings.TrimSpace(w.Labels[teamLabelKey])
}

// TrimLower normalizes map keys and ids.
func TrimLower(s string) string {
	return strings.ToLower(strings.TrimSpace(s))
}

// Trim is strings.TrimSpace exported for quota packages.
func Trim(s string) string {
	return strings.TrimSpace(s)
}

// ParseIntQuota parses numeric limits like MaxWorkloads as string (backward compat helper).
func ParseIntQuota(s string) int32 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		return 0
	}
	return int32(n)
}
