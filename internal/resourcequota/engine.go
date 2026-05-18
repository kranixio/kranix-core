package resourcequota

import (
	"context"
	"fmt"
	"sync"

	"github.com/kranix-io/kranix-core/internal/quotaaggregate"
	"github.com/kranix-io/kranix-core/pkg/types"
	core "k8s.io/apimachinery/pkg/api/resource"
)

// Engine enforces configurable hard quotas per Kubernetes namespace or per team label.
type Engine struct {
	mu          sync.RWMutex
	namespace   map[string]types.HardResourceQuota // key namespace
	team        map[string]types.HardResourceQuota // team id
	storeReader StoreReader
}

// StoreReader is the subset of store needed for aggregates.
type StoreReader interface {
	List(ctx context.Context, namespace string) ([]*types.Workload, error)
}

// New constructs a quota engine backed by workloads in the provided store reader.
func New(store StoreReader, limits []types.HardResourceQuota) *Engine {
	e := &Engine{
		namespace:   make(map[string]types.HardResourceQuota),
		team:        make(map[string]types.HardResourceQuota),
		storeReader: store,
	}
	for _, lim := range limits {
		ns := quotaaggregate.TrimLower(lim.Namespace)
		tm := quotaaggregate.TrimLower(lim.TeamID)
		entry := lim
		if ns != "" {
			e.namespace[ns] = entry
			continue
		}
		if tm != "" {
			e.team[tm] = entry
			continue
		}
	}
	return e
}

// Enforce returns an error when admitting this workload would exceed hard quotas.
func (e *Engine) Enforce(ctx context.Context, wl *types.Workload) error {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if wl == nil {
		return nil
	}

	var limPtr *types.HardResourceQuota
	if nsLim, ok := e.namespace[quotaaggregate.TrimLower(wl.Namespace)]; ok && wl.Namespace != "" {
		l := nsLim
		limPtr = &l
	} else if id := quotaaggregate.TeamLabel(wl); id != "" {
		if tLim, ok := e.team[quotaaggregate.TrimLower(id)]; ok {
			l := tLim
			limPtr = &l
		}
	} else if wl.Tenant != nil && wl.Tenant.ID != "" {
		if tLim, ok := e.team[quotaaggregate.TrimLower(wl.Tenant.ID)]; ok {
			l := tLim
			limPtr = &l
		}
	}
	if limPtr == nil {
		return nil
	}

	// Aggregate peer workloads sharing the quota key.
	namespaceFilter := wl.Namespace
	if quotaaggregate.TrimLower(limPtr.TeamID) != "" {
		namespaceFilter = ""
	}
	all, err := e.storeReader.List(ctx, namespaceFilter)
	if err != nil {
		return fmt.Errorf("quota list workloads: %w", err)
	}

	var peers []*types.Workload
	for _, wi := range all {
		if ! matchesQuotaKey(wl, wi, limPtr) {
			continue
		}
		peers = append(peers, wi)
	}

	cpuPeers, memPeers, repPeers, wlCountPeers, err := quotaaggregate.SumRequests(peers, wl.ID)
	if err != nil {
		return fmt.Errorf("quota aggregation: %w", err)
	}

	cpuNew, err := quotaaggregate.ParseCPU(wl.Spec.Resources.CPURequest)
	if err != nil {
		return err
	}
	mnew, err := quotaaggregate.ParseMemory(wl.Spec.Resources.MemoryRequest)
	if err != nil {
		return err
	}
	cpuNewTotal := cpuPeers.DeepCopy()
	cpuNewTotal.Add(cpuNew)

	memTotal := memPeers.DeepCopy()
	memTotal.Add(mnew)

	wlCountAfter := wlCountPeers + 1
	replicasAfter := repPeers + wl.Spec.Replicas

	if limPtr.MaxWorkloads > 0 && int32(wlCountAfter) > limPtr.MaxWorkloads {
		return fmt.Errorf("namespace/team quota: max workloads %d exceeded (would be %d)", limPtr.MaxWorkloads, wlCountAfter)
	}
	if limPtr.MaxReplicasTotal > 0 && replicasAfter > limPtr.MaxReplicasTotal {
		return fmt.Errorf("namespace/team quota: max replicas %d exceeded (would be %d)", limPtr.MaxReplicasTotal, replicasAfter)
	}
	if cpuLim := limPtr.MaxCPURequests; quotaaggregate.Trim(cpuLim) != "" {
		capQ, err := core.ParseQuantity(cpuLim)
		if err != nil {
			return fmt.Errorf("invalid max_cpu_requests in quota policy: %w", err)
		}
		if cpuNewTotal.Cmp(capQ) > 0 {
			return fmt.Errorf("namespace/team quota: CPU requests %s would exceed limit %s", cpuNewTotal.String(), capQ.String())
		}
	}
	if memLim := limPtr.MaxMemoryRequests; quotaaggregate.Trim(memLim) != "" {
		capQ, err := core.ParseQuantity(memLim)
		if err != nil {
			return fmt.Errorf("invalid max_memory_requests in quota policy: %w", err)
		}
		if memTotal.Cmp(capQ) > 0 {
			return fmt.Errorf("namespace/team quota: memory requests %s would exceed limit %s", memTotal.String(), capQ.String())
		}
	}
	return nil
}

func matchesQuotaKey(ref, candidate *types.Workload, lim *types.HardResourceQuota) bool {
	if qt := quotaaggregate.TrimLower(lim.Namespace); qt != "" {
		return quotaaggregate.TrimLower(candidate.Namespace) == qt
	}
	if qt := quotaaggregate.TrimLower(lim.TeamID); qt != "" {
		if quotaaggregate.TrimLower(quotaaggregate.TeamLabel(candidate)) == qt {
			return true
		}
		return candidate.Tenant != nil && quotaaggregate.TrimLower(candidate.Tenant.ID) == qt
	}
	return quotaaggregate.TrimLower(candidate.Namespace) == quotaaggregate.TrimLower(ref.Namespace)
}
