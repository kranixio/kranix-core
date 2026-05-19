package resourcequota

import (
	"context"
	"fmt"
	"strings"

	"github.com/kranix-io/kranix-core/internal/quotaaggregate"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// SetNamespaceQuota creates or updates hard limits for a namespace.
func (e *Engine) SetNamespaceQuota(lim types.HardResourceQuota) error {
	ns := quotaaggregate.TrimLower(lim.Namespace)
	if ns == "" {
		return fmt.Errorf("namespace is required")
	}
	lim.Namespace = ns
	e.mu.Lock()
	defer e.mu.Unlock()
	e.namespace[ns] = lim
	return nil
}

// GetNamespaceQuota returns configured limits for a namespace.
func (e *Engine) GetNamespaceQuota(namespace string) (types.HardResourceQuota, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	lim, ok := e.namespace[quotaaggregate.TrimLower(namespace)]
	return lim, ok
}

// DeleteNamespaceQuota removes quota limits for a namespace.
func (e *Engine) DeleteNamespaceQuota(namespace string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	ns := quotaaggregate.TrimLower(namespace)
	if _, ok := e.namespace[ns]; !ok {
		return false
	}
	delete(e.namespace, ns)
	return true
}

// ListNamespaceQuotas returns all namespace-scoped quotas.
func (e *Engine) ListNamespaceQuotas() []types.HardResourceQuota {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]types.HardResourceQuota, 0, len(e.namespace))
	for _, lim := range e.namespace {
		out = append(out, lim)
	}
	return out
}

// NamespaceUsage aggregates current usage for workloads in a namespace.
func (e *Engine) NamespaceUsage(ctx context.Context, namespace string) (*types.ResourceQuotaUsage, error) {
	ns := strings.TrimSpace(namespace)
	if ns == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	lim, ok := e.GetNamespaceQuota(ns)
	if !ok {
		lim = types.HardResourceQuota{Namespace: ns}
	}
	all, err := e.storeReader.List(ctx, ns)
	if err != nil {
		return nil, err
	}
	cpu, mem, replicas, count, err := quotaaggregate.SumRequests(all, "")
	if err != nil {
		return nil, err
	}
	return &types.ResourceQuotaUsage{
		Namespace: ns,
		Limits:    lim,
		Used: types.QuotaUsageTotals{
			WorkloadCount:  count,
			ReplicaCount:   replicas,
			CPURequests:    cpu.String(),
			MemoryRequests: mem.String(),
		},
	}, nil
}
