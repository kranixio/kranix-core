package multitenancy

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Engine manages multi-tenancy with hard isolation between organizations.
type Engine struct {
	store          state.Store
	tenantRegistry *TenantRegistry
	mu             sync.RWMutex
}

// New creates a new multi-tenancy engine.
func New(store state.Store) *Engine {
	return &Engine{
		store:          store,
		tenantRegistry: NewTenantRegistry(),
	}
}

// TenantRegistry manages tenant information and quotas.
type TenantRegistry struct {
	tenants map[string]*TenantInfo
	mu      sync.RWMutex
}

// NewTenantRegistry creates a new tenant registry.
func NewTenantRegistry() *TenantRegistry {
	return &TenantRegistry{
		tenants: make(map[string]*TenantInfo),
	}
}

// TenantInfo represents tenant information in the registry.
type TenantInfo struct {
	ID           string
	Name         string
	Namespace    string
	Labels       map[string]string
	Quota        *types.TenantQuota
	Isolation    *types.TenantIsolation
	ResourceUsage *ResourceUsage
}

// ResourceUsage tracks resource usage for a tenant.
type ResourceUsage struct {
	CPUUsed         string
	MemoryUsed      string
	WorkloadCount   int32
	ReplicaCount    int32
	StorageUsed     string
	CustomMetricCount int32
}

// RegisterTenant registers a new tenant.
func (r *TenantRegistry) RegisterTenant(tenant *types.TenantInfo) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tenants[tenant.ID]; exists {
		return fmt.Errorf("tenant %s already exists", tenant.ID)
	}

	r.tenants[tenant.ID] = &TenantInfo{
		ID:        tenant.ID,
		Name:      tenant.Name,
		Namespace: tenant.Namespace,
		Labels:    tenant.Labels,
		Quota:     tenant.Quota,
		Isolation: tenant.Isolation,
		ResourceUsage: &ResourceUsage{
			CPUUsed:          "0",
			MemoryUsed:       "0",
			WorkloadCount:    0,
			ReplicaCount:     0,
			StorageUsed:      "0",
			CustomMetricCount: 0,
		},
	}

	log.Printf("Tenant registered: %s (namespace: %s)", tenant.ID, tenant.Namespace)
	return nil
}

// GetTenant retrieves tenant information.
func (r *TenantRegistry) GetTenant(tenantID string) (*TenantInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tenant, exists := r.tenants[tenantID]
	if !exists {
		return nil, fmt.Errorf("tenant %s not found", tenantID)
	}
	return tenant, nil
}

// EnforceQuota enforces resource quotas for a tenant.
func (e *Engine) EnforceQuota(ctx context.Context, workload *types.Workload) error {
	if workload.Tenant == nil {
		return nil // no tenant, no quota enforcement
	}

	tenant, err := e.tenantRegistry.GetTenant(workload.Tenant.ID)
	if err != nil {
		return fmt.Errorf("failed to get tenant info: %w", err)
	}

	if tenant.Quota == nil {
		return nil // no quota defined
	}

	// Check workload count
	if tenant.Quota.MaxWorkloads > 0 {
		if tenant.ResourceUsage.WorkloadCount >= tenant.Quota.MaxWorkloads {
			return fmt.Errorf("tenant %s has reached maximum workload limit (%d)", 
				workload.Tenant.ID, tenant.Quota.MaxWorkloads)
		}
	}

	// Check replica count
	if tenant.Quota.MaxReplicas > 0 {
		totalReplicas := tenant.ResourceUsage.ReplicaCount + workload.Spec.Replicas
		if totalReplicas > tenant.Quota.MaxReplicas {
			return fmt.Errorf("tenant %s would exceed replica limit (%d)", 
				workload.Tenant.ID, tenant.Quota.MaxReplicas)
		}
	}

	// Check custom metric count (for auto-scaling)
	if workload.Spec.AutoScaling != nil && workload.Spec.AutoScaling.Enabled {
		if tenant.Quota.MaxCustomMetrics > 0 {
			metricCount := int32(len(workload.Spec.AutoScaling.CustomMetrics))
			if tenant.ResourceUsage.CustomMetricCount+metricCount > tenant.Quota.MaxCustomMetrics {
				return fmt.Errorf("tenant %s would exceed custom metric limit (%d)", 
					workload.Tenant.ID, tenant.Quota.MaxCustomMetrics)
			}
		}
	}

	log.Printf("Quota check passed for workload %s in tenant %s", workload.ID, workload.Tenant.ID)
	return nil
}

// UpdateUsage updates resource usage for a tenant.
func (e *Engine) UpdateUsage(ctx context.Context, workload *types.Workload, delta int32) error {
	if workload.Tenant == nil {
		return nil
	}

	e.tenantRegistry.mu.Lock()
	defer e.tenantRegistry.mu.Unlock()

	tenant, exists := e.tenantRegistry.tenants[workload.Tenant.ID]
	if !exists {
		return fmt.Errorf("tenant %s not found", workload.Tenant.ID)
	}

	tenant.ResourceUsage.WorkloadCount += delta
	tenant.ResourceUsage.ReplicaCount += workload.Spec.Replicas * delta

	log.Printf("Updated usage for tenant %s: workloads=%d, replicas=%d", 
		workload.Tenant.ID, tenant.ResourceUsage.WorkloadCount, tenant.ResourceUsage.ReplicaCount)

	return nil
}

// EnforceIsolation enforces isolation policies for a tenant.
func (e *Engine) EnforceIsolation(ctx context.Context, workload *types.Workload) error {
	if workload.Tenant == nil || workload.Tenant.Isolation == nil {
		return nil
	}

	isolation := workload.Tenant.Isolation

	// Network Policy enforcement
	if isolation.NetworkPolicy {
		// In production, this would create network policies to restrict traffic
		log.Printf("Applying network policy for workload %s in tenant %s", 
			workload.ID, workload.Tenant.ID)
	}

	// Resource Quota enforcement
	if isolation.ResourceQuota {
		// In production, this would create Kubernetes ResourceQuota objects
		log.Printf("Applying resource quota for workload %s in tenant %s", 
			workload.ID, workload.Tenant.ID)
	}

	// Limit Range enforcement
	if isolation.LimitRange {
		// In production, this would create Kubernetes LimitRange objects
		log.Printf("Applying limit range for workload %s in tenant %s", 
			workload.ID, workload.Tenant.ID)
	}

	// Pod Security Policy enforcement
	if isolation.PodSecurityPolicy {
		// In production, this would apply pod security policies
		log.Printf("Applying pod security policy for workload %s in tenant %s", 
			workload.ID, workload.Tenant.ID)
	}

	// Storage Class enforcement
	if isolation.StorageClass != "" {
		// In production, this would set the storage class for persistent volumes
		log.Printf("Applying storage class %s for workload %s in tenant %s", 
			isolation.StorageClass, workload.ID, workload.Tenant.ID)
	}

	return nil
}

// ValidateWorkload validates that a workload belongs to a valid tenant and respects constraints.
func (e *Engine) ValidateWorkload(ctx context.Context, workload *types.Workload) error {
	if workload.Tenant == nil {
		return nil // workload without tenant is valid
	}

	// Check if tenant exists
	_, err := e.tenantRegistry.GetTenant(workload.Tenant.ID)
	if err != nil {
		return fmt.Errorf("tenant validation failed: %w", err)
	}

	// Enforce quota
	if err := e.EnforceQuota(ctx, workload); err != nil {
		return err
	}

	// Enforce isolation
	if err := e.EnforceIsolation(ctx, workload); err != nil {
		return err
	}

	return nil
}

// GetTenantWorkloads retrieves all workloads for a tenant.
func (e *Engine) GetTenantWorkloads(ctx context.Context, tenantID string) ([]*types.Workload, error) {
	workloads, err := e.store.List(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("failed to list workloads: %w", err)
	}

	tenantWorkloads := make([]*types.Workload, 0)
	for _, w := range workloads {
		if w.Tenant != nil && w.Tenant.ID == tenantID {
			tenantWorkloads = append(tenantWorkloads, w)
		}
	}

	return tenantWorkloads, nil
}

// IsolateTenant applies hard isolation between tenants.
func (e *Engine) IsolateTenant(ctx context.Context, tenantID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	tenant, err := e.tenantRegistry.GetTenant(tenantID)
	if err != nil {
		return err
	}

	if tenant.Isolation == nil {
		return fmt.Errorf("no isolation config for tenant %s", tenantID)
	}

	// In production, this would:
	// 1. Create dedicated network policies
	// 2. Apply resource quotas
	// 3. Set up limit ranges
	// 4. Configure pod security policies
	// 5. Set up dedicated storage classes

	log.Printf("Applying hard isolation for tenant %s in namespace %s", tenantID, tenant.Namespace)

	return nil
}

// GetTenantUsage returns resource usage for a tenant.
func (e *Engine) GetTenantUsage(tenantID string) (*ResourceUsage, error) {
	tenant, err := e.tenantRegistry.GetTenant(tenantID)
	if err != nil {
		return nil, err
	}
	return tenant.ResourceUsage, nil
}
