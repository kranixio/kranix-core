package types

// HardResourceQuota defines hard limits aggregated per namespace or team (label/tenant id).
// Used by core for admission before scheduling; runtimes may mirror as Kubernetes ResourceQuota.
type HardResourceQuota struct {
	Namespace string `json:"namespace,omitempty" yaml:"namespace,omitempty"` // Workload.Namespace when set
	// TeamID matches Workload.Labels["kranix.io/team"] or Tenant.ID when no namespace key.
	TeamID string `json:"team_id,omitempty" yaml:"team_id,omitempty"`
	// Resource requests (Kubernetes-style quantities, e.g. "10", "500m", "8Gi").
	MaxCPURequests    string `json:"max_cpu_requests,omitempty" yaml:"max_cpu_requests,omitempty"`
	MaxMemoryRequests string `json:"max_memory_requests,omitempty" yaml:"max_memory_requests,omitempty"`
	MaxWorkloads      int32  `json:"max_workloads,omitempty" yaml:"max_workloads,omitempty"`
	MaxReplicasTotal  int32  `json:"max_replicas_total,omitempty" yaml:"max_replicas_total,omitempty"`
}
