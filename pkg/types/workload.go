package types

import (
	"time"
)

// Workload represents a managed unit with desired and observed state.
type Workload struct {
	// Metadata
	ID        string            `json:"id"`
	Name      string            `json:"name"`
	Namespace string            `json:"namespace"`
	Labels    map[string]string `json:"labels,omitempty"`
	CreatedAt time.Time         `json:"created_at"`
	UpdatedAt time.Time         `json:"updated_at"`

	// Spec - desired configuration
	Spec WorkloadSpec `json:"spec"`

	// Status - current observed state
	Status WorkloadStatus `json:"status"`

	// History - immutable log of state transitions
	History []WorkloadStateTransition `json:"history,omitempty"`
}

// WorkloadSpec defines the desired configuration of a workload.
type WorkloadSpec struct {
	Image           string            `json:"image"`
	Replicas        int32             `json:"replicas"`
	Env             map[string]string `json:"env,omitempty"`
	Resources       ResourceRequirements `json:"resources,omitempty"`
	Backend         string            `json:"backend"` // docker, kubernetes, etc.
	RolloutStrategy RolloutStrategy   `json:"rollout_strategy,omitempty"`
}

// ResourceRequirements defines compute resource requests and limits.
type ResourceRequirements struct {
	CPURequest    string `json:"cpu_request,omitempty"`
	CPULimit      string `json:"cpu_limit,omitempty"`
	MemoryRequest string `json:"memory_request,omitempty"`
	MemoryLimit   string `json:"memory_limit,omitempty"`
}

// RolloutStrategy defines how the workload should be deployed.
type RolloutStrategy struct {
	Type          string `json:"type"` // rolling, recreate, bluegreen
	MaxUnavailable int32  `json:"max_unavailable,omitempty"`
	MaxSurge      int32  `json:"max_surge,omitempty"`
}

// WorkloadStatus represents the current observed state of a workload.
type WorkloadStatus struct {
	Phase           WorkloadPhase `json:"phase"`
	Replicas        int32         `json:"replicas"`
	AvailableReplicas int32       `json:"available_replicas"`
	ReadyReplicas   int32         `json:"ready_replicas"`
	Message         string        `json:"message,omitempty"`
	LastTransition  time.Time     `json:"last_transition"`
}

// WorkloadPhase represents the lifecycle phase of a workload.
type WorkloadPhase string

const (
	WorkloadPhasePending   WorkloadPhase = "Pending"
	WorkloadPhaseRunning   WorkloadPhase = "Running"
	WorkloadPhaseDegraded  WorkloadPhase = "Degraded"
	WorkloadPhaseFailed    WorkloadPhase = "Failed"
	WorkloadPhaseSucceeded WorkloadPhase = "Succeeded"
)

// WorkloadStateTransition records a state change in the workload's history.
type WorkloadStateTransition struct {
	FromPhase  WorkloadPhase `json:"from_phase"`
	ToPhase    WorkloadPhase `json:"to_phase"`
	Timestamp  time.Time     `json:"timestamp"`
	Reason     string        `json:"reason"`
	Message    string        `json:"message,omitempty"`
}
