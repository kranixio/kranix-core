package types

// EventType represents the type of event.
type EventType string

const (
	// WorkloadDeployRequested is emitted when a deployment is requested via API
	WorkloadDeployRequested EventType = "WorkloadDeployRequested"
	// WorkloadScheduled is emitted when the scheduler assigns a workload
	WorkloadScheduled EventType = "WorkloadScheduled"
	// WorkloadRunning is emitted when a workload is successfully running
	WorkloadRunning EventType = "WorkloadRunning"
	// WorkloadFailed is emitted when a workload fails
	WorkloadFailed EventType = "WorkloadFailed"
	// WorkloadUpdated is emitted when a workload spec is updated
	WorkloadUpdated EventType = "WorkloadUpdated"
	// WorkloadDeleted is emitted when a workload is deleted
	WorkloadDeleted EventType = "WorkloadDeleted"
)

// Event represents an internal event in the system.
type Event struct {
	Type      EventType       `json:"type"`
	Workload  *Workload       `json:"workload,omitempty"`
	Timestamp int64           `json:"timestamp"`
	Metadata  map[string]any  `json:"metadata,omitempty"`
}

// RuntimeDriver defines the interface for runtime backends.
type RuntimeDriver interface {
	// Deploy deploys a workload to the runtime
	Deploy(ctx interface{}, workload *Workload) error
	// Update updates an existing workload
	Update(ctx interface{}, workload *Workload) error
	// Delete removes a workload from the runtime
	Delete(ctx interface{}, workloadID string) error
	// GetStatus retrieves the current status of a workload
	GetStatus(ctx interface{}, workloadID string) (*WorkloadStatus, error)
	// Name returns the name of the runtime driver
	Name() string
}
