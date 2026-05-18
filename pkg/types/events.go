package types

// EventType represents the type of event.
type EventType string

const (
	// WorkloadDeployRequested is emitted when a deployment is requested via API
	WorkloadDeployRequested EventType = "WorkloadDeployRequested"
	// WorkloadScheduled is emitted when the scheduler assigns a workload
	WorkloadScheduled EventType = "WorkloadScheduled"
	// WorkloadCronTriggered is emitted when a cron-defined workload triggers the scheduler run.
	WorkloadCronTriggered EventType = "WorkloadCronTriggered"
	// WorkloadRunning is emitted when a workload is successfully running
	WorkloadRunning EventType = "WorkloadRunning"
	// WorkloadFailed is emitted when a workload fails
	WorkloadFailed EventType = "WorkloadFailed"
	// WorkloadUpdated is emitted when a workload spec is updated
	WorkloadUpdated EventType = "WorkloadUpdated"
	// WorkloadDeleted is emitted when a workload is deleted
	WorkloadDeleted EventType = "WorkloadDeleted"
	// WorkloadScaled is emitted when auto-scaling changes replica count
	WorkloadScaled EventType = "WorkloadScaled"
	// WorkloadRolloutStarted is emitted when a rollout strategy begins
	WorkloadRolloutStarted EventType = "WorkloadRolloutStarted"
	// WorkloadRolloutCompleted is emitted when a rollout strategy completes
	WorkloadRolloutCompleted EventType = "WorkloadRolloutCompleted"
	// WorkloadRolloutFailed is emitted when a rollout strategy fails
	WorkloadRolloutFailed EventType = "WorkloadRolloutFailed"
	// WorkloadDriftDetected is emitted when drift is detected between desired and actual state
	WorkloadDriftDetected EventType = "WorkloadDriftDetected"
	// WorkloadDriftReconciled is emitted when drift is automatically reconciled
	WorkloadDriftReconciled EventType = "WorkloadDriftReconciled"
	// EventStored is emitted when a domain event is persisted to the event store
	EventStored EventType = "EventStored"
	// EventReplayed is emitted when events are replayed to reconstruct state
	EventReplayed EventType = "EventReplayed"
	// HealthCheckPassed is emitted when a health check passes
	HealthCheckPassed EventType = "HealthCheckPassed"
	// HealthCheckFailed is emitted when a health check fails
	HealthCheckFailed EventType = "HealthCheckFailed"
	// HealthGateBlocked is emitted when health gates block a rollout
	HealthGateBlocked EventType = "HealthGateBlocked"
	// HealthGatePassed is emitted when health gates allow a rollout to proceed
	HealthGatePassed EventType = "HealthGatePassed"
)

// Event represents an internal event in the system.
type Event struct {
	Type      EventType      `json:"type"`
	Workload  *Workload      `json:"workload,omitempty"`
	Timestamp int64          `json:"timestamp"`
	Metadata  map[string]any `json:"metadata,omitempty"`
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
