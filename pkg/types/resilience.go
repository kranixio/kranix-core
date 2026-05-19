package types

import "time"

// Circuit breaker states.
const (
	CircuitStateClosed   = "closed"
	CircuitStateOpen     = "open"
	CircuitStateHalfOpen = "half-open"
)

// CircuitBreakerSpec configures automatic traffic cut-off for unhealthy workloads.
type CircuitBreakerSpec struct {
	Enabled              bool  `json:"enabled,omitempty"`
	FailureThreshold     int32 `json:"failure_threshold,omitempty"`      // consecutive failures before open
	SuccessThreshold     int32 `json:"success_threshold,omitempty"`      // successes in half-open before closed
	OpenDurationSeconds  int32 `json:"open_duration_seconds,omitempty"`  // how long circuit stays open
	HalfOpenMaxRequests  int32 `json:"half_open_max_requests,omitempty"` // probe requests allowed in half-open
	// TripOnDegraded opens the circuit when phase is Degraded (default true when enabled).
	TripOnDegraded *bool `json:"trip_on_degraded,omitempty"`
}

// CircuitBreakerStatus is observed circuit state persisted on the workload.
type CircuitBreakerStatus struct {
	State            string     `json:"state"`
	ConsecutiveFails int32      `json:"consecutive_fails,omitempty"`
	ConsecutiveOK    int32      `json:"consecutive_ok,omitempty"`
	HalfOpenAttempts int32      `json:"half_open_attempts,omitempty"`
	LastTransition   time.Time  `json:"last_transition,omitempty"`
	OpenUntil        *time.Time `json:"open_until,omitempty"`
	Message          string     `json:"message,omitempty"`
}

// WarmStandbySpec keeps a cold replica workload ready for failover.
type WarmStandbySpec struct {
	Enabled      bool  `json:"enabled,omitempty"`
	Replicas     int32 `json:"replicas,omitempty"`      // cold standby size (typically 1)
	AutoPromote  bool  `json:"auto_promote,omitempty"`  // promote when primary circuit opens
	// StandbyWorkloadID links an existing standby; when empty core creates {id}-standby.
	StandbyWorkloadID string `json:"standby_workload_id,omitempty"`
}

// WarmStandbyPhase describes standby lifecycle.
type WarmStandbyPhase string

const (
	WarmStandbyPhaseCold     WarmStandbyPhase = "Cold"
	WarmStandbyPhaseWarming  WarmStandbyPhase = "Warming"
	WarmStandbyPhasePromoted WarmStandbyPhase = "Promoted"
)

// WarmStandbyStatus records linked standby workload state.
type WarmStandbyStatus struct {
	StandbyWorkloadID string           `json:"standby_workload_id,omitempty"`
	Phase             WarmStandbyPhase `json:"phase,omitempty"`
	ReadyReplicas     int32            `json:"ready_replicas,omitempty"`
	LastFailover      *time.Time       `json:"last_failover,omitempty"`
	Message           string           `json:"message,omitempty"`
}
