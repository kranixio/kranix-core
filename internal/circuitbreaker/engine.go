package circuitbreaker

import (
	"time"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config provides defaults when per-workload spec omits values.
type Config struct {
	Enabled             bool
	DefaultFailureThreshold    int32
	DefaultSuccessThreshold    int32
	DefaultOpenDurationSeconds int32
	DefaultHalfOpenMaxRequests int32
}

// Engine evaluates per-workload circuit breaker state for routing decisions.
type Engine struct {
	cfg Config
}

// New creates a circuit breaker engine.
func New(cfg Config) *Engine {
	if cfg.DefaultFailureThreshold <= 0 {
		cfg.DefaultFailureThreshold = 3
	}
	if cfg.DefaultSuccessThreshold <= 0 {
		cfg.DefaultSuccessThreshold = 2
	}
	if cfg.DefaultOpenDurationSeconds <= 0 {
		cfg.DefaultOpenDurationSeconds = 60
	}
	if cfg.DefaultHalfOpenMaxRequests <= 0 {
		cfg.DefaultHalfOpenMaxRequests = 1
	}
	return &Engine{cfg: cfg}
}

// EnabledFor returns whether the breaker is active for this workload.
func (e *Engine) EnabledFor(w *types.Workload) bool {
	if w == nil {
		return false
	}
	if w.Spec.CircuitBreaker != nil && w.Spec.CircuitBreaker.Enabled {
		return true
	}
	return e.cfg.Enabled
}

// SyncFromWorkload updates circuit status from observed workload phase and advances state machine.
func (e *Engine) SyncFromWorkload(w *types.Workload, now time.Time) (prevState, newState string) {
	if !e.EnabledFor(w) {
		return "", ""
	}
	spec := effectiveSpec(w, e.cfg)
	if w.Status.CircuitBreaker == nil {
		w.Status.CircuitBreaker = &types.CircuitBreakerStatus{
			State:          types.CircuitStateClosed,
			LastTransition: now,
		}
	}
	st := w.Status.CircuitBreaker
	prevState = st.State

	// Advance open -> half-open when cooldown elapses.
	if st.State == types.CircuitStateOpen && st.OpenUntil != nil && !now.Before(*st.OpenUntil) {
		e.transition(st, types.CircuitStateHalfOpen, now, "cooldown elapsed, probing")
	}

	tripDegraded := spec.TripOnDegraded == nil || *spec.TripOnDegraded
	unhealthy := w.Status.Phase == types.WorkloadPhaseFailed ||
		(tripDegraded && w.Status.Phase == types.WorkloadPhaseDegraded)

	switch st.State {
	case types.CircuitStateClosed:
		if unhealthy {
			st.ConsecutiveFails++
			st.ConsecutiveOK = 0
			if st.ConsecutiveFails >= spec.FailureThreshold {
				until := now.Add(time.Duration(spec.OpenDurationSeconds) * time.Second)
				st.OpenUntil = &until
				e.transition(st, types.CircuitStateOpen, now, "failure threshold exceeded")
			}
		} else if w.Status.Phase == types.WorkloadPhaseRunning {
			st.ConsecutiveFails = 0
		}
	case types.CircuitStateOpen:
		// already handled transition to half-open above
	case types.CircuitStateHalfOpen:
		if unhealthy {
			st.ConsecutiveFails++
			st.ConsecutiveOK = 0
			until := now.Add(time.Duration(spec.OpenDurationSeconds) * time.Second)
			st.OpenUntil = &until
			e.transition(st, types.CircuitStateOpen, now, "probe failed")
		} else if w.Status.Phase == types.WorkloadPhaseRunning && w.Status.ReadyReplicas > 0 {
			st.ConsecutiveOK++
			if st.ConsecutiveOK >= spec.SuccessThreshold {
				st.ConsecutiveFails = 0
				st.HalfOpenAttempts = 0
				st.OpenUntil = nil
				e.transition(st, types.CircuitStateClosed, now, "recovered")
			}
		}
	}

	newState = st.State
	return prevState, newState
}

// AllowRoute reports whether new traffic / scheduling should target this workload.
func (e *Engine) AllowRoute(w *types.Workload, now time.Time) bool {
	if w == nil {
		return false
	}
	if !e.EnabledFor(w) {
		return w.Status.Phase != types.WorkloadPhaseFailed
	}
	_, _ = e.SyncFromWorkload(w, now)
	st := w.Status.CircuitBreaker
	if st == nil {
		return true
	}
	switch st.State {
	case types.CircuitStateOpen:
		return false
	case types.CircuitStateHalfOpen:
		max := effectiveSpec(w, e.cfg).HalfOpenMaxRequests
		return st.HalfOpenAttempts < max
	default:
		if w.Status.Phase == types.WorkloadPhaseFailed {
			return false
		}
		if w.Status.Phase == types.WorkloadPhaseDegraded {
			trip := effectiveSpec(w, e.cfg).TripOnDegraded
			if trip == nil || *trip {
				return false
			}
		}
		return true
	}
}

// RecordRouteAttempt increments half-open probe counter when routing is allowed in half-open.
func (e *Engine) RecordRouteAttempt(w *types.Workload) {
	if w == nil || w.Status.CircuitBreaker == nil {
		return
	}
	if w.Status.CircuitBreaker.State == types.CircuitStateHalfOpen {
		w.Status.CircuitBreaker.HalfOpenAttempts++
	}
}

func (e *Engine) transition(st *types.CircuitBreakerStatus, state string, now time.Time, msg string) {
	st.State = state
	st.LastTransition = now
	st.Message = msg
}

type effectiveCircuitSpec struct {
	FailureThreshold    int32
	SuccessThreshold    int32
	OpenDurationSeconds int32
	HalfOpenMaxRequests int32
	TripOnDegraded      *bool
}

func effectiveSpec(w *types.Workload, cfg Config) effectiveCircuitSpec {
	es := effectiveCircuitSpec{
		FailureThreshold:    cfg.DefaultFailureThreshold,
		SuccessThreshold:    cfg.DefaultSuccessThreshold,
		OpenDurationSeconds: cfg.DefaultOpenDurationSeconds,
		HalfOpenMaxRequests: cfg.DefaultHalfOpenMaxRequests,
	}
	if w.Spec.CircuitBreaker == nil {
		return es
	}
	s := w.Spec.CircuitBreaker
	if s.FailureThreshold > 0 {
		es.FailureThreshold = s.FailureThreshold
	}
	if s.SuccessThreshold > 0 {
		es.SuccessThreshold = s.SuccessThreshold
	}
	if s.OpenDurationSeconds > 0 {
		es.OpenDurationSeconds = s.OpenDurationSeconds
	}
	if s.HalfOpenMaxRequests > 0 {
		es.HalfOpenMaxRequests = s.HalfOpenMaxRequests
	}
	es.TripOnDegraded = s.TripOnDegraded
	return es
}
