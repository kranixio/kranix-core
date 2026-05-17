package eventsourcing

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config defines event sourcing configuration.
type Config struct {
	Enabled       bool          `json:"enabled"`
	StorageBackend string       `json:"storage_backend"` // memory, postgres, etcd
	MaxEventAge   time.Duration `json:"max_event_age"`
	Compression   bool          `json:"compression"`
}

// Store implements the EventStore interface with in-memory storage.
type Store struct {
	config   Config
	eventBus *eventbus.EventBus
	events   map[string][]*types.DomainEvent // aggregate ID -> events
	mu       sync.RWMutex
	watchers map[string][]chan *types.DomainEvent // aggregate ID -> watchers
	muWatchers sync.RWMutex
}

// New creates a new event sourcing store.
func New(config Config, eventBus *eventbus.EventBus) *Store {
	return &Store{
		config:   config,
		eventBus: eventBus,
		events:   make(map[string][]*types.DomainEvent),
		watchers: make(map[string][]chan *types.DomainEvent),
	}
}

// Append adds a new event to the store.
func (s *Store) Append(ctx context.Context, event *types.DomainEvent) error {
	if !s.config.Enabled {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Generate ID if not provided
	if event.ID == "" {
		event.ID = uuid.New().String()
	}

	// Set timestamp if not provided
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Determine version
	aggregateEvents := s.events[event.Aggregate]
	if len(aggregateEvents) > 0 {
		event.Version = aggregateEvents[len(aggregateEvents)-1].Version + 1
	} else {
		event.Version = 1
	}

	// Store the event
	s.events[event.Aggregate] = append(s.events[event.Aggregate], event)

	// Publish event stored notification
	s.eventBus.PublishAsync(types.Event{
		Type:     types.EventStored,
		Timestamp: time.Now().Unix(),
		Metadata: map[string]any{
			"event_id":     event.ID,
			"aggregate_id": event.Aggregate,
			"event_type":   event.Type,
			"version":      event.Version,
		},
	})

	// Notify watchers
	s.notifyWatchers(event)

	return nil
}

// GetEvents retrieves events for an aggregate.
func (s *Store) GetEvents(ctx context.Context, aggregateID string, fromVersion int64, limit int) ([]*types.DomainEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	events, exists := s.events[aggregateID]
	if !exists {
		return []*types.DomainEvent{}, nil
	}

	var result []*types.DomainEvent
	for _, event := range events {
		if event.Version >= fromVersion {
			result = append(result, event)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}

	return result, nil
}

// GetEvent retrieves a single event by ID.
func (s *Store) GetEvent(ctx context.Context, eventID string) (*types.DomainEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, events := range s.events {
		for _, event := range events {
			if event.ID == eventID {
				return event, nil
			}
		}
	}

	return nil, fmt.Errorf("event not found: %s", eventID)
}

// Replay reconstructs state by replaying events.
func (s *Store) Replay(ctx context.Context, aggregateID string) (interface{}, error) {
	events, err := s.GetEvents(ctx, aggregateID, 0, 0)
	if err != nil {
		return nil, err
	}

	s.eventBus.PublishAsync(types.Event{
		Type:     types.EventReplayed,
		Timestamp: time.Now().Unix(),
		Metadata: map[string]any{
			"aggregate_id": aggregateID,
			"event_count":  len(events),
		},
	})

	return events, nil
}

// Subscribe to events for an aggregate.
func (s *Store) Subscribe(ctx context.Context, aggregateID string) (<-chan *types.DomainEvent, error) {
	s.muWatchers.Lock()
	defer s.muWatchers.Unlock()

	ch := make(chan *types.DomainEvent, 100)
	s.watchers[aggregateID] = append(s.watchers[aggregateID], ch)

	return ch, nil
}

// notifyWatchers sends an event to all watchers for an aggregate.
func (s *Store) notifyWatchers(event *types.DomainEvent) {
	s.muWatchers.RLock()
	defer s.muWatchers.RUnlock()

	watchers, exists := s.watchers[event.Aggregate]
	if !exists {
		return
	}

	for _, ch := range watchers {
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
	}
}

// RecordWorkloadCreated records a workload creation event.
func (s *Store) RecordWorkloadCreated(ctx context.Context, workload *types.Workload) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     workload.ID,
		AggregateType: "workload",
		Type:          "WorkloadCreated",
		Data: map[string]interface{}{
			"workload_id":   workload.ID,
			"workload_name": workload.Name,
			"namespace":     workload.Namespace,
			"spec":          workload.Spec,
			"labels":        workload.Labels,
		},
		Metadata: map[string]string{
			"namespace": workload.Namespace,
		},
	})
}

// RecordWorkloadUpdated records a workload update event.
func (s *Store) RecordWorkloadUpdated(ctx context.Context, workload *types.Workload, oldSpec *types.WorkloadSpec) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     workload.ID,
		AggregateType: "workload",
		Type:          "WorkloadUpdated",
		Data: map[string]interface{}{
			"workload_id":   workload.ID,
			"workload_name": workload.Name,
			"namespace":     workload.Namespace,
			"new_spec":      workload.Spec,
			"old_spec":      oldSpec,
		},
		Metadata: map[string]string{
			"namespace": workload.Namespace,
		},
	})
}

// RecordWorkloadDeleted records a workload deletion event.
func (s *Store) RecordWorkloadDeleted(ctx context.Context, workload *types.Workload) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     workload.ID,
		AggregateType: "workload",
		Type:          "WorkloadDeleted",
		Data: map[string]interface{}{
			"workload_id":   workload.ID,
			"workload_name": workload.Name,
			"namespace":     workload.Namespace,
		},
		Metadata: map[string]string{
			"namespace": workload.Namespace,
		},
	})
}

// RecordWorkloadPhaseTransition records a workload phase transition event.
func (s *Store) RecordWorkloadPhaseTransition(ctx context.Context, workload *types.Workload, fromPhase, toPhase types.WorkloadPhase, reason string) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     workload.ID,
		AggregateType: "workload",
		Type:          "WorkloadPhaseTransition",
		Data: map[string]interface{}{
			"workload_id":   workload.ID,
			"workload_name": workload.Name,
			"namespace":     workload.Namespace,
			"from_phase":    fromPhase,
			"to_phase":      toPhase,
			"reason":        reason,
		},
		Metadata: map[string]string{
			"namespace": workload.Namespace,
		},
	})
}

// RecordWorkloadDriftDetected records a drift detection event.
func (s *Store) RecordWorkloadDriftDetected(ctx context.Context, report *types.DriftReport) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     report.WorkloadID,
		AggregateType: "workload",
		Type:          "WorkloadDriftDetected",
		Data: map[string]interface{}{
			"workload_id":    report.WorkloadID,
			"workload_name":  report.WorkloadName,
			"namespace":      report.Namespace,
			"drifted_fields": report.DriftedFields,
			"severity":       report.Severity,
			"message":        report.Message,
		},
		Metadata: map[string]string{
			"namespace": report.Namespace,
			"severity":  string(report.Severity),
		},
	})
}

// RecordWorkloadDriftReconciled records a drift reconciliation event.
func (s *Store) RecordWorkloadDriftReconciled(ctx context.Context, report *types.DriftReport) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     report.WorkloadID,
		AggregateType: "workload",
		Type:          "WorkloadDriftReconciled",
		Data: map[string]interface{}{
			"workload_id":    report.WorkloadID,
			"workload_name":  report.WorkloadName,
			"namespace":      report.Namespace,
			"drifted_fields": report.DriftedFields,
		},
		Metadata: map[string]string{
			"namespace": report.Namespace,
		},
	})
}

// RecordWorkloadScaled records a workload scaling event.
func (s *Store) RecordWorkloadScaled(ctx context.Context, workload *types.Workload, oldReplicas, newReplicas int32, reason string) error {
	return s.Append(ctx, &types.DomainEvent{
		Aggregate:     workload.ID,
		AggregateType: "workload",
		Type:          "WorkloadScaled",
		Data: map[string]interface{}{
			"workload_id":   workload.ID,
			"workload_name": workload.Name,
			"namespace":     workload.Namespace,
			"old_replicas":  oldReplicas,
			"new_replicas":  newReplicas,
			"reason":        reason,
		},
		Metadata: map[string]string{
			"namespace": workload.Namespace,
		},
	})
}

// GetWorkloadHistory retrieves the full event history for a workload.
func (s *Store) GetWorkloadHistory(ctx context.Context, workloadID string) ([]*types.DomainEvent, error) {
	return s.GetEvents(ctx, workloadID, 0, 0)
}

// CleanupOldEvents removes events older than maxEventAge.
func (s *Store) CleanupOldEvents(ctx context.Context) error {
	if s.config.MaxEventAge <= 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-s.config.MaxEventAge)
	for aggregateID, events := range s.events {
		var filtered []*types.DomainEvent
		for _, event := range events {
			if event.Timestamp.After(cutoff) {
				filtered = append(filtered, event)
			}
		}
		s.events[aggregateID] = filtered
	}

	return nil
}
