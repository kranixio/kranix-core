package state

import (
	"context"
	"sync"

	"github.com/kranix-io/kranix-core/internal/eventsourcing"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Store defines the interface for workload state storage.
type Store interface {
	// Get retrieves a workload by ID.
	Get(ctx context.Context, id string) (*types.Workload, error)
	// List retrieves all workloads, optionally filtered by namespace.
	List(ctx context.Context, namespace string) ([]*types.Workload, error)
	// Create stores a new workload.
	Create(ctx context.Context, workload *types.Workload) error
	// Update updates an existing workload.
	Update(ctx context.Context, workload *types.Workload) error
	// Delete removes a workload.
	Delete(ctx context.Context, id string) error
	// Watch returns a channel that emits workload change events.
	Watch(ctx context.Context) (<-chan types.Event, error)
}

// MemoryStore provides an in-memory implementation of Store.
type MemoryStore struct {
	workloads  map[string]*types.Workload
	mu         sync.RWMutex
	watchers   []chan types.Event
	muWatchers sync.RWMutex
	eventStore *eventsourcing.Store
}

// NewMemoryStore creates a new in-memory state store.
func NewMemoryStore(eventStore *eventsourcing.Store) *MemoryStore {
	return &MemoryStore{
		workloads:  make(map[string]*types.Workload),
		watchers:   make([]chan types.Event, 0),
		eventStore: eventStore,
	}
}

// Get retrieves a workload by ID.
func (s *MemoryStore) Get(ctx context.Context, id string) (*types.Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.workloads[id], nil
}

// List retrieves all workloads, optionally filtered by namespace.
func (s *MemoryStore) List(ctx context.Context, namespace string) ([]*types.Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []*types.Workload
	for _, w := range s.workloads {
		if namespace == "" || w.Namespace == namespace {
			result = append(result, w)
		}
	}
	return result, nil
}

// Create stores a new workload.
func (s *MemoryStore) Create(ctx context.Context, workload *types.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.workloads[workload.ID]; exists {
		return ErrWorkloadAlreadyExists
	}

	s.workloads[workload.ID] = workload

	// Record event in event store
	if s.eventStore != nil {
		_ = s.eventStore.RecordWorkloadCreated(ctx, workload)
	}

	s.notify(types.Event{
		Type:     types.WorkloadDeployRequested,
		Workload: workload,
	})
	return nil
}

// Update updates an existing workload.
func (s *MemoryStore) Update(ctx context.Context, workload *types.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	oldWorkload, exists := s.workloads[workload.ID]
	if !exists {
		return ErrWorkloadNotFound
	}

	s.workloads[workload.ID] = workload

	// Record event in event store
	if s.eventStore != nil {
		_ = s.eventStore.RecordWorkloadUpdated(ctx, workload, &oldWorkload.Spec)
	}

	s.notify(types.Event{
		Type:     types.WorkloadUpdated,
		Workload: workload,
	})
	return nil
}

// Delete removes a workload.
func (s *MemoryStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	workload, exists := s.workloads[id]
	if !exists {
		return ErrWorkloadNotFound
	}

	delete(s.workloads, id)

	// Record event in event store
	if s.eventStore != nil {
		_ = s.eventStore.RecordWorkloadDeleted(ctx, workload)
	}

	s.notify(types.Event{
		Type:     types.WorkloadDeleted,
		Workload: workload,
	})
	return nil
}

// Watch returns a channel that emits workload change events.
func (s *MemoryStore) Watch(ctx context.Context) (<-chan types.Event, error) {
	s.muWatchers.Lock()
	defer s.muWatchers.Unlock()

	ch := make(chan types.Event, 100)
	s.watchers = append(s.watchers, ch)
	return ch, nil
}

// notify sends an event to all watchers.
func (s *MemoryStore) notify(event types.Event) {
	s.muWatchers.RLock()
	defer s.muWatchers.RUnlock()

	for _, ch := range s.watchers {
		select {
		case ch <- event:
		default:
			// Channel full, skip
		}
	}
}

// Store errors.
var (
	ErrWorkloadAlreadyExists = &StoreError{Message: "workload already exists"}
	ErrWorkloadNotFound      = &StoreError{Message: "workload not found"}
)

// StoreError represents a store-specific error.
type StoreError struct {
	Message string
}

func (e *StoreError) Error() string {
	return e.Message
}
