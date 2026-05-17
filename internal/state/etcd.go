package state

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"go.etcd.io/etcd/client/v3"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// EtcdStore provides an etcd implementation of Store.
type EtcdStore struct {
	client   *clientv3.Client
	mu       sync.RWMutex
	watchers []chan types.Event
	muWatchers sync.RWMutex
}

// NewEtcdStore creates a new etcd state store.
func NewEtcdStore(endpoints []string) (*EtcdStore, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints: endpoints,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to etcd: %w", err)
	}

	return &EtcdStore{
		client:   client,
		watchers: make([]chan types.Event, 0),
	}, nil
}

// Get retrieves a workload by ID.
func (s *EtcdStore) Get(ctx context.Context, id string) (*types.Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := fmt.Sprintf("/workloads/%s", id)
	resp, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get workload from etcd: %w", err)
	}

	if len(resp.Kvs) == 0 {
		return nil, ErrWorkloadNotFound
	}

	var workload types.Workload
	if err := json.Unmarshal(resp.Kvs[0].Value, &workload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal workload: %w", err)
	}

	return &workload, nil
}

// List retrieves all workloads, optionally filtered by namespace.
func (s *EtcdStore) List(ctx context.Context, namespace string) ([]*types.Workload, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var prefix string
	if namespace != "" {
		prefix = fmt.Sprintf("/workloads/%s/", namespace)
	} else {
		prefix = "/workloads/"
	}

	resp, err := s.client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		return nil, fmt.Errorf("failed to list workloads from etcd: %w", err)
	}

	var workloads []*types.Workload

	for _, kv := range resp.Kvs {
		var workload types.Workload
		if err := json.Unmarshal(kv.Value, &workload); err != nil {
			return nil, fmt.Errorf("failed to unmarshal workload: %w", err)
		}
		workloads = append(workloads, &workload)
	}

	return workloads, nil
}

// Create stores a new workload.
func (s *EtcdStore) Create(ctx context.Context, workload *types.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("/workloads/%s", workload.ID)
	data, err := json.Marshal(workload)
	if err != nil {
		return fmt.Errorf("failed to marshal workload: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to create workload in etcd: %w", err)
	}

	s.notify(types.Event{
		Type:     types.WorkloadDeployRequested,
		Workload: workload,
	})

	return nil
}

// Update updates an existing workload.
func (s *EtcdStore) Update(ctx context.Context, workload *types.Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	key := fmt.Sprintf("/workloads/%s", workload.ID)
	data, err := json.Marshal(workload)
	if err != nil {
		return fmt.Errorf("failed to marshal workload: %w", err)
	}

	// Check if workload exists
	_, err = s.client.Get(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to check workload existence: %w", err)
	}

	_, err = s.client.Put(ctx, key, string(data))
	if err != nil {
		return fmt.Errorf("failed to update workload in etcd: %w", err)
	}

	s.notify(types.Event{
		Type:     types.WorkloadUpdated,
		Workload: workload,
	})

	return nil
}

// Delete removes a workload.
func (s *EtcdStore) Delete(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get workload for notification
	workload, err := s.Get(ctx, id)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("/workloads/%s", id)
	_, err = s.client.Delete(ctx, key)
	if err != nil {
		return fmt.Errorf("failed to delete workload from etcd: %w", err)
	}

	s.notify(types.Event{
		Type:     types.WorkloadDeleted,
		Workload: workload,
	})

	return nil
}

// Watch returns a channel that emits workload change events.
func (s *EtcdStore) Watch(ctx context.Context) (<-chan types.Event, error) {
	s.muWatchers.Lock()
	defer s.muWatchers.Unlock()

	ch := make(chan types.Event, 100)
	s.watchers = append(s.watchers, ch)

	// Start etcd watcher
	go s.watchEtcd(ctx, ch)

	return ch, nil
}

// watchEtcd watches for changes in etcd and notifies watchers.
func (s *EtcdStore) watchEtcd(ctx context.Context, eventCh chan types.Event) {
	watchCh := s.client.Watch(ctx, "/workloads/", clientv3.WithPrefix())

	for watchResp := range watchCh {
		for _, event := range watchResp.Events {
			var workload types.Workload
			if err := json.Unmarshal(event.Kv.Value, &workload); err != nil {
				continue
			}

			var eventType types.EventType
			switch event.Type {
			case clientv3.EventTypePut:
				eventType = types.WorkloadUpdated
			case clientv3.EventTypeDelete:
				eventType = types.WorkloadDeleted
			}

			select {
			case eventCh <- types.Event{
				Type:     eventType,
				Workload: &workload,
			}:
			default:
				// Channel full, skip
			}
		}
	}
}

// notify sends an event to all watchers.
func (s *EtcdStore) notify(event types.Event) {
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

// Close closes the etcd client connection.
func (s *EtcdStore) Close() error {
	return s.client.Close()
}
