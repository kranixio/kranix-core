package state

import (
	"context"

	"github.com/kranix-io/kranix-core/internal/rollouthistory"
	"github.com/kranix-io/kranix-core/internal/workloadtags"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// VersionedStore wraps a Store to maintain rollback revision history and normalize tags.
type VersionedStore struct {
	inner       Store
	maxVersions int
}

// NewVersionedStore wraps inner and retains the last maxVersions spec snapshots per workload.
func NewVersionedStore(inner Store, maxVersions int) *VersionedStore {
	if maxVersions <= 0 {
		maxVersions = 10
	}
	return &VersionedStore{inner: inner, maxVersions: maxVersions}
}

func (s *VersionedStore) Get(ctx context.Context, id string) (*types.Workload, error) {
	return s.inner.Get(ctx, id)
}

func (s *VersionedStore) List(ctx context.Context, namespace string) ([]*types.Workload, error) {
	return s.inner.List(ctx, namespace)
}

func (s *VersionedStore) Create(ctx context.Context, workload *types.Workload) error {
	workloadtags.Apply(workload)
	rollouthistory.RecordInitial(workload, s.maxVersions)
	return s.inner.Create(ctx, workload)
}

func (s *VersionedStore) Update(ctx context.Context, workload *types.Workload) error {
	previous, err := s.inner.Get(ctx, workload.ID)
	if err != nil {
		return err
	}
	workloadtags.Apply(workload)
	if previous != nil {
		rollouthistory.RecordBeforeUpdate(workload, previous, s.maxVersions)
	}
	return s.inner.Update(ctx, workload)
}

func (s *VersionedStore) Delete(ctx context.Context, id string) error {
	return s.inner.Delete(ctx, id)
}

func (s *VersionedStore) Watch(ctx context.Context) (<-chan types.Event, error) {
	return s.inner.Watch(ctx)
}

// Inner exposes the wrapped store for components that need direct access.
func (s *VersionedStore) Inner() Store {
	return s.inner
}

// MaxVersions returns configured retention.
func (s *VersionedStore) MaxVersions() int {
	return s.maxVersions
}
