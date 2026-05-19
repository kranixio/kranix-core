package rollouthistory

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/kranix-io/kranix-core/internal/workloadtags"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// ErrWorkloadNotFound is returned when a workload id does not exist.
var ErrWorkloadNotFound = errors.New("workload not found")

// Store is the subset of state needed for rollback operations (avoids import cycles).
type Store interface {
	Get(ctx context.Context, id string) (*types.Workload, error)
	Update(ctx context.Context, workload *types.Workload) error
}

const defaultMaxVersions = 10

// Config controls revision retention.
type Config struct {
	MaxVersions int
}

// RecordBeforeUpdate appends a snapshot of previous onto next.RollbackVersions and trims to maxN.
func RecordBeforeUpdate(next *types.Workload, previous *types.Workload, maxN int) {
	if next == nil || previous == nil || maxN <= 0 {
		return
	}
	if specsEqual(previous.Spec, next.Spec) && tagsEqual(previous, next) {
		return
	}

	rev := snapshotRevision(previous, "update")
	next.RollbackVersions = prependRevision(next.RollbackVersions, rev, maxN)
	refreshStatus(next, maxN, rev.ID)
}

// RecordInitial stores the first revision on create when enabled.
func RecordInitial(w *types.Workload, maxN int) {
	if w == nil || maxN <= 0 {
		return
	}
	if len(w.RollbackVersions) > 0 {
		return
	}
	rev := snapshotRevision(w, "create")
	w.RollbackVersions = []types.WorkloadRevision{rev}
	refreshStatus(w, maxN, rev.ID)
}

func snapshotRevision(w *types.Workload, reason string) types.WorkloadRevision {
	workloadtags.SyncFromLabels(w)
	specCopy := w.Spec
	var tagsCopy *types.WorkloadTags
	if w.Tags != nil {
		t := *w.Tags
		if w.Tags.Custom != nil {
			t.Custom = copyStringMap(w.Tags.Custom)
		}
		tagsCopy = &t
	}
	return types.WorkloadRevision{
		ID:           newRevisionID(),
		RecordedAt:   time.Now().UTC(),
		Spec:         specCopy,
		Tags:         tagsCopy,
		Labels:       copyStringMap(w.Labels),
		ChangeReason: reason,
	}
}

func prependRevision(existing []types.WorkloadRevision, rev types.WorkloadRevision, maxN int) []types.WorkloadRevision {
	out := make([]types.WorkloadRevision, 0, maxN+1)
	out = append(out, rev)
	out = append(out, existing...)
	if len(out) > maxN {
		out = out[:maxN]
	}
	return out
}

func refreshStatus(w *types.Workload, maxN int, activeID string) {
	if w.Status.Rollback == nil {
		w.Status.Rollback = &types.RollbackHistoryStatus{}
	}
	w.Status.Rollback.MaxVersions = maxN
	w.Status.Rollback.Count = len(w.RollbackVersions)
	w.Status.Rollback.ActiveID = activeID
}

// ListRevisions returns stored revisions for a workload (newest first).
func ListRevisions(ctx context.Context, store Store, workloadID string) ([]types.WorkloadRevision, error) {
	w, err := store.Get(ctx, workloadID)
	if err != nil {
		return nil, err
	}
	if w == nil {
		return nil, ErrWorkloadNotFound
	}
	return w.RollbackVersions, nil
}

// Revert restores spec/tags from revisionID and records the pre-revert state in history.
func Revert(ctx context.Context, store Store, workloadID, revisionID string, maxN int) (*types.Workload, error) {
	if maxN <= 0 {
		maxN = defaultMaxVersions
	}
	w, err := store.Get(ctx, workloadID)
	if err != nil {
		return nil, err
	}
	if w == nil {
		return nil, ErrWorkloadNotFound
	}

	rev, ok := findRevision(w.RollbackVersions, revisionID)
	if !ok {
		return nil, fmt.Errorf("revision %q not found for workload %s", revisionID, workloadID)
	}

	// Capture current before revert.
	current := *w
	current.Spec = w.Spec
	if w.Tags != nil {
		t := *w.Tags
		current.Tags = &t
	}

	w.Spec = rev.Spec
	if rev.Tags != nil {
		t := *rev.Tags
		if rev.Tags.Custom != nil {
			t.Custom = copyStringMap(rev.Tags.Custom)
		}
		w.Tags = &t
	} else {
		w.Tags = nil
	}
	if len(rev.Labels) > 0 {
		w.Labels = copyStringMap(rev.Labels)
	}
	workloadtags.Apply(w)

	RecordBeforeUpdate(w, &current, maxN)
	w.UpdatedAt = time.Now().UTC()
	refreshStatus(w, maxN, revisionID)

	if err := store.Update(ctx, w); err != nil {
		return nil, err
	}
	return w, nil
}

func findRevision(revs []types.WorkloadRevision, id string) (types.WorkloadRevision, bool) {
	for _, r := range revs {
		if r.ID == id {
			return r, true
		}
	}
	return types.WorkloadRevision{}, false
}

func newRevisionID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func copyStringMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func specsEqual(a, b types.WorkloadSpec) bool {
	return a.Image == b.Image && a.Replicas == b.Replicas && a.Backend == b.Backend
}

func tagsEqual(a, b *types.Workload) bool {
	workloadtags.SyncFromLabels(a)
	workloadtags.SyncFromLabels(b)
	ta, tb := a.Tags, b.Tags
	if ta == nil && tb == nil {
		return true
	}
	if ta == nil || tb == nil {
		return false
	}
	return ta.Team == tb.Team && ta.Environment == tb.Environment && ta.CostCenter == tb.CostCenter
}
