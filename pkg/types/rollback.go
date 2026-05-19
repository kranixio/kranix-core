package types

import "time"

// WorkloadRevision is an immutable snapshot of a workload spec (and tags) kept for instant rollback.
type WorkloadRevision struct {
	ID           string        `json:"id"`
	RecordedAt   time.Time     `json:"recorded_at"`
	Spec         WorkloadSpec  `json:"spec"`
	Tags         *WorkloadTags `json:"tags,omitempty"`
	Labels       map[string]string `json:"labels,omitempty"`
	ChangeReason string        `json:"change_reason,omitempty"`
}

// RollbackHistoryStatus exposes revision bookkeeping on the workload.
type RollbackHistoryStatus struct {
	MaxVersions int    `json:"max_versions,omitempty"`
	Count       int    `json:"count,omitempty"`
	ActiveID    string `json:"active_revision_id,omitempty"` // revision id of current spec when known
}
