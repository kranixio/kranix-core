package types

// DiffChange describes a single field difference between desired and live state.
type DiffChange struct {
	Field      string      `json:"field"`
	OldValue   interface{} `json:"old_value,omitempty"`
	NewValue   interface{} `json:"new_value,omitempty"`
	ChangeType string      `json:"change_type"` // added | modified | removed
}

// DiffSummary aggregates diff statistics.
type DiffSummary struct {
	TotalChanges int  `json:"total_changes"`
	Added        int  `json:"added"`
	Modified     int  `json:"modified"`
	Removed      int  `json:"removed"`
	HasDrift     bool `json:"has_drift"`
}

// WorkloadDiffResult is returned by the workload diff endpoint.
type WorkloadDiffResult struct {
	WorkloadID   string                 `json:"workload_id"`
	WorkloadName string                 `json:"workload_name,omitempty"`
	Namespace    string                 `json:"namespace,omitempty"`
	Desired      map[string]interface{} `json:"desired,omitempty"`
	Live         map[string]interface{} `json:"live,omitempty"`
	Changes      []DiffChange           `json:"changes"`
	Summary      DiffSummary            `json:"summary"`
}
