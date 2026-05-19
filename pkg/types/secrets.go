package types

import "time"

// SecretRef identifies a Kubernetes or platform secret mounted by the workload.
type SecretRef struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace,omitempty"`
	// EnvKey when set means the workload consumes this secret via env (optional).
	EnvKey string `json:"env_key,omitempty"`
}

// SecretRotationSpec enables rolling restarts when linked secrets change.
type SecretRotationSpec struct {
	Enabled    bool        `json:"enabled,omitempty"`
	SecretRefs []SecretRef `json:"secret_refs,omitempty"`
}

// SecretRotationStatus records observed secret versions and pending restarts.
type SecretRotationStatus struct {
	PendingRestart bool       `json:"pending_restart,omitempty"`
	LastRotation   *time.Time `json:"last_rotation,omitempty"`
	SecretName     string     `json:"secret_name,omitempty"`
	SecretVersion  string     `json:"secret_version,omitempty"`
	RestartCount   int32      `json:"restart_count,omitempty"`
	Message        string     `json:"message,omitempty"`
}
