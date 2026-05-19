package secretrotation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/internal/eventsourcing"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config configures secret rotation awareness.
type Config struct {
	Enabled bool
}

// Engine detects secret version changes and marks workloads for rolling restart.
type Engine struct {
	cfg        Config
	registry   *Registry
	store      state.Store
	eventStore *eventsourcing.Store
	eventBus   *eventbus.EventBus
}

// New creates a secret rotation engine.
func New(cfg Config, registry *Registry, store state.Store, eventStore *eventsourcing.Store, bus *eventbus.EventBus) *Engine {
	if registry == nil {
		registry = NewRegistry()
	}
	return &Engine{cfg: cfg, registry: registry, store: store, eventStore: eventStore, eventBus: bus}
}

// Registry returns the underlying secret registry.
func (e *Engine) Registry() *Registry {
	return e.registry
}

// EnabledFor reports whether rotation handling is active for the workload.
func (e *Engine) EnabledFor(w *types.Workload) bool {
	if w == nil {
		return false
	}
	if w.Spec.SecretRotation != nil && w.Spec.SecretRotation.Enabled {
		return true
	}
	return e.cfg.Enabled
}

// IndexWorkload registers secret references from a workload.
func (e *Engine) IndexWorkload(w *types.Workload) {
	if w == nil || !e.EnabledFor(w) {
		return
	}
	var names []string
	if w.Spec.SecretRotation != nil {
		for _, ref := range w.Spec.SecretRotation.SecretRefs {
			if ref.Name != "" {
				names = append(names, ref.Name)
			}
		}
	}
	e.registry.RegisterWorkload(w.ID, w.Namespace, names)
}

// RotationNotification is sent when an external controller detects a secret change.
type RotationNotification struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	ResourceVersion string `json:"resource_version,omitempty"`
}

// HandleRotation records a new secret version and queues rolling restarts for dependents.
func (e *Engine) HandleRotation(ctx context.Context, n RotationNotification) ([]string, error) {
	if strings.TrimSpace(n.Name) == "" {
		return nil, fmt.Errorf("secret name required")
	}
	version := strings.TrimSpace(n.Version)
	if version == "" {
		version = strings.TrimSpace(n.ResourceVersion)
	}
	if version == "" {
		return nil, fmt.Errorf("version or resource_version required")
	}

	changed, _ := e.registry.ObserveVersion(n.Namespace, n.Name, version)
	if !changed {
		return nil, nil
	}

	affected := e.registry.WorkloadsForSecret(n.Namespace, n.Name)
	now := time.Now().UTC()
	var restarted []string

	for _, id := range affected {
		w, err := e.store.Get(ctx, id)
		if err != nil || w == nil {
			continue
		}
		if !e.EnabledFor(w) {
			continue
		}
		if w.Status.SecretRotation == nil {
			w.Status.SecretRotation = &types.SecretRotationStatus{}
		}
		w.Status.SecretRotation.PendingRestart = true
		w.Status.SecretRotation.SecretName = n.Name
		w.Status.SecretRotation.SecretVersion = version
		t := now
		w.Status.SecretRotation.LastRotation = &t
		w.Status.SecretRotation.Message = "secret rotated; rolling restart pending"
		w.UpdatedAt = now

		if err := e.store.Update(ctx, w); err != nil {
			continue
		}
		if e.eventStore != nil {
			_ = e.eventStore.RecordSecretRotated(ctx, w, n.Name, version)
		}
		if e.eventBus != nil {
			e.eventBus.PublishAsync(types.Event{
				Type:     types.WorkloadSecretRotated,
				Workload: w,
				Metadata: map[string]any{
					"secret_name":    n.Name,
					"secret_version": version,
				},
			})
		}
		restarted = append(restarted, id)
	}
	return restarted, nil
}

// SecretRefNames extracts secret names from a workload spec.
func SecretRefNames(w *types.Workload) []string {
	if w == nil || w.Spec.SecretRotation == nil {
		return nil
	}
	var out []string
	for _, ref := range w.Spec.SecretRotation.SecretRefs {
		if ref.Name != "" {
			out = append(out, ref.Name)
		}
	}
	return out
}
