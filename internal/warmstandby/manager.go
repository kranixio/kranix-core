package warmstandby

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/kranix-io/kranix-core/internal/circuitbreaker"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/internal/workloadtags"
	"github.com/kranix-io/kranix-core/pkg/types"
)

const (
	LabelStandbyFor = "kranix.io/standby-for"
	LabelRole       = "kranix.io/role"
	RoleStandby     = "standby"
)

// Config provides warm standby defaults.
type Config struct {
	Enabled                bool
	DefaultStandbyReplicas int32
	DefaultAutoPromote     bool
}

// Manager provisions and promotes cold standby workloads.
type Manager struct {
	cfg      Config
	circuits *circuitbreaker.Engine
}

// New creates a warm standby manager.
func New(cfg Config, circuits *circuitbreaker.Engine) *Manager {
	if cfg.DefaultStandbyReplicas <= 0 {
		cfg.DefaultStandbyReplicas = 1
	}
	return &Manager{cfg: cfg, circuits: circuits}
}

// IsStandbyWorkload reports workloads created as cold failover replicas.
func IsStandbyWorkload(w *types.Workload) bool {
	return w != nil && w.Labels != nil && w.Labels[LabelRole] == RoleStandby
}

// EnabledFor reports whether warm standby is configured for the workload.
func (m *Manager) EnabledFor(w *types.Workload) bool {
	if w == nil || IsStandbyWorkload(w) {
		return false
	}
	if w.Spec.WarmStandby != nil && w.Spec.WarmStandby.Enabled {
		return true
	}
	return m.cfg.Enabled
}

// EnsureColdStandby creates or updates a linked standby workload at zero desired replicas (cold).
func (m *Manager) EnsureColdStandby(ctx context.Context, store state.Store, primary *types.Workload) error {
	if !m.EnabledFor(primary) {
		return nil
	}
	spec := primary.Spec.WarmStandby
	if spec == nil {
		spec = &types.WarmStandbySpec{Enabled: true}
	}
	standbyID := strings.TrimSpace(spec.StandbyWorkloadID)
	if standbyID == "" {
		standbyID = primary.ID + "-standby"
	}

	standby, err := store.Get(ctx, standbyID)
	if err != nil || standby == nil {
		standby = m.newStandbyWorkload(primary, standbyID, spec)
		workloadtags.Apply(standby)
		if err := store.Create(ctx, standby); err != nil {
			return fmt.Errorf("create standby workload: %w", err)
		}
	} else {
		standby.Spec = cloneSpecForStandby(primary, 0)
		standby.Namespace = primary.Namespace
		standby.Name = primary.Name + "-standby"
		standby.UpdatedAt = time.Now().UTC()
		workloadtags.Apply(standby)
		if err := store.Update(ctx, standby); err != nil {
			return fmt.Errorf("update standby workload: %w", err)
		}
	}

	if primary.Status.WarmStandby == nil {
		primary.Status.WarmStandby = &types.WarmStandbyStatus{}
	}
	primary.Status.WarmStandby.StandbyWorkloadID = standbyID
	primary.Status.WarmStandby.Phase = types.WarmStandbyPhaseCold
	primary.Status.WarmStandby.ReadyReplicas = 0
	primary.Status.WarmStandby.Message = "cold standby ready"
	return store.Update(ctx, primary)
}

// Promote scales the standby workload to active replicas for instant failover.
func (m *Manager) Promote(ctx context.Context, store state.Store, primary *types.Workload) (*types.Workload, error) {
	if !m.EnabledFor(primary) {
		return nil, fmt.Errorf("warm standby not enabled")
	}
	standbyID := standbyIDFor(primary)
	if standbyID == "" {
		return nil, fmt.Errorf("no standby workload linked")
	}
	standby, err := store.Get(ctx, standbyID)
	if err != nil || standby == nil {
		return nil, fmt.Errorf("standby workload %s not found", standbyID)
	}

	replicas := primary.Spec.Replicas
	if r := standbyReplicas(primary); r > 0 {
		if replicas < r {
			replicas = r
		}
	}
	standby.Spec = cloneSpecForStandby(primary, replicas)
	standby.Status.Phase = types.WorkloadPhasePending
	standby.UpdatedAt = time.Now().UTC()
	workloadtags.Apply(standby)
	if err := store.Update(ctx, standby); err != nil {
		return nil, err
	}

	now := time.Now().UTC()
	if primary.Status.WarmStandby == nil {
		primary.Status.WarmStandby = &types.WarmStandbyStatus{}
	}
	primary.Status.WarmStandby.Phase = types.WarmStandbyPhasePromoted
	primary.Status.WarmStandby.LastFailover = &now
	primary.Status.WarmStandby.Message = "failover promoted to standby"
	_ = store.Update(ctx, primary)

	return standby, nil
}

// ShouldAutoPromote returns true when primary circuit is open and auto_promote is set.
func (m *Manager) ShouldAutoPromote(primary *types.Workload, now time.Time) bool {
	if !m.EnabledFor(primary) {
		return false
	}
	auto := m.cfg.DefaultAutoPromote
	if primary.Spec.WarmStandby != nil {
		if primary.Spec.WarmStandby.AutoPromote {
			auto = true
		}
	}
	if !auto {
		return false
	}
	if m.circuits == nil || !m.circuits.EnabledFor(primary) {
		return primary.Status.Phase == types.WorkloadPhaseDegraded || primary.Status.Phase == types.WorkloadPhaseFailed
	}
	st := primary.Status.CircuitBreaker
	if st == nil {
		m.circuits.SyncFromWorkload(primary, now)
		st = primary.Status.CircuitBreaker
	}
	return st != nil && st.State == types.CircuitStateOpen
}

func standbyIDFor(primary *types.Workload) string {
	if primary == nil {
		return ""
	}
	if primary.Status.WarmStandby != nil && primary.Status.WarmStandby.StandbyWorkloadID != "" {
		return primary.Status.WarmStandby.StandbyWorkloadID
	}
	if primary.Spec.WarmStandby != nil && primary.Spec.WarmStandby.StandbyWorkloadID != "" {
		return primary.Spec.WarmStandby.StandbyWorkloadID
	}
	return primary.ID + "-standby"
}

func standbyReplicas(primary *types.Workload) int32 {
	if primary.Spec.WarmStandby != nil && primary.Spec.WarmStandby.Replicas > 0 {
		return primary.Spec.WarmStandby.Replicas
	}
	return 1
}

func (m *Manager) newStandbyWorkload(primary *types.Workload, id string, spec *types.WarmStandbySpec) *types.Workload {
	now := time.Now().UTC()
	labels := map[string]string{
		LabelStandbyFor: primary.ID,
		LabelRole:       RoleStandby,
	}
	for k, v := range primary.Labels {
		labels[k] = v
	}
	return &types.Workload{
		ID:        id,
		Name:      primary.Name + "-standby",
		Namespace: primary.Namespace,
		Labels:    labels,
		Tags:      primary.Tags,
		CreatedAt: now,
		UpdatedAt: now,
		Spec:      cloneSpecForStandby(primary, 0),
		Status: types.WorkloadStatus{
			Phase:          types.WorkloadPhasePending,
			Replicas:       0,
			LastTransition: now,
		},
	}
}

func cloneSpecForStandby(primary *types.Workload, replicas int32) types.WorkloadSpec {
	cp := primary.Spec
	cp.Replicas = replicas
	cp.CronSchedule = nil
	cp.WarmStandby = nil
	cp.CircuitBreaker = nil
	return cp
}
