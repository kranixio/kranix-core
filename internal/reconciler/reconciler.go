package reconciler

import (
	"context"
	"log"
	"sort"
	"time"

	"github.com/kranix-io/kranix-core/internal/autoscaler"
	"github.com/kranix-io/kranix-core/internal/circuitbreaker"
	"github.com/kranix-io/kranix-core/internal/cronsched"
	"github.com/kranix-io/kranix-core/internal/drift"
	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/internal/plugin"
	"github.com/kranix-io/kranix-core/internal/policy"
	"github.com/kranix-io/kranix-core/internal/resourcequota"
	"github.com/kranix-io/kranix-core/internal/rollout"
	"github.com/kranix-io/kranix-core/internal/scheduler"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/internal/warmstandby"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config defines reconciler configuration.
type Config struct {
	ReconcileInterval       time.Duration
	MaxConcurrentReconciles int
}

// Deps holds optional collaborators for validation, quota gates, and cron scheduling.
type Deps struct {
	Policy  *policy.Engine
	Quota   *resourcequota.Engine
	Cron    *cronsched.Evaluator // when nil, no cron gating (continuous scheduling)
	Circuit *circuitbreaker.Engine
	Standby *warmstandby.Manager
}

// Engine manages the main reconciliation loop.
type Engine struct {
	config         Config
	store          state.Store
	eventBus       *eventbus.EventBus
	scheduler      *scheduler.Scheduler
	controllerReg  *plugin.Registry
	rolloutManager *rollout.Manager
	autoscaler     *autoscaler.Engine
	driftDetector  *drift.Detector
	deps           Deps
	stopCh         chan struct{}
}

// New creates a new reconciliation engine.
func New(config Config, store state.Store, eventBus *eventbus.EventBus, sched *scheduler.Scheduler,
	controllerReg *plugin.Registry, rolloutManager *rollout.Manager, autoscaler *autoscaler.Engine,
	driftDetector *drift.Detector, deps Deps,
) *Engine {
	return &Engine{
		config:         config,
		store:          store,
		eventBus:       eventBus,
		scheduler:      sched,
		controllerReg:  controllerReg,
		rolloutManager: rolloutManager,
		autoscaler:     autoscaler,
		driftDetector:  driftDetector,
		deps:           deps,
		stopCh:         make(chan struct{}),
	}
}

// Start begins the reconciliation loop.
func (e *Engine) Start(ctx context.Context) {
	ticker := time.NewTicker(e.config.ReconcileInterval)
	defer ticker.Stop()

	// Start the auto-scaler engine
	if e.autoscaler != nil {
		go e.autoscaler.Start(ctx)
	}

	// Start the drift detector
	if e.driftDetector != nil {
		workloads, err := e.store.List(ctx, "")
		if err == nil {
			go e.driftDetector.Start(ctx, workloads)
		}
	}

	log.Println("Reconciliation engine started")

	for {
		select {
		case <-ctx.Done():
			log.Println("Reconciliation engine stopped")
			return
		case <-e.stopCh:
			log.Println("Reconciliation engine stopped")
			return
		case <-ticker.C:
			e.reconcileAll(ctx)
		}
	}
}

// Stop halts the reconciliation loop.
func (e *Engine) Stop() {
	close(e.stopCh)
}

// reconcileAll runs reconciliation for all workloads.
func (e *Engine) reconcileAll(ctx context.Context) {
	workloads, err := e.store.List(ctx, "")
	if err != nil {
		log.Printf("Failed to list workloads: %v", err)
		return
	}

	sort.SliceStable(workloads, func(i, j int) bool {
		return scheduler.WorkloadSchedulingRank(workloads[i]) > scheduler.WorkloadSchedulingRank(workloads[j])
	})

	for _, workload := range workloads {
		if err := e.reconcileOne(ctx, workload); err != nil {
			log.Printf("Failed to reconcile workload %s: %v", workload.ID, err)
		}
	}
}

// reconcileOne reconciles a single workload.
func (e *Engine) reconcileOne(ctx context.Context, workload *types.Workload) error {
	// Get controllers that should handle this workload
	controllers := e.controllerReg.GetControllersForWorkload(workload)

	// Run custom controllers
	for _, controller := range controllers {
		if err := controller.Reconcile(ctx, workload); err != nil {
			return err
		}
	}

	if e.deps.Policy != nil {
		if err := e.deps.Policy.Validate(workload); err != nil {
			log.Printf("Policy rejected workload %s: %v", workload.ID, err)
			return nil
		}
	}
	if e.deps.Quota != nil {
		if err := e.deps.Quota.Enforce(ctx, workload); err != nil {
			log.Printf("Hard resource quota rejected workload %s: %v", workload.ID, err)
			return nil
		}
	}

	now := time.Now()
	if e.deps.Circuit != nil {
		prev, cur := e.deps.Circuit.SyncFromWorkload(workload, now)
		if err := e.store.Update(ctx, workload); err != nil {
			log.Printf("Failed to persist circuit status for %s: %v", workload.ID, err)
		}
		if prev != cur && cur == types.CircuitStateOpen {
			e.eventBus.PublishAsync(types.Event{Type: types.WorkloadCircuitOpen, Workload: workload})
		}
		if prev != cur && cur == types.CircuitStateClosed {
			e.eventBus.PublishAsync(types.Event{Type: types.WorkloadCircuitClosed, Workload: workload})
		}
		if e.deps.Standby != nil && e.deps.Standby.ShouldAutoPromote(workload, now) {
			if sw, err := e.deps.Standby.Promote(ctx, e.store, workload); err != nil {
				log.Printf("Warm standby promote failed for %s: %v", workload.ID, err)
			} else if sw != nil {
				e.eventBus.PublishAsync(types.Event{Type: types.WorkloadStandbyPromoted, Workload: workload})
				if err := e.scheduler.Schedule(ctx, sw); err != nil {
					log.Printf("Failed to schedule promoted standby %s: %v", sw.ID, err)
				}
			}
		}
	}

	if e.deps.Standby != nil && e.deps.Standby.EnabledFor(workload) &&
		workload.Status.Phase == types.WorkloadPhaseRunning {
		if err := e.deps.Standby.EnsureColdStandby(ctx, e.store, workload); err != nil {
			log.Printf("Ensure cold standby failed for %s: %v", workload.ID, err)
		}
	}

	cronTriggers := false
	evaluator := e.deps.Cron
	if evaluator == nil {
		evaluator = &cronsched.Evaluator{}
	}
	if workload.Spec.CronSchedule != nil && !workload.Spec.CronSchedule.Suspended {
		due, err := evaluator.ShouldTriggerDeploy(workload, time.Now())
		if err != nil {
			log.Printf("Cron evaluation failed for workload %s: %v", workload.ID, err)
			return err
		}
		if !due {
			return nil
		}
		cronTriggers = true
	}

	postSchedule := func() error {
		now := time.Now()
		workload.Status.Phase = types.WorkloadPhaseRunning
		workload.Status.LastTransition = now
		if cronTriggers && workload.Spec.CronSchedule != nil {
			if workload.Status.Cron == nil {
				workload.Status.Cron = &types.CronScheduleStatus{}
			}
			t := now
			workload.Status.Cron.LastScheduleTime = &t
		}
		if err := e.store.Update(ctx, workload); err != nil {
			return err
		}
		if cronTriggers && workload.Spec.CronSchedule != nil {
			e.eventBus.PublishAsync(types.Event{
				Type:     types.WorkloadCronTriggered,
				Workload: workload,
			})
		}
		e.eventBus.PublishAsync(types.Event{
			Type:     types.WorkloadScheduled,
			Workload: workload,
		})
		return nil
	}

	if e.deps.Circuit != nil && !e.deps.Circuit.AllowRoute(workload, now) {
		log.Printf("Circuit breaker open: skipping route for workload %s", workload.ID)
		return nil
	}
	if e.deps.Circuit != nil {
		e.deps.Circuit.RecordRouteAttempt(workload)
	}

	// Execute rollout strategy if needed
	if workload.Status.Phase == types.WorkloadPhasePending && e.rolloutManager != nil {
		if err := e.rolloutManager.ExecuteRollout(ctx, workload); err != nil {
			log.Printf("Rollout failed for workload %s: %v", workload.ID, err)
			return err
		}
		return postSchedule()
	} else if workload.Status.Phase == types.WorkloadPhasePending {
		if err := e.scheduler.Schedule(ctx, workload); err != nil {
			return err
		}
		return postSchedule()
	}

	return nil
}
