package reconciler

import (
	"context"
	"log"
	"time"

	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/internal/plugin"
	"github.com/kranix-io/kranix-core/internal/scheduler"
	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config defines reconciler configuration.
type Config struct {
	ReconcileInterval      time.Duration
	MaxConcurrentReconciles int
}

// Engine manages the main reconciliation loop.
type Engine struct {
	config         Config
	store          state.Store
	eventBus       *eventbus.EventBus
	scheduler      *scheduler.Scheduler
	controllerReg  *plugin.Registry
	stopCh         chan struct{}
}

// New creates a new reconciliation engine.
func New(config Config, store state.Store, eventBus *eventbus.EventBus, scheduler *scheduler.Scheduler, controllerReg *plugin.Registry) *Engine {
	return &Engine{
		config:        config,
		store:         store,
		eventBus:      eventBus,
		scheduler:     scheduler,
		controllerReg: controllerReg,
		stopCh:        make(chan struct{}),
	}
}

// Start begins the reconciliation loop.
func (e *Engine) Start(ctx context.Context) {
	ticker := time.NewTicker(e.config.ReconcileInterval)
	defer ticker.Stop()

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

	// Schedule the workload if needed
	if workload.Status.Phase == types.WorkloadPhasePending {
		if err := e.scheduler.Schedule(ctx, workload); err != nil {
			return err
		}

		// Update status to scheduled
		workload.Status.Phase = types.WorkloadPhaseRunning
		workload.Status.LastTransition = time.Now()
		if err := e.store.Update(ctx, workload); err != nil {
			return err
		}

		// Publish event
		e.eventBus.PublishAsync(types.Event{
			Type:     types.WorkloadScheduled,
			Workload: workload,
		})
	}

	return nil
}
