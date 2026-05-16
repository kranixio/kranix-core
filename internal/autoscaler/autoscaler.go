package autoscaler

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Engine handles auto-scaling decisions for workloads.
type Engine struct {
	store          state.Store
	metricsProvider MetricsProvider
	checkInterval  time.Duration
}

// MetricsProvider defines the interface for fetching metrics.
type MetricsProvider interface {
	GetCPUUtilization(ctx context.Context, workloadID string) (float64, error)
	GetMemoryUtilization(ctx context.Context, workloadID string) (float64, error)
	GetCustomMetric(ctx context.Context, workloadID string, metricName string) (float64, error)
}

// Config defines auto-scaling engine configuration.
type Config struct {
	CheckInterval time.Duration
}

// New creates a new auto-scaling engine.
func New(config Config, store state.Store, metricsProvider MetricsProvider) *Engine {
	return &Engine{
		store:          store,
		metricsProvider: metricsProvider,
		checkInterval:  config.CheckInterval,
	}
}

// Start begins the auto-scaling loop.
func (e *Engine) Start(ctx context.Context) {
	ticker := time.NewTicker(e.checkInterval)
	defer ticker.Stop()

	log.Println("Auto-scaling engine started")

	for {
		select {
		case <-ctx.Done():
			log.Println("Auto-scaling engine stopped")
			return
		case <-ticker.C:
			e.checkAllWorkloads(ctx)
		}
	}
}

// checkAllWorkloads evaluates scaling decisions for all workloads.
func (e *Engine) checkAllWorkloads(ctx context.Context) {
	workloads, err := e.store.List(ctx, "")
	if err != nil {
		log.Printf("Failed to list workloads for auto-scaling: %v", err)
		return
	}

	for _, workload := range workloads {
		if workload.Spec.AutoScaling == nil || !workload.Spec.AutoScaling.Enabled {
			continue
		}

		if err := e.evaluateScaling(ctx, workload); err != nil {
			log.Printf("Failed to evaluate scaling for workload %s: %v", workload.ID, err)
		}
	}
}

// evaluateScaling determines if a workload needs to be scaled.
func (e *Engine) evaluateScaling(ctx context.Context, workload *types.Workload) error {
	config := workload.Spec.AutoScaling
	currentReplicas := workload.Spec.Replicas

	// Check CPU utilization
	if config.TargetCPUUtilization > 0 {
		cpuUtil, err := e.metricsProvider.GetCPUUtilization(ctx, workload.ID)
		if err != nil {
			return fmt.Errorf("failed to get CPU utilization: %w", err)
		}

		if cpuUtil > float64(config.TargetCPUUtilization) {
			// Scale up
			newReplicas := min(config.MaxReplicas, currentReplicas+1)
			if newReplicas > currentReplicas {
				return e.scaleWorkload(ctx, workload, newReplicas, "CPU utilization above target")
			}
		} else if cpuUtil < float64(config.TargetCPUUtilization)*0.5 {
			// Scale down (if below 50% of target)
			newReplicas := max(config.MinReplicas, currentReplicas-1)
			if newReplicas < currentReplicas {
				return e.scaleWorkload(ctx, workload, newReplicas, "CPU utilization below target")
			}
		}
	}

	// Check memory utilization
	if config.TargetMemoryUtilization > 0 {
		memUtil, err := e.metricsProvider.GetMemoryUtilization(ctx, workload.ID)
		if err != nil {
			return fmt.Errorf("failed to get memory utilization: %w", err)
		}

		if memUtil > float64(config.TargetMemoryUtilization) {
			// Scale up
			newReplicas := min(config.MaxReplicas, currentReplicas+1)
			if newReplicas > currentReplicas {
				return e.scaleWorkload(ctx, workload, newReplicas, "Memory utilization above target")
			}
		} else if memUtil < float64(config.TargetMemoryUtilization)*0.5 {
			// Scale down
			newReplicas := max(config.MinReplicas, currentReplicas-1)
			if newReplicas < currentReplicas {
				return e.scaleWorkload(ctx, workload, newReplicas, "Memory utilization below target")
			}
		}
	}

	// Check custom metrics
	for _, metric := range config.CustomMetrics {
		metricValue, err := e.metricsProvider.GetCustomMetric(ctx, workload.ID, metric.MetricName)
		if err != nil {
			log.Printf("Failed to get custom metric %s for workload %s: %v", metric.MetricName, workload.ID, err)
			continue
		}

		// Simple threshold-based scaling for custom metrics
		// In production, this would be more sophisticated
		if metricValue > 100.0 { // Example threshold
			newReplicas := min(config.MaxReplicas, currentReplicas+1)
			if newReplicas > currentReplicas {
				return e.scaleWorkload(ctx, workload, newReplicas, fmt.Sprintf("Custom metric %s above threshold", metric.Name))
			}
		}
	}

	return nil
}

// scaleWorkload updates the replica count for a workload.
func (e *Engine) scaleWorkload(ctx context.Context, workload *types.Workload, newReplicas int32, reason string) error {
	oldReplicas := workload.Spec.Replicas
	workload.Spec.Replicas = newReplicas
	workload.UpdatedAt = time.Now()

	if err := e.store.Update(ctx, workload); err != nil {
		return fmt.Errorf("failed to update workload replicas: %w", err)
	}

	log.Printf("Scaled workload %s from %d to %d replicas: %s", workload.ID, oldReplicas, newReplicas, reason)

	// Record state transition
	workload.History = append(workload.History, types.WorkloadStateTransition{
		FromPhase: workload.Status.Phase,
		ToPhase:   workload.Status.Phase,
		Timestamp: time.Now(),
		Reason:    "AutoScaling",
		Message:   fmt.Sprintf("Scaled from %d to %d replicas: %s", oldReplicas, newReplicas, reason),
	})

	return nil
}

// DefaultMetricsProvider provides a default implementation of MetricsProvider.
type DefaultMetricsProvider struct{}

// NewDefaultMetricsProvider creates a new default metrics provider.
func NewDefaultMetricsProvider() *DefaultMetricsProvider {
	return &DefaultMetricsProvider{}
}

// GetCPUUtilization returns mock CPU utilization.
func (p *DefaultMetricsProvider) GetCPUUtilization(ctx context.Context, workloadID string) (float64, error) {
	// In production, this would query a metrics system like Prometheus
	// For now, return a mock value
	return 50.0, nil
}

// GetMemoryUtilization returns mock memory utilization.
func (p *DefaultMetricsProvider) GetMemoryUtilization(ctx context.Context, workloadID string) (float64, error) {
	// In production, this would query a metrics system like Prometheus
	// For now, return a mock value
	return 50.0, nil
}

// GetCustomMetric returns mock custom metric value.
func (p *DefaultMetricsProvider) GetCustomMetric(ctx context.Context, workloadID string, metricName string) (float64, error) {
	// In production, this would query a metrics system like Prometheus
	// For now, return a mock value
	return 50.0, nil
}

func min(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

func max(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}
