package healthgate

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config defines health gate configuration.
type Config struct {
	Enabled         bool          `json:"enabled"`
	DefaultTimeout  time.Duration `json:"default_timeout"`
	CheckInterval   time.Duration `json:"check_interval"`
}

// Engine manages health checks for workloads.
type Engine struct {
	config   Config
	eventBus *eventbus.EventBus
	stopCh   chan struct{}
}

// New creates a new health gate engine.
func New(config Config, eventBus *eventbus.EventBus) *Engine {
	return &Engine{
		config:   config,
		eventBus: eventBus,
		stopCh:   make(chan struct{}),
	}
}

// EvaluateHealthGates evaluates health gates for a workload before rollout.
func (e *Engine) EvaluateHealthGates(ctx context.Context, workload *types.Workload) (*types.HealthGateStatus, error) {
	if workload.Spec.HealthGate == nil || !workload.Spec.HealthGate.Enabled {
		return &types.HealthGateStatus{
			WorkloadID:    workload.ID,
			OverallStatus: "passing",
			Results:       []types.HealthCheckResult{},
			LastEvaluated: time.Now(),
			Blocked:       false,
		}, nil
	}

	config := workload.Spec.HealthGate
	status := &types.HealthGateStatus{
		WorkloadID:    workload.ID,
		OverallStatus: "passing",
		Results:       make([]types.HealthCheckResult, 0, len(config.Checks)),
		LastEvaluated: time.Now(),
		Blocked:       false,
	}

	timeout := e.config.DefaultTimeout
	if config.Timeout != "" {
		var err error
		timeout, err = time.ParseDuration(config.Timeout)
		if err != nil {
			log.Printf("Invalid timeout %s, using default: %v", config.Timeout, err)
		}
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for _, check := range config.Checks {
		result := e.executeCheck(ctx, check)
		status.Results = append(status.Results, result)

		if result.Status == "failing" {
			status.OverallStatus = "failing"
			if config.FailureMode == "block" {
				status.Blocked = true
				status.BlockReason = fmt.Sprintf("Health check '%s' failed: %s", check.Name, result.Message)
			}
		}
	}

	// Publish health gate status event
	if status.Blocked {
		e.eventBus.PublishAsync(types.Event{
			Type:     types.HealthGateBlocked,
			Workload: workload,
			Timestamp: time.Now().Unix(),
			Metadata: map[string]any{
				"health_gate_status": status,
			},
		})
	} else {
		e.eventBus.PublishAsync(types.Event{
			Type:     types.HealthGatePassed,
			Workload: workload,
			Timestamp: time.Now().Unix(),
			Metadata: map[string]any{
				"health_gate_status": status,
			},
		})
	}

	return status, nil
}

// executeCheck executes a single health check.
func (e *Engine) executeCheck(ctx context.Context, check types.HealthCheck) types.HealthCheckResult {
	startTime := time.Now()
	result := types.HealthCheckResult{
		CheckName:   check.Name,
		Status:      "unknown",
		LastChecked: time.Now(),
		Metadata:    make(map[string]string),
	}

	switch check.Type {
	case "http":
		result = e.executeHTTPCheck(ctx, check, startTime)
	case "tcp":
		result = e.executeTCPCheck(ctx, check, startTime)
	case "command":
		result = e.executeCommandCheck(ctx, check, startTime)
	case "prometheus":
		result = e.executePrometheusCheck(ctx, check, startTime)
	default:
		result.Status = "unknown"
		result.Message = fmt.Sprintf("Unknown check type: %s", check.Type)
	}

	result.Duration = time.Since(startTime)

	// Publish individual check result event
	if result.Status == "passing" {
		e.eventBus.PublishAsync(types.Event{
			Type:     types.HealthCheckPassed,
			Timestamp: time.Now().Unix(),
			Metadata: map[string]any{
				"check_name": check.Name,
				"result":     result,
			},
		})
	} else if result.Status == "failing" {
		e.eventBus.PublishAsync(types.Event{
			Type:     types.HealthCheckFailed,
			Timestamp: time.Now().Unix(),
			Metadata: map[string]any{
				"check_name": check.Name,
				"result":     result,
			},
		})
	}

	return result
}

// executeHTTPCheck executes an HTTP health check.
func (e *Engine) executeHTTPCheck(ctx context.Context, check types.HealthCheck, startTime time.Time) types.HealthCheckResult {
	url := check.Config["url"]
	method := check.Config["method"]
	if method == "" {
		method = "GET"
	}

	expectedStatus := check.Config["expected_status"]
	if expectedStatus == "" {
		expectedStatus = "200"
	}

	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return types.HealthCheckResult{
			CheckName:   check.Name,
			Status:      "failing",
			Message:     fmt.Sprintf("Failed to create request: %v", err),
			LastChecked: startTime,
			Duration:    time.Since(startTime),
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return types.HealthCheckResult{
			CheckName:   check.Name,
			Status:      "failing",
			Message:     fmt.Sprintf("Request failed: %v", err),
			LastChecked: startTime,
			Duration:    time.Since(startTime),
		}
	}
	defer resp.Body.Close()

	if fmt.Sprintf("%d", resp.StatusCode) != expectedStatus {
		return types.HealthCheckResult{
			CheckName:   check.Name,
			Status:      "failing",
			Message:     fmt.Sprintf("Expected status %s, got %d", expectedStatus, resp.StatusCode),
			LastChecked: startTime,
			Duration:    time.Since(startTime),
			Metadata: map[string]string{
				"status_code": fmt.Sprintf("%d", resp.StatusCode),
			},
		}
	}

	return types.HealthCheckResult{
		CheckName:   check.Name,
		Status:      "passing",
		Message:     "HTTP check passed",
		LastChecked: startTime,
		Duration:    time.Since(startTime),
		Metadata: map[string]string{
			"status_code": fmt.Sprintf("%d", resp.StatusCode),
		},
	}
}

// executeTCPCheck executes a TCP health check.
func (e *Engine) executeTCPCheck(ctx context.Context, check types.HealthCheck, startTime time.Time) types.HealthCheckResult {
	host := check.Config["host"]
	port := check.Config["port"]
	address := fmt.Sprintf("%s:%s", host, port)

	// TODO: Implement TCP dial check
	return types.HealthCheckResult{
		CheckName:   check.Name,
		Status:      "unknown",
		Message:     "TCP check not yet implemented",
		LastChecked: startTime,
		Duration:    time.Since(startTime),
		Metadata: map[string]string{
			"address": address,
		},
	}
}

// executeCommandCheck executes a command health check.
func (e *Engine) executeCommandCheck(ctx context.Context, check types.HealthCheck, startTime time.Time) types.HealthCheckResult {
	command := check.Config["command"]

	// TODO: Implement command execution check
	return types.HealthCheckResult{
		CheckName:   check.Name,
		Status:      "unknown",
		Message:     "Command check not yet implemented",
		LastChecked: startTime,
		Duration:    time.Since(startTime),
		Metadata: map[string]string{
			"command": command,
		},
	}
}

// executePrometheusCheck executes a Prometheus query health check.
func (e *Engine) executePrometheusCheck(ctx context.Context, check types.HealthCheck, startTime time.Time) types.HealthCheckResult {
	query := check.Config["query"]
	prometheusURL := check.Config["prometheus_url"]

	// TODO: Implement Prometheus query check
	return types.HealthCheckResult{
		CheckName:   check.Name,
		Status:      "unknown",
		Message:     "Prometheus check not yet implemented",
		LastChecked: startTime,
		Duration:    time.Since(startTime),
		Metadata: map[string]string{
			"query":          query,
			"prometheus_url": prometheusURL,
		},
	}
}

// Start begins continuous health check monitoring.
func (e *Engine) Start(ctx context.Context) {
	if !e.config.Enabled {
		log.Println("Health gate engine disabled")
		return
	}

	log.Println("Health gate engine started")
	ticker := time.NewTicker(e.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Health gate engine stopped")
			return
		case <-e.stopCh:
			log.Println("Health gate engine stopped")
			return
		case <-ticker.C:
			// Continuous monitoring would be implemented here
			// For now, health gates are evaluated on-demand before rollouts
		}
	}
}

// Stop halts the health gate engine.
func (e *Engine) Stop() {
	close(e.stopCh)
}
