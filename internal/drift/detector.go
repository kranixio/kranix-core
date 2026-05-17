package drift

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/kranix-io/kranix-core/internal/eventbus"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Config defines drift detection configuration.
type Config struct {
	Enabled          bool                  `json:"enabled"`
	CheckInterval    time.Duration         `json:"check_interval"`
	DefaultTolerance *types.DriftTolerance `json:"default_tolerance,omitempty"`
}

// Detector monitors workloads for drift between desired and actual state.
type Detector struct {
	config        Config
	eventBus      *eventbus.EventBus
	runtimeDriver types.RuntimeDriver
	stopCh        chan struct{}
}

// New creates a new drift detector.
func New(config Config, eventBus *eventbus.EventBus, runtimeDriver types.RuntimeDriver) *Detector {
	return &Detector{
		config:        config,
		eventBus:      eventBus,
		runtimeDriver: runtimeDriver,
		stopCh:        make(chan struct{}),
	}
}

// Start begins the drift detection loop.
func (d *Detector) Start(ctx context.Context, workloads []*types.Workload) {
	if !d.config.Enabled {
		log.Println("Drift detection disabled")
		return
	}

	log.Println("Drift detection started")
	ticker := time.NewTicker(d.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Drift detection stopped")
			return
		case <-d.stopCh:
			log.Println("Drift detection stopped")
			return
		case <-ticker.C:
			d.checkWorkloads(ctx, workloads)
		}
	}
}

// Stop halts the drift detection loop.
func (d *Detector) Stop() {
	close(d.stopCh)
}

// checkWorkloads checks all workloads for drift.
func (d *Detector) checkWorkloads(ctx context.Context, workloads []*types.Workload) {
	for _, workload := range workloads {
		if workload.Spec.DriftDetection == nil || !workload.Spec.DriftDetection.Enabled {
			continue
		}

		report, err := d.detectDrift(ctx, workload)
		if err != nil {
			log.Printf("Failed to detect drift for workload %s: %v", workload.ID, err)
			continue
		}

		if report != nil {
			d.handleDrift(ctx, workload, report)
		}
	}
}

// detectDrift compares desired spec with actual runtime state.
func (d *Detector) detectDrift(ctx context.Context, workload *types.Workload) (*types.DriftReport, error) {
	actualStatus, err := d.runtimeDriver.GetStatus(ctx, workload.ID)
	if err != nil {
		return nil, fmt.Errorf("failed to get actual status: %w", err)
	}

	config := workload.Spec.DriftDetection
	tolerance := config.Tolerance
	if tolerance == nil {
		tolerance = d.config.DefaultTolerance
	}

	report := &types.DriftReport{
		WorkloadID:    workload.ID,
		WorkloadName:  workload.Name,
		Namespace:     workload.Namespace,
		Timestamp:     time.Now(),
		DetectedAt:    time.Now(),
		DriftedFields: []types.DriftedField{},
		Severity:      types.DriftSeverityLow,
	}

	// Check replica drift
	if actualStatus.Replicas != workload.Spec.Replicas {
		if tolerance == nil || (tolerance.ReplicaVariance == 0 ||
			abs(actualStatus.Replicas-workload.Spec.Replicas) > tolerance.ReplicaVariance) {
			report.DriftedFields = append(report.DriftedFields, types.DriftedField{
				FieldPath: "spec.replicas",
				Desired:   workload.Spec.Replicas,
				Actual:    actualStatus.Replicas,
				DiffType:  "modified",
			})
		}
	}

	// Check resource drift
	// Note: Resource drift detection requires runtime driver to return actual resource usage
	// This is a placeholder for future enhancement

	// Check environment variable drift
	if config.MonitoredFields == nil || contains(config.MonitoredFields, "env") {
		if !tolerance.EnvVarDriftAllowed {
			// Compare env vars - this would require runtime driver to return actual env
			// This is a placeholder for future enhancement
		}
	}

	// Determine severity based on drifted fields
	if len(report.DriftedFields) == 0 {
		return nil, nil
	}

	report.Severity = d.calculateSeverity(report)
	report.Message = fmt.Sprintf("Detected %d drifted field(s)", len(report.DriftedFields))

	return report, nil
}

// handleDrift processes a detected drift.
func (d *Detector) handleDrift(ctx context.Context, workload *types.Workload, report *types.DriftReport) {
	config := workload.Spec.DriftDetection

	// Publish drift detected event
	d.eventBus.PublishAsync(types.Event{
		Type:      types.WorkloadDriftDetected,
		Workload:  workload,
		Timestamp: time.Now().Unix(),
		Metadata: map[string]any{
			"drift_report": report,
		},
	})

	// Send notifications if configured
	if config.AlertOnDrift && len(config.NotificationHooks) > 0 {
		d.sendNotifications(ctx, report, config.NotificationHooks)
	}

	// Auto-reconcile if enabled
	if config.AutoReconcile {
		report.AutoReconciled = true
		log.Printf("Auto-reconciling drift for workload %s", workload.ID)
		// The reconciler will handle the actual reconciliation
		d.eventBus.PublishAsync(types.Event{
			Type:      types.WorkloadDriftReconciled,
			Workload:  workload,
			Timestamp: time.Now().Unix(),
			Metadata: map[string]any{
				"drift_report": report,
			},
		})
	}
}

// sendNotifications sends drift alerts via configured hooks.
func (d *Detector) sendNotifications(ctx context.Context, report *types.DriftReport, hooks []types.NotificationHook) {
	for _, hook := range hooks {
		go func(h types.NotificationHook) {
			if err := d.sendNotification(ctx, report, h); err != nil {
				log.Printf("Failed to send notification via %s: %v", h.Type, err)
			}
		}(hook)
	}
}

// sendNotification sends a single notification.
func (d *Detector) sendNotification(ctx context.Context, report *types.DriftReport, hook types.NotificationHook) error {
	switch hook.Type {
	case "webhook":
		return d.sendWebhook(ctx, report, hook)
	case "slack":
		return d.sendSlack(ctx, report, hook)
	default:
		return fmt.Errorf("unsupported notification type: %s", hook.Type)
	}
}

// sendWebhook sends a webhook notification.
func (d *Detector) sendWebhook(ctx context.Context, report *types.DriftReport, hook types.NotificationHook) error {
	payload := map[string]interface{}{
		"workload_id":    report.WorkloadID,
		"workload_name":  report.WorkloadName,
		"namespace":      report.Namespace,
		"severity":       report.Severity,
		"drifted_fields": report.DriftedFields,
		"timestamp":      report.Timestamp,
		"message":        report.Message,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", hook.URL, bytes.NewReader(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	for k, v := range hook.Headers {
		req.Header.Set(k, v)
	}

	if hook.Secret != "" {
		req.Header.Set("Authorization", "Bearer "+hook.Secret)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send webhook: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// sendSlack sends a Slack notification.
func (d *Detector) sendSlack(ctx context.Context, report *types.DriftReport, hook types.NotificationHook) error {
	attachment := map[string]interface{}{
		"color": d.severityToColor(report.Severity),
		"title": fmt.Sprintf("Drift Detected: %s/%s", report.Namespace, report.WorkloadName),
		"text":  report.Message,
		"fields": []map[string]interface{}{
			{
				"title": "Severity",
				"value": string(report.Severity),
				"short": true,
			},
			{
				"title": "Timestamp",
				"value": report.Timestamp.Format(time.RFC3339),
				"short": true,
			},
		},
	}

	payload := map[string]interface{}{
		"attachments": []map[string]interface{}{attachment},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", hook.URL, bytes.NewReader(jsonPayload))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send slack notification: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("slack webhook returned status %d", resp.StatusCode)
	}

	return nil
}

// calculateSeverity determines the severity level of a drift.
func (d *Detector) calculateSeverity(report *types.DriftReport) types.DriftSeverity {
	for _, field := range report.DriftedFields {
		if field.FieldPath == "spec.replicas" {
			return types.DriftSeverityHigh
		}
	}

	if len(report.DriftedFields) > 3 {
		return types.DriftSeverityMedium
	}

	return types.DriftSeverityLow
}

// severityToColor maps severity to Slack color.
func (d *Detector) severityToColor(severity types.DriftSeverity) string {
	switch severity {
	case types.DriftSeverityCritical:
		return "#ff0000"
	case types.DriftSeverityHigh:
		return "#ff6600"
	case types.DriftSeverityMedium:
		return "#ffcc00"
	case types.DriftSeverityLow:
		return "#36a64f"
	default:
		return "#cccccc"
	}
}

// abs returns the absolute value of an int32.
func abs(x int32) int32 {
	if x < 0 {
		return -x
	}
	return x
}

// contains checks if a string slice contains a specific string.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
