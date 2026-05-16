package prediction

import (
	"context"
	"fmt"
	"log"
	"math"
	"time"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// Predictor handles ML-based failure prediction.
type Predictor struct {
	model Model
}

// Model defines the interface for failure prediction models.
type Model interface {
	Predict(features []float64) (float64, error)
	Train(data []TrainingData) error
}

// New creates a new failure predictor.
func New(model Model) *Predictor {
	return &Predictor{
		model: model,
	}
}

// TrainingData represents historical data for training the model.
type TrainingData struct {
	Features    []float64 // cpu_usage, memory_usage, request_rate, error_rate, etc.
	Failed      bool      // whether the workload failed
	FailureType string    // oom, crash, timeout, etc.
	Timestamp   time.Time
}

// PredictionResult represents the result of a failure prediction.
type PredictionResult struct {
	WorkloadID       string
	FailureProb      float64
	Predicted        bool
	MitigationAction string
	Features         map[string]float64
}

// PredictFailure predicts if a workload is likely to fail.
func (p *Predictor) PredictFailure(ctx context.Context, workload *types.Workload) (*PredictionResult, error) {
	if workload.Spec.FailurePrediction == nil || !workload.Spec.FailurePrediction.Enabled {
		return &PredictionResult{
			WorkloadID:  workload.ID,
			FailureProb: 0,
			Predicted:   false,
		}, nil
	}

	// Extract features from workload status
	features, err := p.extractFeatures(workload)
	if err != nil {
		return nil, fmt.Errorf("failed to extract features: %w", err)
	}

	// Get feature values in order
	featureValues := p.getFeatureValues(workload.Spec.FailurePrediction.Features, features)

	// Run prediction
	prob, err := p.model.Predict(featureValues)
	if err != nil {
		return nil, fmt.Errorf("prediction failed: %w", err)
	}

	// Determine if mitigation is needed
	threshold := workload.Spec.FailurePrediction.Threshold
	if threshold == 0 {
		threshold = 0.7 // default threshold
	}

	result := &PredictionResult{
		WorkloadID:  workload.ID,
		FailureProb: prob,
		Predicted:   prob >= threshold,
		Features:    features,
	}

	if result.Predicted {
		result.MitigationAction = p.selectMitigationAction(workload.Spec.FailurePrediction.MitigationActions)
		log.Printf("Failure predicted for workload %s (prob: %.2f), mitigation: %s",
			workload.ID, prob, result.MitigationAction)
	}

	return result, nil
}

// extractFeatures extracts features from workload status.
func (p *Predictor) extractFeatures(workload *types.Workload) (map[string]float64, error) {
	features := make(map[string]float64)

	// CPU usage (simulated - in production would come from metrics)
	features["cpu_usage"] = p.simulateCPUUsage(workload)

	// Memory usage (simulated)
	features["memory_usage"] = p.simulateMemoryUsage(workload)

	// Request rate (simulated)
	features["request_rate"] = p.simulateRequestRate(workload)

	// Error rate (simulated)
	features["error_rate"] = p.simulateErrorRate(workload)

	// Restart count
	features["restart_count"] = float64(workload.Status.AvailableReplicas - workload.Status.ReadyReplicas)
	if features["restart_count"] < 0 {
		features["restart_count"] = 0
	}

	// Time since last transition
	features["time_since_transition"] = time.Since(workload.Status.LastTransition).Seconds()

	return features, nil
}

// getFeatureValues returns feature values in the order specified by the config.
func (p *Predictor) getFeatureValues(featureNames []string, features map[string]float64) []float64 {
	if len(featureNames) == 0 {
		// Default feature order
		return []float64{
			features["cpu_usage"],
			features["memory_usage"],
			features["request_rate"],
			features["error_rate"],
		}
	}

	values := make([]float64, len(featureNames))
	for i, name := range featureNames {
		if val, exists := features[name]; exists {
			values[i] = val
		} else {
			values[i] = 0
		}
	}
	return values
}

// selectMitigationAction selects the best mitigation action based on the prediction.
func (p *Predictor) selectMitigationAction(actions []string) string {
	if len(actions) == 0 {
		return "scale_up" // default action
	}
	return actions[0]
}

// Train trains the model with historical data.
func (p *Predictor) Train(ctx context.Context, data []TrainingData) error {
	if err := p.model.Train(data); err != nil {
		return fmt.Errorf("failed to train model: %w", err)
	}
	log.Printf("Model trained with %d data points", len(data))
	return nil
}

// Simulated feature extraction methods (in production, these would query actual metrics)
func (p *Predictor) simulateCPUUsage(workload *types.Workload) float64 {
	// Simulate CPU usage based on resource limits and replica count
	if workload.Spec.Resources.CPULimit == "" {
		return 0.5 // default 50%
	}
	return 0.6 // simulated value
}

func (p *Predictor) simulateMemoryUsage(workload *types.Workload) float64 {
	// Simulate memory usage
	if workload.Spec.Resources.MemoryLimit == "" {
		return 0.5 // default 50%
	}
	return 0.7 // simulated value
}

func (p *Predictor) simulateRequestRate(workload *types.Workload) float64 {
	// Simulate request rate (requests per second)
	_ = workload // avoid unused parameter warning
	return 100.0
}

func (p *Predictor) simulateErrorRate(workload *types.Workload) float64 {
	// Simulate error rate (percentage)
	if workload.Status.Phase == types.WorkloadPhaseDegraded {
		return 5.0
	}
	if workload.Status.Phase == types.WorkloadPhaseFailed {
		return 10.0
	}
	return 0.1
}

// SimpleModel is a simple heuristic-based model for failure prediction.
type SimpleModel struct{}

// NewSimpleModel creates a new simple model.
func NewSimpleModel() Model {
	return &SimpleModel{}
}

// Predict performs a simple heuristic-based prediction.
func (m *SimpleModel) Predict(features []float64) (float64, error) {
	if len(features) < 4 {
		return 0, fmt.Errorf("need at least 4 features")
	}

	cpuUsage := features[0]
	memoryUsage := features[1]
	requestRate := features[2]
	errorRate := features[3]

	// Simple heuristic: high CPU/memory usage or high error rate increases failure probability
	prob := 0.0

	// CPU usage factor
	if cpuUsage > 0.9 {
		prob += 0.4
	} else if cpuUsage > 0.8 {
		prob += 0.2
	}

	// Memory usage factor
	if memoryUsage > 0.9 {
		prob += 0.4
	} else if memoryUsage > 0.8 {
		prob += 0.2
	}

	// Request rate factor (high request rate can indicate stress)
	if requestRate > 1000.0 {
		prob += 0.1
	}

	// Error rate factor
	if errorRate > 5.0 {
		prob += 0.3
	} else if errorRate > 1.0 {
		prob += 0.1
	}

	// Cap at 1.0
	if prob > 1.0 {
		prob = 1.0
	}

	return prob, nil
}

// Train is a no-op for the simple model.
func (m *SimpleModel) Train(data []TrainingData) error {
	// Simple model doesn't need training
	return nil
}

// HistoricalDataCollector collects historical data for training.
type HistoricalDataCollector struct {
	data []TrainingData
}

// NewHistoricalDataCollector creates a new data collector.
func NewHistoricalDataCollector() *HistoricalDataCollector {
	return &HistoricalDataCollector{
		data: make([]TrainingData, 0),
	}
}

// Collect collects data from a workload.
func (c *HistoricalDataCollector) Collect(workload *types.Workload, failed bool, failureType string) {
	// Simulated feature extraction
	features := []float64{
		0.6,   // cpu_usage
		0.7,   // memory_usage
		100.0, // request_rate
		0.1,   // error_rate
	}

	c.data = append(c.data, TrainingData{
		Features:    features,
		Failed:      failed,
		FailureType: failureType,
		Timestamp:   time.Now(),
	})
}

// GetData returns the collected training data.
func (c *HistoricalDataCollector) GetData() []TrainingData {
	return c.data
}

// MockMLModel is a mock ML model for testing.
type MockMLModel struct {
	weights []float64
	bias    float64
}

// NewMockMLModel creates a new mock ML model.
func NewMockMLModel() Model {
	return &MockMLModel{
		weights: []float64{0.3, 0.3, 0.2, 0.2}, // weights for cpu, memory, request_rate, error_rate
		bias:    0.1,
	}
}

// Predict performs a weighted sum prediction.
func (m *MockMLModel) Predict(features []float64) (float64, error) {
	if len(features) != len(m.weights) {
		return 0, fmt.Errorf("feature count mismatch")
	}

	sum := m.bias
	for i, feature := range features {
		sum += feature * m.weights[i]
	}

	// Apply sigmoid function
	prob := 1.0 / (1.0 + math.Exp(-sum))
	return prob, nil
}

// Train updates the model weights based on training data.
func (m *MockMLModel) Train(data []TrainingData) error {
	// Simple gradient descent simulation
	learningRate := 0.01
	epochs := 10

	for epoch := 0; epoch < epochs; epoch++ {
		for _, d := range data {
			pred, _ := m.Predict(d.Features)
			target := 0.0
			if d.Failed {
				target = 1.0
			}

			// Update weights
			error := target - pred
			for i := range m.weights {
				m.weights[i] += learningRate * error * d.Features[i]
			}
			m.bias += learningRate * error
		}
	}

	return nil
}
