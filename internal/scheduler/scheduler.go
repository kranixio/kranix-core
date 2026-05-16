package scheduler

import (
	"context"
	"fmt"
	"log"

	"github.com/kranix-io/kranix-core/pkg/types"
)

// Scheduler handles workload placement and coordination.
type Scheduler struct {
	runtimeDrivers map[string]types.RuntimeDriver
	costProvider   CostProvider
	nodeRegistry   NodeRegistry
}

// CostProvider defines the interface for querying node/region costs.
type CostProvider interface {
	GetNodeCost(ctx context.Context, nodeID string) (float64, error)
	GetRegionCost(ctx context.Context, region string) (float64, error)
	GetZoneCost(ctx context.Context, region, zone string) (float64, error)
}

// NodeRegistry defines the interface for querying node information.
type NodeRegistry interface {
	ListNodes(ctx context.Context) ([]NodeInfo, error)
	GetNodesByRegion(ctx context.Context, region string) ([]NodeInfo, error)
	GetNodesByZone(ctx context.Context, region, zone string) ([]NodeInfo, error)
}

// NodeInfo contains information about a node.
type NodeInfo struct {
	ID       string
	Region   string
	Zone     string
	CPU      int64
	Memory   int64
	Labels   map[string]string
	Capacity ResourceCapacity
}

// ResourceCapacity represents node resource capacity.
type ResourceCapacity struct {
	CPU    int64
	Memory int64
}

// New creates a new scheduler.
func New(costProvider CostProvider, nodeRegistry NodeRegistry) *Scheduler {
	return &Scheduler{
		runtimeDrivers: make(map[string]types.RuntimeDriver),
		costProvider:   costProvider,
		nodeRegistry:   nodeRegistry,
	}
}

// RegisterDriver adds a runtime driver to the scheduler.
func (s *Scheduler) RegisterDriver(driver types.RuntimeDriver) {
	s.runtimeDrivers[driver.Name()] = driver
}

// Schedule assigns a workload to an appropriate runtime backend.
func (s *Scheduler) Schedule(ctx context.Context, workload *types.Workload) error {
	driver, exists := s.runtimeDrivers[workload.Spec.Backend]
	if !exists {
		return fmt.Errorf("no runtime driver found for backend: %s", workload.Spec.Backend)
	}

	// Apply cost-aware scheduling if enabled
	if workload.Spec.Scheduling != nil && workload.Spec.Scheduling.CostAware {
		if err := s.applyCostAwareScheduling(ctx, workload); err != nil {
			return fmt.Errorf("cost-aware scheduling failed: %w", err)
		}
	}

	// Deploy the workload using the appropriate driver
	if err := driver.Deploy(ctx, workload); err != nil {
		return fmt.Errorf("failed to deploy workload: %w", err)
	}

	return nil
}

// applyCostAwareScheduling selects the cheapest node/region for the workload.
func (s *Scheduler) applyCostAwareScheduling(ctx context.Context, workload *types.Workload) error {
	scheduling := workload.Spec.Scheduling

	// Get available nodes
	nodes, err := s.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to list nodes: %w", err)
	}

	// Filter nodes based on preferences
	candidateNodes := s.filterNodes(nodes, scheduling)

	if len(candidateNodes) == 0 {
		log.Printf("No nodes match scheduling preferences, using all available nodes")
		candidateNodes = nodes
	}

	// Select the cheapest node
	selectedNode, err := s.selectCheapestNode(ctx, candidateNodes, scheduling)
	if err != nil {
		return fmt.Errorf("failed to select cheapest node: %w", err)
	}

	// Apply node selector to workload
	if workload.Spec.Scheduling.NodeSelectors == nil {
		workload.Spec.Scheduling.NodeSelectors = make(map[string]string)
	}
	workload.Spec.Scheduling.NodeSelectors["kubernetes.io/hostname"] = selectedNode.ID

	log.Printf("Cost-aware scheduling: selected node %s in region %s, zone %s for workload %s",
		selectedNode.ID, selectedNode.Region, selectedNode.Zone, workload.ID)

	return nil
}

// filterNodes filters nodes based on scheduling preferences.
func (s *Scheduler) filterNodes(nodes []NodeInfo, scheduling *types.SchedulingConfig) []NodeInfo {
	var filtered []NodeInfo

	for _, node := range nodes {
		// Filter by preferred regions
		if len(scheduling.PreferredRegions) > 0 {
			if !contains(scheduling.PreferredRegions, node.Region) {
				continue
			}
		}

		// Filter by preferred zones
		if len(scheduling.PreferredZones) > 0 {
			if !contains(scheduling.PreferredZones, node.Zone) {
				continue
			}
		}

		// Filter by node selectors
		if len(scheduling.NodeSelectors) > 0 {
			if !matchesNodeSelectors(node, scheduling.NodeSelectors) {
				continue
			}
		}

		filtered = append(filtered, node)
	}

	return filtered
}

// selectCheapestNode selects the cheapest node from candidates.
func (s *Scheduler) selectCheapestNode(ctx context.Context, nodes []NodeInfo, scheduling *types.SchedulingConfig) (*NodeInfo, error) {
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes available")
	}

	var cheapestNode *NodeInfo
	minCost := float64(-1)

	for _, node := range nodes {
		cost, err := s.costProvider.GetNodeCost(ctx, node.ID)
		if err != nil {
			log.Printf("Failed to get cost for node %s: %v", node.ID, err)
			continue
		}

		// Check if cost exceeds max cost per hour
		if scheduling.MaxCostPerHour != "" {
			maxCost, err := parseCost(scheduling.MaxCostPerHour)
			if err != nil {
				log.Printf("Failed to parse max cost: %v", err)
			} else if cost > maxCost {
				continue
			}
		}

		if minCost < 0 || cost < minCost {
			minCost = cost
			cheapestNode = &node
		}
	}

	if cheapestNode == nil {
		return nil, fmt.Errorf("no suitable node found within cost constraints")
	}

	return cheapestNode, nil
}

// GetDriver returns a runtime driver by name.
func (s *Scheduler) GetDriver(name string) (types.RuntimeDriver, bool) {
	driver, exists := s.runtimeDrivers[name]
	return driver, exists
}

// DefaultCostProvider provides a default implementation of CostProvider.
type DefaultCostProvider struct{}

// NewDefaultCostProvider creates a new default cost provider.
func NewDefaultCostProvider() *DefaultCostProvider {
	return &DefaultCostProvider{}
}

// GetNodeCost returns mock node cost.
func (p *DefaultCostProvider) GetNodeCost(ctx context.Context, nodeID string) (float64, error) {
	// In production, this would query a cloud provider's pricing API
	// For now, return a mock cost
	return 0.10, nil // $0.10 per hour
}

// GetRegionCost returns mock region cost.
func (p *DefaultCostProvider) GetRegionCost(ctx context.Context, region string) (float64, error) {
	// In production, this would query a cloud provider's pricing API
	// For now, return a mock cost
	return 0.10, nil // $0.10 per hour
}

// GetZoneCost returns mock zone cost.
func (p *DefaultCostProvider) GetZoneCost(ctx context.Context, region, zone string) (float64, error) {
	// In production, this would query a cloud provider's pricing API
	// For now, return a mock cost
	return 0.10, nil // $0.10 per hour
}

// DefaultNodeRegistry provides a default implementation of NodeRegistry.
type DefaultNodeRegistry struct{}

// NewDefaultNodeRegistry creates a new default node registry.
func NewDefaultNodeRegistry() *DefaultNodeRegistry {
	return &DefaultNodeRegistry{}
}

// ListNodes returns mock node list.
func (r *DefaultNodeRegistry) ListNodes(ctx context.Context) ([]NodeInfo, error) {
	// In production, this would query the Kubernetes API
	// For now, return mock nodes
	return []NodeInfo{
		{
			ID:     "node-1",
			Region: "us-east-1",
			Zone:   "us-east-1a",
			CPU:    4000,
			Memory: 16384,
			Labels: map[string]string{},
			Capacity: ResourceCapacity{
				CPU:    4000,
				Memory: 16384,
			},
		},
		{
			ID:     "node-2",
			Region: "us-west-2",
			Zone:   "us-west-2a",
			CPU:    4000,
			Memory: 16384,
			Labels: map[string]string{},
			Capacity: ResourceCapacity{
				CPU:    4000,
				Memory: 16384,
			},
		},
	}, nil
}

// GetNodesByRegion returns mock nodes by region.
func (r *DefaultNodeRegistry) GetNodesByRegion(ctx context.Context, region string) ([]NodeInfo, error) {
	nodes, err := r.ListNodes(ctx)
	if err != nil {
		return nil, err
	}

	var filtered []NodeInfo
	for _, node := range nodes {
		if node.Region == region {
			filtered = append(filtered, node)
		}
	}

	return filtered, nil
}

// GetNodesByZone returns mock nodes by zone.
func (r *DefaultNodeRegistry) GetNodesByZone(ctx context.Context, region, zone string) ([]NodeInfo, error) {
	nodes, err := r.ListNodes(ctx)
	if err != nil {
		return nil, err
	}

	var filtered []NodeInfo
	for _, node := range nodes {
		if node.Region == region && node.Zone == zone {
			filtered = append(filtered, node)
		}
	}

	return filtered, nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

func matchesNodeSelectors(node NodeInfo, selectors map[string]string) bool {
	for key, value := range selectors {
		if nodeValue, exists := node.Labels[key]; !exists || nodeValue != value {
			return false
		}
	}
	return true
}

func parseCost(costStr string) (float64, error) {
	// Simple parser for cost strings like "0.50", "$0.50", "50c"
	// In production, this would be more robust
	var cost float64
	_, err := fmt.Sscanf(costStr, "%f", &cost)
	return cost, err
}
