package dependency

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/kranix-io/kranix-core/internal/state"
	"github.com/kranix-io/kranix-core/pkg/types"
)

// Resolver handles dependency resolution for workloads.
type Resolver struct {
	store state.Store
}

// New creates a new dependency resolver.
func New(store state.Store) *Resolver {
	return &Resolver{
		store: store,
	}
}

// ResolveDeploymentOrder determines the correct deployment order for workloads based on dependencies.
func (r *Resolver) ResolveDeploymentOrder(ctx context.Context, workloads []*types.Workload) ([]*types.Workload, error) {
	// Build dependency graph
	graph := r.buildDependencyGraph(workloads)

	// Perform topological sort
	ordered, err := r.topologicalSort(graph)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve deployment order: %w", err)
	}

	// Map ordered IDs back to workloads
	workloadMap := make(map[string]*types.Workload)
	for _, w := range workloads {
		workloadMap[w.ID] = w
	}

	result := make([]*types.Workload, 0, len(ordered))
	for _, id := range ordered {
		if w, exists := workloadMap[id]; exists {
			result = append(result, w)
		}
	}

	return result, nil
}

// CheckDependencies verifies if all dependencies for a workload are satisfied.
func (r *Resolver) CheckDependencies(ctx context.Context, workload *types.Workload) (bool, error) {
	if len(workload.Spec.Dependencies) == 0 {
		return true, nil
	}

	for _, dep := range workload.Spec.Dependencies {
		satisfied, err := r.checkDependency(ctx, dep)
		if err != nil {
			return false, fmt.Errorf("failed to check dependency %s: %w", dep.WorkloadID, err)
		}
		if !satisfied {
			log.Printf("Dependency %s not satisfied for workload %s", dep.WorkloadID, workload.ID)
			return false, nil
		}
	}

	return true, nil
}

// checkDependency checks if a single dependency is satisfied.
func (r *Resolver) checkDependency(ctx context.Context, dep types.Dependency) (bool, error) {
	// Get the dependent workload
	depWorkload, err := r.store.Get(ctx, dep.WorkloadID)
	if err != nil {
		// Dependency workload doesn't exist
		return false, nil
	}

	// Check the condition
	switch dep.Condition {
	case "", "running":
		return depWorkload.Status.Phase == types.WorkloadPhaseRunning, nil
	case "healthy":
		return r.isHealthy(depWorkload), nil
	case "ready":
		return r.isReady(depWorkload), nil
	default:
		// Unknown condition, default to running
		return depWorkload.Status.Phase == types.WorkloadPhaseRunning, nil
	}
}

// isHealthy checks if a workload is healthy.
func (r *Resolver) isHealthy(workload *types.Workload) bool {
	return workload.Status.Phase == types.WorkloadPhaseRunning &&
		workload.Status.ReadyReplicas > 0
}

// isReady checks if a workload is ready.
func (r *Resolver) isReady(workload *types.Workload) bool {
	return workload.Status.Phase == types.WorkloadPhaseRunning &&
		workload.Status.ReadyReplicas == workload.Spec.Replicas
}

// WaitDependencies waits for dependencies to be satisfied with a timeout.
func (r *Resolver) WaitDependencies(ctx context.Context, workload *types.Workload) error {
	if len(workload.Spec.Dependencies) == 0 {
		return nil
	}

	// Calculate max timeout
	maxTimeout := 5 * time.Minute
	for _, dep := range workload.Spec.Dependencies {
		if dep.Timeout != "" {
			if d, err := time.ParseDuration(dep.Timeout); err == nil {
				if d > maxTimeout {
					maxTimeout = d
				}
			}
		}
	}

	ctx, cancel := context.WithTimeout(ctx, maxTimeout)
	defer cancel()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("timeout waiting for dependencies for workload %s", workload.ID)
		case <-ticker.C:
			satisfied, err := r.CheckDependencies(ctx, workload)
			if err != nil {
				return err
			}
			if satisfied {
				return nil
			}
		}
	}
}

// buildDependencyGraph builds a dependency graph from workloads.
func (r *Resolver) buildDependencyGraph(workloads []*types.Workload) map[string][]string {
	graph := make(map[string][]string)
	
	// Initialize graph with all workloads
	for _, w := range workloads {
		graph[w.ID] = []string{}
	}
	
	// Add edges based on dependencies
	for _, w := range workloads {
		for _, dep := range w.Spec.Dependencies {
			// Edge from dependency to workload (dependency must be deployed first)
			graph[dep.WorkloadID] = append(graph[dep.WorkloadID], w.ID)
		}
	}
	
	return graph
}

// topologicalSort performs topological sort on the dependency graph.
func (r *Resolver) topologicalSort(graph map[string][]string) ([]string, error) {
	// Calculate in-degree for each node
	inDegree := make(map[string]int)
	for node := range graph {
		inDegree[node] = 0
	}
	
	for _, edges := range graph {
		for _, edge := range edges {
			inDegree[edge]++
		}
	}
	
	// Find nodes with no incoming edges
	queue := make([]string, 0)
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}
	
	// Process nodes
	result := make([]string, 0)
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		result = append(result, node)
		
		// Reduce in-degree of neighbors
		for _, neighbor := range graph[node] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}
	
	// Check for cycles
	if len(result) != len(graph) {
		return nil, fmt.Errorf("circular dependency detected")
	}
	
	return result, nil
}

// DetectCycles detects if there are circular dependencies in the workload set.
func (r *Resolver) DetectCycles(workloads []*types.Workload) ([]string, error) {
	graph := r.buildDependencyGraph(workloads)
	_, err := r.topologicalSort(graph)
	if err != nil {
		return r.findCycle(graph), nil
	}
	return nil, nil
}

// findCycle finds a cycle in the dependency graph.
func (r *Resolver) findCycle(graph map[string][]string) []string {
	visited := make(map[string]bool)
	recursionStack := make(map[string]bool)
	path := []string{}
	
	for node := range graph {
		if !visited[node] {
			if cycle := r.findCycleDFS(node, graph, visited, recursionStack, &path); cycle != nil {
				return cycle
			}
		}
	}
	
	return nil
}

// findCycleDFS performs DFS to find cycles.
func (r *Resolver) findCycleDFS(node string, graph map[string][]string, visited, recursionStack map[string]bool, path *[]string) []string {
	visited[node] = true
	recursionStack[node] = true
	*path = append(*path, node)
	
	for _, neighbor := range graph[node] {
		if !visited[neighbor] {
			if cycle := r.findCycleDFS(neighbor, graph, visited, recursionStack, path); cycle != nil {
				return cycle
			}
		} else if recursionStack[neighbor] {
			// Found a cycle
			cycleStart := -1
			for i, n := range *path {
				if n == neighbor {
					cycleStart = i
					break
				}
			}
			if cycleStart != -1 {
				return append((*path)[cycleStart:], neighbor)
			}
		}
	}
	
	recursionStack[node] = false
	*path = (*path)[:len(*path)-1]
	return nil
}
