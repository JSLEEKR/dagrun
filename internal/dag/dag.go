// Package dag provides DAG construction with Kahn's algorithm for topological
// sorting and cycle detection.
package dag

import (
	"fmt"

	"github.com/JSLEEKR/dagrun/internal/model"
)

// Graph represents a dependency graph built from DAG steps.
type Graph struct {
	Nodes         []*Node
	nodeByName    map[string]*Node
	Dependencies  map[string][]string // node -> its upstream deps
	Dependents    map[string][]string // node -> its downstream dependents
	TopoOrder     []string            // topologically sorted node names
}

// Node wraps a step with graph metadata.
type Node struct {
	Step     model.Step
	InDegree int
}

// Build constructs a dependency graph from a DAG definition.
// Returns an error if there are missing dependencies or cycles.
func Build(dag *model.DAG) (*Graph, error) {
	g := &Graph{
		nodeByName:   make(map[string]*Node),
		Dependencies: make(map[string][]string),
		Dependents:   make(map[string][]string),
	}

	// Create nodes
	for _, step := range dag.Steps {
		if step.Name == "" {
			return nil, fmt.Errorf("step has empty name")
		}
		if _, exists := g.nodeByName[step.Name]; exists {
			return nil, fmt.Errorf("duplicate step name: %q", step.Name)
		}
		node := &Node{Step: step}
		g.Nodes = append(g.Nodes, node)
		g.nodeByName[step.Name] = node
	}

	// Build edges
	if err := g.buildEdges(); err != nil {
		return nil, err
	}

	// Topological sort + cycle detection via Kahn's algorithm
	order, err := g.kahnSort()
	if err != nil {
		return nil, err
	}
	g.TopoOrder = order

	return g, nil
}

// buildEdges populates the dependency and dependent maps.
func (g *Graph) buildEdges() error {
	for _, node := range g.Nodes {
		name := node.Step.Name
		for _, dep := range node.Step.Depends {
			depNode, ok := g.nodeByName[dep]
			if !ok {
				return fmt.Errorf("step %q depends on unknown step %q", name, dep)
			}
			g.Dependencies[name] = append(g.Dependencies[name], dep)
			g.Dependents[dep] = append(g.Dependents[dep], name)
			_ = depNode // used for existence check
			node.InDegree++
		}
	}
	return nil
}

// kahnSort performs Kahn's topological sort and detects cycles.
func (g *Graph) kahnSort() ([]string, error) {
	// Copy in-degrees so we don't mutate the graph
	inDegree := make(map[string]int, len(g.Nodes))
	for _, n := range g.Nodes {
		inDegree[n.Step.Name] = n.InDegree
	}

	// Start with zero in-degree nodes
	var queue []string
	for _, n := range g.Nodes {
		if inDegree[n.Step.Name] == 0 {
			queue = append(queue, n.Step.Name)
		}
	}

	var order []string
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		order = append(order, name)

		for _, dep := range g.Dependents[name] {
			inDegree[dep]--
			if inDegree[dep] == 0 {
				queue = append(queue, dep)
			}
		}
	}

	if len(order) != len(g.Nodes) {
		// Find cycle participants
		var cycleNodes []string
		for _, n := range g.Nodes {
			if inDegree[n.Step.Name] > 0 {
				cycleNodes = append(cycleNodes, n.Step.Name)
			}
		}
		return nil, fmt.Errorf("cycle detected involving nodes: %v", cycleNodes)
	}

	return order, nil
}

// GetNode returns the node with the given name.
func (g *Graph) GetNode(name string) (*Node, bool) {
	n, ok := g.nodeByName[name]
	return n, ok
}

// RootNodes returns nodes with no dependencies (in-degree 0).
func (g *Graph) RootNodes() []*Node {
	var roots []*Node
	for _, n := range g.Nodes {
		if len(g.Dependencies[n.Step.Name]) == 0 {
			roots = append(roots, n)
		}
	}
	return roots
}

// ToDot generates a DOT graph representation for visualization.
func (g *Graph) ToDot(dagName string) string {
	dot := fmt.Sprintf("digraph %q {\n  rankdir=TB;\n  node [shape=box];\n", dagName)
	for _, n := range g.Nodes {
		dot += fmt.Sprintf("  %q;\n", n.Step.Name)
	}
	for name, deps := range g.Dependencies {
		for _, dep := range deps {
			dot += fmt.Sprintf("  %q -> %q;\n", dep, name)
		}
	}
	dot += "}\n"
	return dot
}
