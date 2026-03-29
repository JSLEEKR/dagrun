package dag

import (
	"strings"
	"testing"

	"github.com/JSLEEKR/dagrun/internal/model"
)

func TestBuildSimpleLinear(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b", Depends: []string{"a"}},
			{Name: "c", Command: "echo c", Depends: []string{"b"}},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(g.Nodes))
	}
	if len(g.TopoOrder) != 3 {
		t.Errorf("topo order should have 3 elements, got %d", len(g.TopoOrder))
	}
	// a must come before b, b before c
	aIdx, bIdx, cIdx := indexOf(g.TopoOrder, "a"), indexOf(g.TopoOrder, "b"), indexOf(g.TopoOrder, "c")
	if aIdx >= bIdx || bIdx >= cIdx {
		t.Errorf("incorrect topo order: %v", g.TopoOrder)
	}
}

func TestBuildDiamond(t *testing.T) {
	// Diamond: a -> b, a -> c, b -> d, c -> d
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b", Depends: []string{"a"}},
			{Name: "c", Command: "echo c", Depends: []string{"a"}},
			{Name: "d", Command: "echo d", Depends: []string{"b", "c"}},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.Nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(g.Nodes))
	}
	// a must be first, d must be last
	if g.TopoOrder[0] != "a" {
		t.Errorf("first should be 'a', got %q", g.TopoOrder[0])
	}
	if g.TopoOrder[3] != "d" {
		t.Errorf("last should be 'd', got %q", g.TopoOrder[3])
	}
}

func TestBuildParallel(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b"},
			{Name: "c", Command: "echo c"},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	roots := g.RootNodes()
	if len(roots) != 3 {
		t.Errorf("expected 3 roots, got %d", len(roots))
	}
}

func TestBuildCycleDetection(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a", Depends: []string{"c"}},
			{Name: "b", Command: "echo b", Depends: []string{"a"}},
			{Name: "c", Command: "echo c", Depends: []string{"b"}},
		},
	}
	_, err := Build(d)
	if err == nil {
		t.Fatal("expected cycle error")
	}
	if !strings.Contains(err.Error(), "cycle") {
		t.Errorf("expected cycle error, got: %v", err)
	}
}

func TestBuildSelfCycle(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a", Depends: []string{"a"}},
		},
	}
	_, err := Build(d)
	if err == nil {
		t.Fatal("expected cycle error for self-dependency")
	}
}

func TestBuildMissingDependency(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a", Depends: []string{"nonexistent"}},
		},
	}
	_, err := Build(d)
	if err == nil {
		t.Fatal("expected missing dependency error")
	}
	if !strings.Contains(err.Error(), "nonexistent") {
		t.Errorf("error should mention missing dep: %v", err)
	}
}

func TestBuildDuplicateNames(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "a", Command: "echo a2"},
		},
	}
	_, err := Build(d)
	if err == nil {
		t.Fatal("expected duplicate name error")
	}
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error should mention duplicate: %v", err)
	}
}

func TestBuildEmptyName(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "", Command: "echo a"},
		},
	}
	_, err := Build(d)
	if err == nil {
		t.Fatal("expected empty name error")
	}
}

func TestGetNode(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b"},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}

	n, ok := g.GetNode("a")
	if !ok || n.Step.Name != "a" {
		t.Error("GetNode(a) failed")
	}

	_, ok = g.GetNode("nonexistent")
	if ok {
		t.Error("GetNode(nonexistent) should return false")
	}
}

func TestRootNodes(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "root1", Command: "echo 1"},
			{Name: "root2", Command: "echo 2"},
			{Name: "child", Command: "echo 3", Depends: []string{"root1"}},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	roots := g.RootNodes()
	if len(roots) != 2 {
		t.Errorf("expected 2 roots, got %d", len(roots))
	}
	names := make(map[string]bool)
	for _, r := range roots {
		names[r.Step.Name] = true
	}
	if !names["root1"] || !names["root2"] {
		t.Errorf("unexpected roots: %v", names)
	}
}

func TestToDot(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b", Depends: []string{"a"}},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	dot := g.ToDot("test")
	if !strings.Contains(dot, "digraph") {
		t.Error("DOT should contain digraph")
	}
	if !strings.Contains(dot, `"a"`) {
		t.Error("DOT should contain node a")
	}
	if !strings.Contains(dot, `"b"`) {
		t.Error("DOT should contain node b")
	}
	if !strings.Contains(dot, `"a" -> "b"`) {
		t.Error("DOT should contain edge a -> b")
	}
}

func TestBuildComplexDAG(t *testing.T) {
	// Complex: a -> b, a -> c, b -> d, c -> d, d -> e, a -> e
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b", Depends: []string{"a"}},
			{Name: "c", Command: "echo c", Depends: []string{"a"}},
			{Name: "d", Command: "echo d", Depends: []string{"b", "c"}},
			{Name: "e", Command: "echo e", Depends: []string{"d", "a"}},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.TopoOrder) != 5 {
		t.Errorf("expected 5 in topo order, got %d", len(g.TopoOrder))
	}

	// Verify all ordering constraints
	positions := make(map[string]int)
	for i, name := range g.TopoOrder {
		positions[name] = i
	}
	if positions["a"] >= positions["b"] {
		t.Error("a must come before b")
	}
	if positions["a"] >= positions["c"] {
		t.Error("a must come before c")
	}
	if positions["b"] >= positions["d"] {
		t.Error("b must come before d")
	}
	if positions["c"] >= positions["d"] {
		t.Error("c must come before d")
	}
	if positions["d"] >= positions["e"] {
		t.Error("d must come before e")
	}
}

func TestDependencyMaps(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b", Depends: []string{"a"}},
			{Name: "c", Command: "echo c", Depends: []string{"a"}},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}

	// a has no deps
	if len(g.Dependencies["a"]) != 0 {
		t.Errorf("a should have 0 deps, got %d", len(g.Dependencies["a"]))
	}
	// a has 2 dependents
	if len(g.Dependents["a"]) != 2 {
		t.Errorf("a should have 2 dependents, got %d", len(g.Dependents["a"]))
	}
	// b depends on a
	if len(g.Dependencies["b"]) != 1 || g.Dependencies["b"][0] != "a" {
		t.Errorf("b should depend on a, got %v", g.Dependencies["b"])
	}
}

func TestSingleNode(t *testing.T) {
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "only", Command: "echo only"},
		},
	}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if len(g.TopoOrder) != 1 {
		t.Errorf("expected 1 in topo order, got %d", len(g.TopoOrder))
	}
	roots := g.RootNodes()
	if len(roots) != 1 || roots[0].Step.Name != "only" {
		t.Error("single node should be root")
	}
}

func TestBuildWideDAG(t *testing.T) {
	// 10 parallel steps depending on a single root
	steps := []model.Step{{Name: "root", Command: "echo root"}}
	for i := 0; i < 10; i++ {
		steps = append(steps, model.Step{
			Name:    strings.Replace("child_NN", "NN", string(rune('A'+i)), 1),
			Command: "echo child",
			Depends: []string{"root"},
		})
	}
	d := &model.DAG{Steps: steps}
	g, err := Build(d)
	if err != nil {
		t.Fatal(err)
	}
	if g.TopoOrder[0] != "root" {
		t.Error("root should be first")
	}
	roots := g.RootNodes()
	if len(roots) != 1 {
		t.Errorf("expected 1 root, got %d", len(roots))
	}
}

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}
