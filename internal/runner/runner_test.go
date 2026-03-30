package runner

import (
	"context"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/JSLEEKR/dagrun/internal/dag"
	"github.com/JSLEEKR/dagrun/internal/model"
)

func skipWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-based test skipped on Windows")
	}
}

func buildAndRun(t *testing.T, d *model.DAG) model.DAGResult {
	t.Helper()
	g, err := dag.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	r := New(d, g)
	return r.Run(context.Background())
}

func TestRunSingleStep(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "single",
		Steps: []model.Step{
			{Name: "hello", Command: "echo hello"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v, want succeeded", result.Status)
	}
	if len(result.Nodes) != 1 {
		t.Errorf("expected 1 node result, got %d", len(result.Nodes))
	}
	if result.Nodes[0].Output != "hello" {
		t.Errorf("output = %q", result.Nodes[0].Output)
	}
}

func TestRunLinearDeps(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "linear",
		Steps: []model.Step{
			{Name: "first", Command: "echo first"},
			{Name: "second", Command: "echo second", Depends: []string{"first"}},
			{Name: "third", Command: "echo third", Depends: []string{"second"}},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if len(result.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(result.Nodes))
	}
}

func TestRunParallelSteps(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "parallel",
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b"},
			{Name: "c", Command: "echo c"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if len(result.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(result.Nodes))
	}
}

func TestRunDiamond(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "diamond",
		Steps: []model.Step{
			{Name: "root", Command: "echo root"},
			{Name: "left", Command: "echo left", Depends: []string{"root"}},
			{Name: "right", Command: "echo right", Depends: []string{"root"}},
			{Name: "join", Command: "echo join", Depends: []string{"left", "right"}},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if len(result.Nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(result.Nodes))
	}
}

func TestRunFailedStep(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "fail",
		Steps: []model.Step{
			{Name: "fail", Command: "exit 1"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeFailed {
		t.Errorf("status = %v, want failed", result.Status)
	}
}

func TestRunFailureCascade(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "cascade",
		Steps: []model.Step{
			{Name: "fail", Command: "exit 1"},
			{Name: "blocked", Command: "echo ok", Depends: []string{"fail"}},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeFailed {
		t.Errorf("status = %v, want failed", result.Status)
	}
	// blocked step should be skipped
	for _, nr := range result.Nodes {
		if nr.Name == "blocked" {
			if nr.Status != model.NodeSkipped {
				t.Errorf("blocked status = %v, want skipped", nr.Status)
			}
		}
	}
}

func TestRunContinueOnFailure(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "continue",
		Steps: []model.Step{
			{Name: "fail", Command: "exit 1", ContinueOn: model.ContinueOn{Failure: true}},
			{Name: "next", Command: "echo ok", Depends: []string{"fail"}},
		},
	}
	result := buildAndRun(t, d)
	// Overall should be failed because fail step failed, but next should succeed
	for _, nr := range result.Nodes {
		if nr.Name == "next" {
			if nr.Status != model.NodeSucceeded {
				t.Errorf("next status = %v, want succeeded", nr.Status)
			}
		}
	}
}

func TestRunOutputCapture(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "output",
		Steps: []model.Step{
			{Name: "producer", Command: "echo captured_value", Output: "MY_OUTPUT"},
			{Name: "consumer", Command: "echo $MY_OUTPUT", Depends: []string{"producer"}},
		},
	}
	g, err := dag.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	r := New(d, g)
	result := r.Run(context.Background())
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	for _, nr := range result.Nodes {
		if nr.Name == "consumer" {
			if nr.Output != "captured_value" {
				t.Errorf("consumer output = %q, want %q", nr.Output, "captured_value")
			}
		}
	}
}

func TestRunWithEnv(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "env",
		Env:  map[string]string{"DAG_VAR": "from_dag"},
		Steps: []model.Step{
			{Name: "use-env", Command: "echo $DAG_VAR"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if result.Nodes[0].Output != "from_dag" {
		t.Errorf("output = %q", result.Nodes[0].Output)
	}
}

func TestRunWithParams(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name:   "params",
		Params: []string{"GREETING=hello"},
		Steps: []model.Step{
			{Name: "use-param", Command: "echo $GREETING"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if result.Nodes[0].Output != "hello" {
		t.Errorf("output = %q", result.Nodes[0].Output)
	}
}

func TestRunTimeout(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name:       "timeout",
		TimeoutSec: 1,
		Steps: []model.Step{
			{Name: "slow", Command: "sleep 30"},
		},
	}
	start := time.Now()
	result := buildAndRun(t, d)
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Errorf("timeout took too long: %v", elapsed)
	}
	if result.Status == model.NodeSucceeded {
		t.Error("should not have succeeded")
	}
}

func TestRunStepTimeout(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "step-timeout",
		Steps: []model.Step{
			{Name: "slow", Command: "sleep 30", TimeoutSec: 1},
		},
	}
	start := time.Now()
	result := buildAndRun(t, d)
	elapsed := time.Since(start)
	if elapsed > 5*time.Second {
		t.Errorf("step timeout took too long: %v", elapsed)
	}
	found := false
	for _, nr := range result.Nodes {
		if nr.Name == "slow" {
			found = true
			if nr.Status != model.NodeTimedOut && nr.Status != model.NodeFailed {
				t.Errorf("slow status = %v", nr.Status)
			}
		}
	}
	if !found {
		t.Error("missing slow node result")
	}
}

func TestRunContextCancellation(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "cancel",
		Steps: []model.Step{
			{Name: "slow", Command: "sleep 30"},
		},
	}
	g, err := dag.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(200 * time.Millisecond)
		cancel()
	}()
	r := New(d, g)
	result := r.Run(ctx)
	if result.Status == model.NodeSucceeded {
		t.Error("should not have succeeded after cancel")
	}
}

func TestRunMaxActive(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name:      "max-active",
		MaxActive: 1,
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
			{Name: "b", Command: "echo b"},
			{Name: "c", Command: "echo c"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if len(result.Nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(result.Nodes))
	}
}

func TestRunPreconditionPass(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "precond-pass",
		Steps: []model.Step{
			{
				Name:    "guarded",
				Command: "echo ok",
				Preconditions: []model.Precondition{
					{Condition: "echo yes", Expected: "yes"},
				},
			},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
}

func TestRunPreconditionFail(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "precond-fail",
		Steps: []model.Step{
			{
				Name:    "guarded",
				Command: "echo ok",
				Preconditions: []model.Precondition{
					{Condition: "echo no", Expected: "yes"},
				},
			},
		},
	}
	result := buildAndRun(t, d)
	for _, nr := range result.Nodes {
		if nr.Name == "guarded" {
			if nr.Status != model.NodeSkipped {
				t.Errorf("guarded status = %v, want skipped", nr.Status)
			}
		}
	}
}

func TestRunRetry(t *testing.T) {
	skipWindows(t)
	// Use a command that fails a few times then succeeds
	// We test retry by setting limit=2 on a command that always fails
	d := &model.DAG{
		Name: "retry",
		Steps: []model.Step{
			{
				Name:    "flaky",
				Command: "exit 1",
				RetryPolicy: &model.RetryPolicy{
					Limit:       2,
					IntervalSec: 0,
				},
			},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeFailed {
		t.Errorf("status = %v, want failed (should fail after retries)", result.Status)
	}
	for _, nr := range result.Nodes {
		if nr.Name == "flaky" {
			if nr.Retries != 2 {
				t.Errorf("retries = %d, want 2", nr.Retries)
			}
		}
	}
}

func TestRunDuration(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "duration",
		Steps: []model.Step{
			{Name: "quick", Command: "echo quick"},
		},
	}
	result := buildAndRun(t, d)
	if result.Duration() <= 0 {
		t.Error("duration should be positive")
	}
	for _, nr := range result.Nodes {
		if nr.Duration() < 0 {
			t.Errorf("node %q has negative duration", nr.Name)
		}
	}
}

func TestRunSummary(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "summary-test",
		Steps: []model.Step{
			{Name: "ok", Command: "echo ok"},
		},
	}
	result := buildAndRun(t, d)
	summary := result.Summary()
	if !strings.Contains(summary, "summary-test") {
		t.Errorf("summary missing dag name: %s", summary)
	}
}

func TestGetResults(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "results",
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
		},
	}
	g, err := dag.Build(d)
	if err != nil {
		t.Fatal(err)
	}
	r := New(d, g)
	r.Run(context.Background())
	results := r.GetResults()
	if len(results) != 1 {
		t.Errorf("expected 1 result, got %d", len(results))
	}
	if _, ok := results["a"]; !ok {
		t.Error("missing result for 'a'")
	}
}

func TestRunDeepCascade(t *testing.T) {
	skipWindows(t)
	// fail -> b -> c -> d : all should cascade skip
	d := &model.DAG{
		Name: "deep-cascade",
		Steps: []model.Step{
			{Name: "fail", Command: "exit 1"},
			{Name: "b", Command: "echo b", Depends: []string{"fail"}},
			{Name: "c", Command: "echo c", Depends: []string{"b"}},
			{Name: "d", Command: "echo d", Depends: []string{"c"}},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeFailed {
		t.Errorf("status = %v", result.Status)
	}
	for _, nr := range result.Nodes {
		if nr.Name != "fail" && nr.Status != model.NodeSkipped {
			t.Errorf("node %q should be skipped, got %v", nr.Name, nr.Status)
		}
	}
}

func TestRunMixedSuccessFailure(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "mixed",
		Steps: []model.Step{
			{Name: "ok", Command: "echo ok"},
			{Name: "fail", Command: "exit 1"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeFailed {
		t.Errorf("status = %v, want failed (one step failed)", result.Status)
	}
	okFound, failFound := false, false
	for _, nr := range result.Nodes {
		if nr.Name == "ok" {
			okFound = true
			if nr.Status != model.NodeSucceeded {
				t.Errorf("ok status = %v", nr.Status)
			}
		}
		if nr.Name == "fail" {
			failFound = true
			if nr.Status != model.NodeFailed {
				t.Errorf("fail status = %v", nr.Status)
			}
		}
	}
	if !okFound || !failFound {
		t.Error("missing node results")
	}
}

func TestRunWithStepEnv(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name: "step-env",
		Steps: []model.Step{
			{
				Name:    "with-env",
				Command: "echo $STEP_VAR",
				Env:     map[string]string{"STEP_VAR": "step_value"},
			},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
	if result.Nodes[0].Output != "step_value" {
		t.Errorf("output = %q, want %q", result.Nodes[0].Output, "step_value")
	}
}

func TestRunHTTPStep(t *testing.T) {
	// HTTP test uses the executor's HTTP capability
	// Just verifying the runner can dispatch HTTP steps
	d := &model.DAG{
		Name: "http-step",
		Steps: []model.Step{
			{
				Name: "api",
				Type: "http",
				HTTPConfig: &model.HTTPConfig{
					Method: "GET",
					URL:    "http://localhost:1", // will fail, but tests dispatch
				},
			},
		},
	}
	result := buildAndRun(t, d)
	// Should fail (no server) but not panic
	if result.Status == model.NodeSucceeded {
		t.Error("should not succeed with no server")
	}
}

func TestRunEmptyDAGName(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Steps: []model.Step{
			{Name: "a", Command: "echo a"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v", result.Status)
	}
}

func TestRunInheritDAGShell(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name:  "inherit-shell",
		Shell: "bash",
		Steps: []model.Step{
			{Name: "uses-dag-shell", Command: "echo $BASH_VERSION"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v, want succeeded", result.Status)
	}
	// The step should have run with bash (inheriting from DAG level)
	if result.Nodes[0].Output == "" {
		t.Error("expected non-empty output from bash (BASH_VERSION should be set)")
	}
}

func TestRunInheritDAGWorkingDir(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name:       "inherit-workdir",
		WorkingDir: "/tmp",
		Steps: []model.Step{
			{Name: "uses-dag-dir", Command: "pwd"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v, want succeeded", result.Status)
	}
	// On macOS/Linux, /tmp may resolve to /private/tmp
	output := result.Nodes[0].Output
	if !strings.Contains(output, "tmp") {
		t.Errorf("output = %q, want to contain 'tmp' (working dir should be /tmp)", output)
	}
}

func TestRunStepOverridesDAGShell(t *testing.T) {
	skipWindows(t)
	// Step-level shell should override DAG-level
	d := &model.DAG{
		Name:  "override-shell",
		Shell: "bash",
		Steps: []model.Step{
			{Name: "uses-step-shell", Command: "echo ok", Shell: "sh"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v, want succeeded", result.Status)
	}
}

func TestRunStepOverridesDAGWorkingDir(t *testing.T) {
	skipWindows(t)
	d := &model.DAG{
		Name:       "override-workdir",
		WorkingDir: "/tmp",
		Steps: []model.Step{
			{Name: "uses-step-dir", Command: "pwd", WorkingDir: "/"},
		},
	}
	result := buildAndRun(t, d)
	if result.Status != model.NodeSucceeded {
		t.Errorf("status = %v, want succeeded", result.Status)
	}
	if result.Nodes[0].Output != "/" {
		t.Errorf("output = %q, want '/' (step working_dir should override DAG)", result.Nodes[0].Output)
	}
}
