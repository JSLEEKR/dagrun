package model

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestNodeStatusString(t *testing.T) {
	tests := []struct {
		status NodeStatus
		want   string
	}{
		{NodePending, "pending"},
		{NodeRunning, "running"},
		{NodeSucceeded, "succeeded"},
		{NodeFailed, "failed"},
		{NodeSkipped, "skipped"},
		{NodeAborted, "aborted"},
		{NodeTimedOut, "timed_out"},
		{NodeStatus(99), "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := tt.status.String(); got != tt.want {
				t.Errorf("NodeStatus(%d).String() = %q, want %q", tt.status, got, tt.want)
			}
		})
	}
}

func TestNodeStatusIsTerminal(t *testing.T) {
	tests := []struct {
		status   NodeStatus
		terminal bool
	}{
		{NodePending, false},
		{NodeRunning, false},
		{NodeSucceeded, true},
		{NodeFailed, true},
		{NodeSkipped, true},
		{NodeAborted, true},
		{NodeTimedOut, true},
	}
	for _, tt := range tests {
		t.Run(tt.status.String(), func(t *testing.T) {
			if got := tt.status.IsTerminal(); got != tt.terminal {
				t.Errorf("IsTerminal() = %v, want %v", got, tt.terminal)
			}
		})
	}
}

func TestRetryPolicyInterval(t *testing.T) {
	rp := RetryPolicy{IntervalSec: 5}
	if got := rp.Interval(); got != 5*time.Second {
		t.Errorf("Interval() = %v, want %v", got, 5*time.Second)
	}
}

func TestRetryPolicyZeroInterval(t *testing.T) {
	rp := RetryPolicy{}
	if got := rp.Interval(); got != 0 {
		t.Errorf("Interval() = %v, want 0", got)
	}
}

func TestNodeResultDuration(t *testing.T) {
	now := time.Now()
	nr := NodeResult{
		StartedAt: now,
		EndedAt:   now.Add(2 * time.Second),
	}
	dur := nr.Duration()
	if dur != 2*time.Second {
		t.Errorf("Duration() = %v, want %v", dur, 2*time.Second)
	}
}

func TestNodeResultDurationZero(t *testing.T) {
	nr := NodeResult{}
	if dur := nr.Duration(); dur != 0 {
		t.Errorf("Duration() = %v, want 0", dur)
	}
}

func TestNodeResultDurationOnlyStart(t *testing.T) {
	nr := NodeResult{StartedAt: time.Now()}
	if dur := nr.Duration(); dur != 0 {
		t.Errorf("Duration() = %v, want 0", dur)
	}
}

func TestDAGResultDuration(t *testing.T) {
	now := time.Now()
	dr := DAGResult{
		StartedAt: now,
		EndedAt:   now.Add(5 * time.Second),
	}
	if dur := dr.Duration(); dur != 5*time.Second {
		t.Errorf("Duration() = %v, want %v", dur, 5*time.Second)
	}
}

func TestDAGResultDurationZero(t *testing.T) {
	dr := DAGResult{}
	if dur := dr.Duration(); dur != 0 {
		t.Errorf("Duration() = %v, want 0", dur)
	}
}

func TestDAGResultSummary(t *testing.T) {
	now := time.Now()
	dr := DAGResult{
		Name:      "test-dag",
		Status:    NodeSucceeded,
		StartedAt: now,
		EndedAt:   now.Add(100 * time.Millisecond),
		Nodes: []NodeResult{
			{Name: "a", Status: NodeSucceeded},
			{Name: "b", Status: NodeSucceeded},
			{Name: "c", Status: NodeFailed},
			{Name: "d", Status: NodeSkipped},
		},
	}
	summary := dr.Summary()
	if summary == "" {
		t.Fatal("Summary() returned empty string")
	}
	// Should contain key info
	for _, want := range []string{"test-dag", "succeeded", "total=4", "succeeded=2", "failed=1", "skipped=1"} {
		if !strings.Contains(summary, want) {
			t.Errorf("Summary() missing %q: %s", want, summary)
		}
	}
}

func TestDAGResultSummaryWithTimedOut(t *testing.T) {
	dr := DAGResult{
		Name:   "timeout-dag",
		Status: NodeFailed,
		Nodes: []NodeResult{
			{Name: "a", Status: NodeTimedOut},
			{Name: "b", Status: NodeAborted},
		},
	}
	summary := dr.Summary()
	if !strings.Contains(summary, "failed=1") || !strings.Contains(summary, "aborted=1") {
		t.Errorf("Summary() = %s", summary)
	}
}

func TestStepResolveType(t *testing.T) {
	tests := []struct {
		name string
		step Step
		want string
	}{
		{"explicit_command", Step{Type: "command"}, "command"},
		{"explicit_http", Step{Type: "http"}, "http"},
		{"explicit_script", Step{Type: "script"}, "script"},
		{"infer_http", Step{HTTPConfig: &HTTPConfig{URL: "http://example.com"}}, "http"},
		{"infer_script", Step{Script: "echo hello"}, "script"},
		{"default_command", Step{Command: "ls"}, "command"},
		{"empty_default", Step{}, "command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.step.ResolveType(); got != tt.want {
				t.Errorf("ResolveType() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPreconditionFields(t *testing.T) {
	pc := Precondition{Condition: "test -f /tmp/foo", Expected: "true"}
	if pc.Condition != "test -f /tmp/foo" {
		t.Errorf("Condition = %q", pc.Condition)
	}
	if pc.Expected != "true" {
		t.Errorf("Expected = %q", pc.Expected)
	}
}

func TestContinueOnDefaults(t *testing.T) {
	co := ContinueOn{}
	if co.Failure || co.Skipped {
		t.Error("ContinueOn should default to false")
	}
}

func TestHTTPConfigDefaults(t *testing.T) {
	cfg := HTTPConfig{URL: "http://example.com"}
	if cfg.Method != "" {
		t.Errorf("Method should be empty by default, got %q", cfg.Method)
	}
	if cfg.Timeout != 0 {
		t.Errorf("Timeout should be 0 by default, got %d", cfg.Timeout)
	}
}

func TestHandlerOnFields(t *testing.T) {
	h := HandlerOn{
		Success: &Step{Name: "on_success", Command: "echo ok"},
		Failure: &Step{Name: "on_failure", Command: "echo fail"},
		Exit:    &Step{Name: "on_exit", Command: "echo exit"},
	}
	if h.Success.Name != "on_success" {
		t.Error("Success handler wrong")
	}
	if h.Failure.Name != "on_failure" {
		t.Error("Failure handler wrong")
	}
	if h.Exit.Name != "on_exit" {
		t.Error("Exit handler wrong")
	}
}

func TestDAGFields(t *testing.T) {
	d := DAG{
		Name:        "test",
		Description: "desc",
		MaxActive:   5,
		TimeoutSec:  30,
		Shell:       "bash",
		WorkingDir:  "/tmp",
		LogDir:      "/var/log",
		Env:         map[string]string{"FOO": "bar"},
		Params:      []string{"a=1", "b=2"},
	}
	if d.Name != "test" {
		t.Errorf("Name = %q", d.Name)
	}
	if d.MaxActive != 5 {
		t.Errorf("MaxActive = %d", d.MaxActive)
	}
	if len(d.Params) != 2 {
		t.Errorf("Params len = %d", len(d.Params))
	}
}

func TestNodeResultExitCode(t *testing.T) {
	nr := NodeResult{ExitCode: 124}
	if nr.ExitCode != 124 {
		t.Errorf("ExitCode = %d, want 124", nr.ExitCode)
	}
}

func TestNodeResultRetries(t *testing.T) {
	nr := NodeResult{Retries: 3}
	if nr.Retries != 3 {
		t.Errorf("Retries = %d, want 3", nr.Retries)
	}
}

func TestNodeResultErrorNil(t *testing.T) {
	nr := NodeResult{}
	if nr.Error != nil {
		t.Error("Error should be nil by default")
	}
}

func TestNodeResultErrorSet(t *testing.T) {
	nr := NodeResult{Error: fmt.Errorf("test error")}
	if nr.Error == nil || nr.Error.Error() != "test error" {
		t.Errorf("Error = %v", nr.Error)
	}
}

func TestStepDependsEmpty(t *testing.T) {
	s := Step{Name: "test"}
	if len(s.Depends) != 0 {
		t.Errorf("Depends should be empty, got %v", s.Depends)
	}
}

func TestStepDependsMultiple(t *testing.T) {
	s := Step{Name: "test", Depends: []string{"a", "b", "c"}}
	if len(s.Depends) != 3 {
		t.Errorf("Depends len = %d, want 3", len(s.Depends))
	}
}
