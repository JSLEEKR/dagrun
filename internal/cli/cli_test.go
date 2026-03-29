package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/JSLEEKR/dagrun/internal/model"
)

func writeWorkflow(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "workflow.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func skipWindows(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell-based test skipped on Windows")
	}
}

func TestVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"version"})
	if code != 0 {
		t.Errorf("exit code = %d", code)
	}
	if !strings.Contains(stdout.String(), "dagrun") {
		t.Errorf("version output = %q", stdout.String())
	}
}

func TestHelp(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"help"})
	if code != 0 {
		t.Errorf("exit code = %d", code)
	}
	output := stdout.String()
	if !strings.Contains(output, "dagrun") {
		t.Error("help should mention dagrun")
	}
	if !strings.Contains(output, "run") {
		t.Error("help should mention run command")
	}
}

func TestHelpFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"-h"})
	if code != 0 {
		t.Errorf("exit code = %d", code)
	}
}

func TestHelpLongFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"--help"})
	if code != 0 {
		t.Errorf("exit code = %d", code)
	}
}

func TestNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"foobar"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "unknown") {
		t.Errorf("stderr = %q", stderr.String())
	}
}

func TestValidate(t *testing.T) {
	path := writeWorkflow(t, `name: valid
steps:
  - name: hello
    command: echo hello
  - name: world
    command: echo world
    depends:
      - hello
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"validate", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Valid") {
		t.Errorf("output = %q", stdout.String())
	}
}

func TestValidateMissingFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"validate", "/nonexistent.yaml"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestValidateNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"validate"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestValidateCycle(t *testing.T) {
	path := writeWorkflow(t, `name: cycle
steps:
  - name: a
    command: echo a
    depends:
      - b
  - name: b
    command: echo b
    depends:
      - a
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"validate", path})
	if code != 1 {
		t.Errorf("exit code = %d, want 1 for cycle", code)
	}
}

func TestDot(t *testing.T) {
	path := writeWorkflow(t, `name: dot-test
steps:
  - name: a
    command: echo a
  - name: b
    command: echo b
    depends:
      - a
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"dot", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "digraph") {
		t.Error("DOT output should contain digraph")
	}
	if !strings.Contains(output, `"a" -> "b"`) {
		t.Error("DOT output should contain edge a -> b")
	}
}

func TestDotNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"dot"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestStatus(t *testing.T) {
	path := writeWorkflow(t, `name: status-test
description: Test workflow
steps:
  - name: first
    command: echo first
  - name: second
    command: echo second
    depends:
      - first
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"status", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "status-test") {
		t.Error("status should show workflow name")
	}
	if !strings.Contains(output, "Execution Plan") {
		t.Error("status should show execution plan")
	}
}

func TestStatusNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"status"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunCommand(t *testing.T) {
	skipWindows(t)
	path := writeWorkflow(t, `name: run-test
steps:
  - name: hello
    command: echo hello
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
}

func TestRunVerbose(t *testing.T) {
	skipWindows(t)
	path := writeWorkflow(t, `name: verbose-test
steps:
  - name: hello
    command: echo hello_world
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run", "-v", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, "Executing") {
		t.Error("verbose should show executing message")
	}
}

func TestRunJSON(t *testing.T) {
	skipWindows(t)
	path := writeWorkflow(t, `name: json-test
steps:
  - name: hello
    command: echo hello
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run", "-json", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, `"name"`) {
		t.Error("JSON output should contain name field")
	}
	if !strings.Contains(output, `"status"`) {
		t.Error("JSON output should contain status field")
	}
}

func TestRunNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunMissingFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run", "/nonexistent.yaml"})
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunFailedWorkflow(t *testing.T) {
	skipWindows(t)
	path := writeWorkflow(t, `name: fail-test
steps:
  - name: fail
    command: exit 1
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run", path})
	if code != 1 {
		t.Errorf("exit code = %d, want 1 for failed workflow", code)
	}
}

func TestRunWithParams(t *testing.T) {
	skipWindows(t)
	path := writeWorkflow(t, `name: param-test
steps:
  - name: show
    command: echo $GREETING
`)
	var stdout, stderr bytes.Buffer
	app := New(&stdout, &stderr)
	code := app.Run([]string{"run", "-params", "GREETING=hi", path})
	if code != 0 {
		t.Errorf("exit code = %d, stderr: %s", code, stderr.String())
	}
}

func TestStatusIcon(t *testing.T) {
	tests := []struct {
		status model.NodeStatus
		want   string
	}{
		{model.NodeSucceeded, "[OK]"},
		{model.NodeFailed, "[FAIL]"},
		{model.NodeSkipped, "[SKIP]"},
		{model.NodeAborted, "[ABORT]"},
		{model.NodeTimedOut, "[TIMEOUT]"},
		{model.NodeRunning, "[RUN]"},
		{model.NodePending, "[?]"},
	}
	for _, tt := range tests {
		if got := statusIcon(tt.status); got != tt.want {
			t.Errorf("statusIcon(%v) = %q, want %q", tt.status, got, tt.want)
		}
	}
}

func TestTruncate(t *testing.T) {
	tests := []struct {
		input string
		max   int
		want  string
	}{
		{"hello", 10, "hello"},
		{"hello world", 5, "hello..."},
		{"", 5, ""},
		{"abc", 3, "abc"},
	}
	for _, tt := range tests {
		if got := truncate(tt.input, tt.max); got != tt.want {
			t.Errorf("truncate(%q, %d) = %q, want %q", tt.input, tt.max, got, tt.want)
		}
	}
}
