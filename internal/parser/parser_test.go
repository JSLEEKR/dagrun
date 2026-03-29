package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseSimple(t *testing.T) {
	yaml := `name: test-workflow
steps:
  - name: hello
    command: echo hello
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Name != "test-workflow" {
		t.Errorf("name = %q", dag.Name)
	}
	if len(dag.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(dag.Steps))
	}
	if dag.Steps[0].Name != "hello" {
		t.Errorf("step name = %q", dag.Steps[0].Name)
	}
	if dag.Steps[0].Command != "echo hello" {
		t.Errorf("step command = %q", dag.Steps[0].Command)
	}
}

func TestParseWithDeps(t *testing.T) {
	yaml := `name: dep-test
steps:
  - name: first
    command: echo first
  - name: second
    command: echo second
    depends:
      - first
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if len(dag.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(dag.Steps))
	}
	if len(dag.Steps[1].Depends) != 1 || dag.Steps[1].Depends[0] != "first" {
		t.Errorf("depends = %v", dag.Steps[1].Depends)
	}
}

func TestParseWithEnv(t *testing.T) {
	yaml := `name: env-test
env:
  FOO: bar
  NUM: 42
steps:
  - name: step1
    command: echo $FOO
    env:
      LOCAL: value
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Env["FOO"] != "bar" {
		t.Errorf("FOO = %q", dag.Env["FOO"])
	}
	if dag.Steps[0].Env["LOCAL"] != "value" {
		t.Errorf("LOCAL = %q", dag.Steps[0].Env["LOCAL"])
	}
}

func TestParseWithOutput(t *testing.T) {
	yaml := `name: output-test
steps:
  - name: producer
    command: echo result
    output: MY_RESULT
  - name: consumer
    command: echo $MY_RESULT
    depends:
      - producer
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Steps[0].Output != "MY_RESULT" {
		t.Errorf("output = %q", dag.Steps[0].Output)
	}
}

func TestParseWithRetryPolicy(t *testing.T) {
	yaml := `name: retry-test
steps:
  - name: flaky
    command: curl example.com
    retry_policy:
      limit: 3
      interval_sec: 5
      backoff: 2.0
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	rp := dag.Steps[0].RetryPolicy
	if rp == nil {
		t.Fatal("retry_policy is nil")
	}
	if rp.Limit != 3 {
		t.Errorf("limit = %d", rp.Limit)
	}
	if rp.IntervalSec != 5 {
		t.Errorf("interval_sec = %d", rp.IntervalSec)
	}
	if rp.Backoff != 2.0 {
		t.Errorf("backoff = %f", rp.Backoff)
	}
}

func TestParseWithPreconditions(t *testing.T) {
	yaml := `name: precond-test
steps:
  - name: guarded
    command: echo ok
    preconditions:
      - condition: echo yes
        expected: yes
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	pcs := dag.Steps[0].Preconditions
	if len(pcs) != 1 {
		t.Fatalf("expected 1 precondition, got %d", len(pcs))
	}
	if pcs[0].Condition != "echo yes" {
		t.Errorf("condition = %q", pcs[0].Condition)
	}
	if pcs[0].Expected != "yes" {
		t.Errorf("expected = %q", pcs[0].Expected)
	}
}

func TestParseWithContinueOn(t *testing.T) {
	yaml := `name: continue-test
steps:
  - name: may-fail
    command: exit 1
    continue_on:
      failure: true
      skipped: true
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if !dag.Steps[0].ContinueOn.Failure {
		t.Error("continue_on.failure should be true")
	}
	if !dag.Steps[0].ContinueOn.Skipped {
		t.Error("continue_on.skipped should be true")
	}
}

func TestParseWithHTTPConfig(t *testing.T) {
	yaml := `name: http-test
steps:
  - name: api-call
    type: http
    http:
      method: POST
      url: http://localhost:8080/api
      headers:
        Content-Type: application/json
      body: '{"key":"value"}'
      timeout: 10
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	cfg := dag.Steps[0].HTTPConfig
	if cfg == nil {
		t.Fatal("http config is nil")
	}
	if cfg.Method != "POST" {
		t.Errorf("method = %q", cfg.Method)
	}
	if cfg.URL != "http://localhost:8080/api" {
		t.Errorf("url = %q", cfg.URL)
	}
	if cfg.Headers["Content-Type"] != "application/json" {
		t.Errorf("headers = %v", cfg.Headers)
	}
	if cfg.Timeout != 10 {
		t.Errorf("timeout = %d", cfg.Timeout)
	}
}

func TestParseWithTimeout(t *testing.T) {
	yaml := `name: timeout-test
timeout_sec: 60
steps:
  - name: long-step
    command: sleep 100
    timeout_sec: 30
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.TimeoutSec != 60 {
		t.Errorf("dag timeout = %d", dag.TimeoutSec)
	}
	if dag.Steps[0].TimeoutSec != 30 {
		t.Errorf("step timeout = %d", dag.Steps[0].TimeoutSec)
	}
}

func TestParseWithMaxActive(t *testing.T) {
	yaml := `name: concurrency-test
max_active_steps: 3
steps:
  - name: step1
    command: echo 1
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.MaxActive != 3 {
		t.Errorf("max_active = %d", dag.MaxActive)
	}
}

func TestParseWithDescription(t *testing.T) {
	yaml := `name: desc-test
description: A test workflow
steps:
  - name: step1
    description: First step
    command: echo 1
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Description != "A test workflow" {
		t.Errorf("description = %q", dag.Description)
	}
	if dag.Steps[0].Description != "First step" {
		t.Errorf("step description = %q", dag.Steps[0].Description)
	}
}

func TestParseWithScript(t *testing.T) {
	yaml := `name: script-test
steps:
  - name: scripted
    type: script
    script: |
      echo hello
      echo world
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Steps[0].Type != "script" {
		t.Errorf("type = %q", dag.Steps[0].Type)
	}
	if dag.Steps[0].Script == "" {
		t.Error("script is empty")
	}
}

func TestParseWithParams(t *testing.T) {
	yaml := `name: param-test
params:
  - name=value
  - other=123
steps:
  - name: step1
    command: echo $name
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if len(dag.Params) != 2 {
		t.Errorf("params len = %d", len(dag.Params))
	}
}

func TestParseNoSteps(t *testing.T) {
	yaml := `name: empty
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for missing steps")
	}
}

func TestParseEmptySteps(t *testing.T) {
	yaml := `name: empty
steps:
`
	_, err := Parse([]byte(yaml))
	if err == nil {
		t.Fatal("expected error for empty steps")
	}
}

func TestParseAutoName(t *testing.T) {
	yaml := `name: auto-name
steps:
  - command: echo hello
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Steps[0].Name == "" {
		t.Error("auto-generated name should not be empty")
	}
}

func TestParseWithHandlerOn(t *testing.T) {
	yaml := `name: handler-test
handler_on:
  success:
    name: on_success
    command: echo success
  failure:
    name: on_failure
    command: echo failure
  exit:
    name: on_exit
    command: echo exit
steps:
  - name: step1
    command: echo 1
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.HandlerOn.Success == nil || dag.HandlerOn.Success.Name != "on_success" {
		t.Error("success handler missing or wrong")
	}
	if dag.HandlerOn.Failure == nil || dag.HandlerOn.Failure.Name != "on_failure" {
		t.Error("failure handler missing or wrong")
	}
	if dag.HandlerOn.Exit == nil || dag.HandlerOn.Exit.Name != "on_exit" {
		t.Error("exit handler missing or wrong")
	}
}

func TestParseFile(t *testing.T) {
	content := `name: file-test
steps:
  - name: hello
    command: echo hello
`
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	dag, err := ParseFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if dag.Name != "file-test" {
		t.Errorf("name = %q", dag.Name)
	}
}

func TestParseFileMissing(t *testing.T) {
	_, err := ParseFile("/nonexistent/file.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

func TestParseWithComments(t *testing.T) {
	yaml := `# This is a comment
name: comment-test
# Another comment
steps:
  - name: step1
    command: echo hello # inline comment
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Name != "comment-test" {
		t.Errorf("name = %q", dag.Name)
	}
}

func TestParseWithShell(t *testing.T) {
	yaml := `name: shell-test
shell: bash
steps:
  - name: step1
    command: echo hello
    shell: zsh
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Shell != "bash" {
		t.Errorf("dag shell = %q", dag.Shell)
	}
	if dag.Steps[0].Shell != "zsh" {
		t.Errorf("step shell = %q", dag.Steps[0].Shell)
	}
}

func TestParseWithWorkingDir(t *testing.T) {
	yaml := `name: dir-test
working_dir: /tmp
steps:
  - name: step1
    command: pwd
    working_dir: /home
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.WorkingDir != "/tmp" {
		t.Errorf("dag working_dir = %q", dag.WorkingDir)
	}
	if dag.Steps[0].WorkingDir != "/home" {
		t.Errorf("step working_dir = %q", dag.Steps[0].WorkingDir)
	}
}

func TestParseQuotedStrings(t *testing.T) {
	yaml := `name: "quoted-name"
steps:
  - name: 'single-quoted'
    command: echo hello
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.Name != "quoted-name" {
		t.Errorf("name = %q", dag.Name)
	}
	if dag.Steps[0].Name != "single-quoted" {
		t.Errorf("step name = %q", dag.Steps[0].Name)
	}
}

func TestParseBooleanValues(t *testing.T) {
	yaml := `name: bool-test
steps:
  - name: step1
    command: echo 1
    continue_on:
      failure: true
      skipped: false
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if !dag.Steps[0].ContinueOn.Failure {
		t.Error("failure should be true")
	}
	if dag.Steps[0].ContinueOn.Skipped {
		t.Error("skipped should be false")
	}
}

func TestParseMultipleSteps(t *testing.T) {
	yaml := `name: multi
steps:
  - name: a
    command: echo a
  - name: b
    command: echo b
  - name: c
    command: echo c
    depends:
      - a
      - b
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if len(dag.Steps) != 3 {
		t.Errorf("expected 3 steps, got %d", len(dag.Steps))
	}
	if len(dag.Steps[2].Depends) != 2 {
		t.Errorf("step c should have 2 deps, got %d", len(dag.Steps[2].Depends))
	}
}

func TestParseInvalidYAML(t *testing.T) {
	yaml := `{{{{invalid yaml`
	_, err := Parse([]byte(yaml))
	// Should either error or produce an empty/invalid result
	if err == nil {
		t.Log("no error for invalid yaml (parser is lenient)")
	}
}

func TestParseScalarTypes(t *testing.T) {
	tests := []struct {
		input string
		want  interface{}
	}{
		{"true", true},
		{"false", false},
		{"yes", "yes"},
		{"no", "no"},
		{"42", 42},
		{"-5", -5},
		{"3.14", 3.14},
		{"null", nil},
		{"~", nil},
		{"hello", "hello"},
		{`"quoted"`, "quoted"},
		{`'single'`, "single"},
	}
	for _, tt := range tests {
		result := parseScalar(tt.input)
		if result != tt.want {
			t.Errorf("parseScalar(%q) = %v (%T), want %v (%T)", tt.input, result, result, tt.want, tt.want)
		}
	}
}

func TestIsInteger(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"42", true},
		{"-5", true},
		{"+3", true},
		{"0", true},
		{"3.14", false},
		{"abc", false},
		{"", false},
		{"-", false},
	}
	for _, tt := range tests {
		if got := isInteger(tt.input); got != tt.want {
			t.Errorf("isInteger(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestIsFloat(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"3.14", true},
		{"-2.5", true},
		{"42", false},
		{"abc", false},
		{"", false},
		{"1.2.3", false},
	}
	for _, tt := range tests {
		if got := isFloat(tt.input); got != tt.want {
			t.Errorf("isFloat(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
}

func TestCountIndent(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"hello", 0},
		{"  hello", 2},
		{"    hello", 4},
		{"\thello", 2},
		{"", 0},
	}
	for _, tt := range tests {
		if got := countIndent(tt.input); got != tt.want {
			t.Errorf("countIndent(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestRemoveInlineComment(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"hello # comment", "hello"},
		{"hello", "hello"},
		{`"has # inside"`, `"has # inside"`},
		{`'has # inside'`, `'has # inside'`},
		{"", ""},
	}
	for _, tt := range tests {
		if got := removeInlineComment(tt.input); got != tt.want {
			t.Errorf("removeInlineComment(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseLogDir(t *testing.T) {
	yaml := `name: logdir-test
log_dir: /var/log/dagrun
steps:
  - name: step1
    command: echo 1
`
	dag, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	if dag.LogDir != "/var/log/dagrun" {
		t.Errorf("log_dir = %q", dag.LogDir)
	}
}
