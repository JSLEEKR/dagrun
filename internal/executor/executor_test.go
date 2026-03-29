package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/JSLEEKR/dagrun/internal/model"
)

func TestNewCommand(t *testing.T) {
	e, err := New("command")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*CommandExecutor); !ok {
		t.Error("expected CommandExecutor")
	}
}

func TestNewEmpty(t *testing.T) {
	e, err := New("")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*CommandExecutor); !ok {
		t.Error("expected CommandExecutor for empty type")
	}
}

func TestNewHTTP(t *testing.T) {
	e, err := New("http")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*HTTPExecutor); !ok {
		t.Error("expected HTTPExecutor")
	}
}

func TestNewScript(t *testing.T) {
	e, err := New("script")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := e.(*ScriptExecutor); !ok {
		t.Error("expected ScriptExecutor")
	}
}

func TestNewUnknown(t *testing.T) {
	_, err := New("docker")
	if err == nil {
		t.Fatal("expected error for unknown executor type")
	}
}

func TestCommandExecutorEcho(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command test skipped on Windows")
	}
	e := &CommandExecutor{}
	step := model.Step{Name: "echo", Command: "echo hello"}
	result := e.Execute(context.Background(), step, nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != "hello" {
		t.Errorf("output = %q, want %q", result.Output, "hello")
	}
	if result.ExitCode != 0 {
		t.Errorf("exit code = %d, want 0", result.ExitCode)
	}
}

func TestCommandExecutorEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command test skipped on Windows")
	}
	e := &CommandExecutor{}
	step := model.Step{Name: "env", Command: "echo $MY_VAR"}
	env := map[string]string{"MY_VAR": "test_value"}
	result := e.Execute(context.Background(), step, env)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != "test_value" {
		t.Errorf("output = %q, want %q", result.Output, "test_value")
	}
}

func TestCommandExecutorFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command test skipped on Windows")
	}
	e := &CommandExecutor{}
	step := model.Step{Name: "fail", Command: "exit 42"}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for non-zero exit")
	}
	if result.ExitCode != 42 {
		t.Errorf("exit code = %d, want 42", result.ExitCode)
	}
}

func TestCommandExecutorEmpty(t *testing.T) {
	e := &CommandExecutor{}
	step := model.Step{Name: "empty", Command: ""}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for empty command")
	}
}

func TestCommandExecutorTimeout(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command test skipped on Windows")
	}
	e := &CommandExecutor{}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	step := model.Step{Name: "timeout", Command: "sleep 10"}
	result := e.Execute(ctx, step, nil)
	if result.Error == nil {
		t.Fatal("expected timeout error")
	}
}

func TestCommandExecutorMultiOutput(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell command test skipped on Windows")
	}
	e := &CommandExecutor{}
	step := model.Step{Name: "multi", Command: "echo line1; echo line2"}
	result := e.Execute(context.Background(), step, nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if !strings.Contains(result.Output, "line1") || !strings.Contains(result.Output, "line2") {
		t.Errorf("output = %q, should contain both lines", result.Output)
	}
}

func TestHTTPExecutorGET(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Errorf("method = %q, want GET", r.Method)
		}
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer server.Close()

	e := &HTTPExecutor{}
	step := model.Step{
		Name: "http-get",
		HTTPConfig: &model.HTTPConfig{
			Method: "GET",
			URL:    server.URL,
		},
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != `{"status":"ok"}` {
		t.Errorf("output = %q", result.Output)
	}
}

func TestHTTPExecutorPOST(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Errorf("method = %q, want POST", r.Method)
		}
		if r.Header.Get("Content-Type") != "application/json" {
			t.Errorf("Content-Type = %q", r.Header.Get("Content-Type"))
		}
		fmt.Fprint(w, "created")
	}))
	defer server.Close()

	e := &HTTPExecutor{}
	step := model.Step{
		Name: "http-post",
		HTTPConfig: &model.HTTPConfig{
			Method:  "POST",
			URL:     server.URL,
			Headers: map[string]string{"Content-Type": "application/json"},
			Body:    `{"key":"value"}`,
		},
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != "created" {
		t.Errorf("output = %q", result.Output)
	}
}

func TestHTTPExecutorError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		fmt.Fprint(w, "internal error")
	}))
	defer server.Close()

	e := &HTTPExecutor{}
	step := model.Step{
		Name:       "http-err",
		HTTPConfig: &model.HTTPConfig{URL: server.URL},
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for 500 response")
	}
	if result.ExitCode != 500 {
		t.Errorf("exit code = %d, want 500", result.ExitCode)
	}
}

func TestHTTPExecutorNilConfig(t *testing.T) {
	e := &HTTPExecutor{}
	step := model.Step{Name: "nil-http"}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for nil HTTP config")
	}
}

func TestHTTPExecutorEmptyURL(t *testing.T) {
	e := &HTTPExecutor{}
	step := model.Step{
		Name:       "empty-url",
		HTTPConfig: &model.HTTPConfig{URL: ""},
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for empty URL")
	}
}

func TestHTTPExecutorDefaultMethod(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, r.Method)
	}))
	defer server.Close()

	e := &HTTPExecutor{}
	step := model.Step{
		Name:       "default-method",
		HTTPConfig: &model.HTTPConfig{URL: server.URL},
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != "GET" {
		t.Errorf("default method should be GET, got %q", result.Output)
	}
}

func TestHTTPExecutorEnvExpansion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	}))
	defer server.Close()

	e := &HTTPExecutor{}
	step := model.Step{
		Name: "env-url",
		HTTPConfig: &model.HTTPConfig{
			URL: "${BASE_URL}/api",
		},
	}
	env := map[string]string{"BASE_URL": server.URL}
	result := e.Execute(context.Background(), step, env)
	// Should try to connect even if URL expansion happens
	// Just verify no panic
	_ = result
}

func TestScriptExecutor(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("script test skipped on Windows")
	}
	e := &ScriptExecutor{}
	step := model.Step{
		Name:   "script",
		Script: "#!/bin/sh\necho script_output",
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != "script_output" {
		t.Errorf("output = %q, want %q", result.Output, "script_output")
	}
}

func TestScriptExecutorEmpty(t *testing.T) {
	e := &ScriptExecutor{}
	step := model.Step{Name: "empty-script", Script: ""}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for empty script")
	}
}

func TestScriptExecutorWithEnv(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("script test skipped on Windows")
	}
	e := &ScriptExecutor{}
	step := model.Step{
		Name:   "script-env",
		Script: "#!/bin/sh\necho $MY_SCRIPT_VAR",
	}
	env := map[string]string{"MY_SCRIPT_VAR": "from_env"}
	result := e.Execute(context.Background(), step, env)
	if result.Error != nil {
		t.Fatalf("unexpected error: %v", result.Error)
	}
	if result.Output != "from_env" {
		t.Errorf("output = %q, want %q", result.Output, "from_env")
	}
}

func TestScriptExecutorFailure(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("script test skipped on Windows")
	}
	e := &ScriptExecutor{}
	step := model.Step{
		Name:   "script-fail",
		Script: "#!/bin/sh\nexit 7",
	}
	result := e.Execute(context.Background(), step, nil)
	if result.Error == nil {
		t.Fatal("expected error for failed script")
	}
	if result.ExitCode != 7 {
		t.Errorf("exit code = %d, want 7", result.ExitCode)
	}
}

func TestBuildEnv(t *testing.T) {
	env := buildEnv(map[string]string{"TEST_KEY": "test_val"})
	found := false
	for _, e := range env {
		if e == "TEST_KEY=test_val" {
			found = true
			break
		}
	}
	if !found {
		t.Error("buildEnv should include custom env var")
	}
}

func TestBuildEnvNil(t *testing.T) {
	env := buildEnv(nil)
	if len(env) == 0 {
		t.Error("buildEnv with nil should still have os.Environ")
	}
}
