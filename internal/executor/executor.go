// Package executor provides the step execution interface and built-in executors.
package executor

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/JSLEEKR/dagrun/internal/model"
)

// Result holds the output of an executor run.
type Result struct {
	Output   string
	ExitCode int
	Error    error
}

// Executor defines the interface for step execution.
type Executor interface {
	Execute(ctx context.Context, step model.Step, env map[string]string) Result
}

// New creates an executor based on the step type.
func New(stepType string) (Executor, error) {
	switch stepType {
	case "command", "":
		return &CommandExecutor{}, nil
	case "http":
		return &HTTPExecutor{}, nil
	case "script":
		return &ScriptExecutor{}, nil
	default:
		return nil, fmt.Errorf("unknown executor type: %q", stepType)
	}
}

// buildEnv merges environment variables into os.Environ format.
// Deduplicates by replacing existing keys instead of appending (H4).
func buildEnv(env map[string]string) []string {
	osEnv := os.Environ()
	if len(env) == 0 {
		return osEnv
	}
	// Build index of existing keys for O(1) lookup
	keyIndex := make(map[string]int, len(osEnv))
	for i, e := range osEnv {
		if idx := strings.IndexByte(e, '='); idx >= 0 {
			keyIndex[strings.ToUpper(e[:idx])] = i
		}
	}
	for k, v := range env {
		entry := k + "=" + v
		if idx, exists := keyIndex[strings.ToUpper(k)]; exists {
			osEnv[idx] = entry
		} else {
			osEnv = append(osEnv, entry)
			keyIndex[strings.ToUpper(k)] = len(osEnv) - 1
		}
	}
	return osEnv
}

// CommandExecutor runs shell commands.
type CommandExecutor struct{}

func (e *CommandExecutor) Execute(ctx context.Context, step model.Step, env map[string]string) Result {
	if step.Command == "" {
		return Result{Error: fmt.Errorf("command is empty for step %q", step.Name)}
	}

	shell := step.Shell
	if shell == "" {
		if runtime.GOOS == "windows" {
			shell = "cmd"
		} else {
			shell = "sh"
		}
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" && (shell == "cmd" || shell == "cmd.exe") {
		cmd = exec.CommandContext(ctx, "cmd", "/C", step.Command)
	} else {
		cmd = exec.CommandContext(ctx, shell, "-c", step.Command)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = buildEnv(env)
	if step.WorkingDir != "" {
		cmd.Dir = step.WorkingDir
	}

	err := cmd.Run()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() == context.DeadlineExceeded {
			return Result{
				Output:   stdout.String(),
				ExitCode: 124, // timeout convention
				Error:    fmt.Errorf("step %q timed out: %w", step.Name, ctx.Err()),
			}
		} else {
			return Result{
				Output:   stdout.String(),
				ExitCode: -1,
				Error:    fmt.Errorf("step %q failed: %w (stderr: %s)", step.Name, err, stderr.String()),
			}
		}
	}

	if exitCode != 0 {
		return Result{
			Output:   stdout.String(),
			ExitCode: exitCode,
			Error:    fmt.Errorf("step %q exited with code %d (stderr: %s)", step.Name, exitCode, stderr.String()),
		}
	}

	return Result{
		Output:   strings.TrimRight(stdout.String(), "\n\r"),
		ExitCode: 0,
	}
}

// HTTPExecutor performs HTTP requests.
type HTTPExecutor struct{}

func (e *HTTPExecutor) Execute(ctx context.Context, step model.Step, env map[string]string) Result {
	cfg := step.HTTPConfig
	if cfg == nil {
		return Result{Error: fmt.Errorf("step %q: http config is nil", step.Name)}
	}
	if cfg.URL == "" {
		return Result{Error: fmt.Errorf("step %q: http url is empty", step.Name)}
	}

	method := strings.ToUpper(cfg.Method)
	if method == "" {
		method = "GET"
	}

	// Expand env vars in URL
	url := os.Expand(cfg.URL, func(key string) string {
		if v, ok := env[key]; ok {
			return v
		}
		return os.Getenv(key)
	})

	var body io.Reader
	if cfg.Body != "" {
		expanded := os.Expand(cfg.Body, func(key string) string {
			if v, ok := env[key]; ok {
				return v
			}
			return os.Getenv(key)
		})
		body = strings.NewReader(expanded)
	}

	timeout := 30 * time.Second
	if cfg.Timeout > 0 {
		timeout = time.Duration(cfg.Timeout) * time.Second
	}

	client := &http.Client{Timeout: timeout}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return Result{Error: fmt.Errorf("step %q: failed to create request: %w", step.Name, err)}
	}

	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return Result{Error: fmt.Errorf("step %q: http request failed: %w", step.Name, err)}
	}
	defer resp.Body.Close()

	// M6: Limit response body to 10MB to prevent OOM
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10*1024*1024))
	if err != nil {
		return Result{Error: fmt.Errorf("step %q: failed to read response: %w", step.Name, err)}
	}

	if resp.StatusCode >= 400 {
		return Result{
			Output:   string(respBody),
			ExitCode: resp.StatusCode,
			Error:    fmt.Errorf("step %q: HTTP %d %s", step.Name, resp.StatusCode, resp.Status),
		}
	}

	return Result{
		Output:   string(respBody),
		ExitCode: 0,
	}
}

// ScriptExecutor writes a script to a temp file and executes it.
type ScriptExecutor struct{}

func (e *ScriptExecutor) Execute(ctx context.Context, step model.Step, env map[string]string) Result {
	if step.Script == "" {
		return Result{Error: fmt.Errorf("step %q: script is empty", step.Name)}
	}

	shell := step.Shell
	if shell == "" {
		if runtime.GOOS == "windows" {
			shell = "cmd"
		} else {
			shell = "sh"
		}
	}

	// Write script to temp file
	ext := ".sh"
	if runtime.GOOS == "windows" && (shell == "cmd" || shell == "cmd.exe") {
		ext = ".bat"
	}
	tmpFile, err := os.CreateTemp("", "dagrun-script-*"+ext)
	if err != nil {
		return Result{Error: fmt.Errorf("step %q: failed to create temp file: %w", step.Name, err)}
	}
	tmpPath := tmpFile.Name()

	if _, err := tmpFile.WriteString(step.Script); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return Result{Error: fmt.Errorf("step %q: failed to write script: %w", step.Name, err)}
	}
	tmpFile.Close()

	if runtime.GOOS != "windows" {
		os.Chmod(tmpPath, 0o700) // H5: restrict permissions
	}

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" && (shell == "cmd" || shell == "cmd.exe") {
		cmd = exec.CommandContext(ctx, "cmd", "/C", tmpPath)
	} else {
		cmd = exec.CommandContext(ctx, shell, tmpPath)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.Env = buildEnv(env)
	if step.WorkingDir != "" {
		cmd.Dir = step.WorkingDir
	}

	err = cmd.Run()
	// H5: Remove temp file AFTER cmd.Run completes
	os.Remove(tmpPath)
	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else if ctx.Err() == context.DeadlineExceeded {
			return Result{
				Output:   stdout.String(),
				ExitCode: 124,
				Error:    fmt.Errorf("step %q script timed out: %w", step.Name, ctx.Err()),
			}
		} else {
			return Result{
				Output:   stdout.String(),
				ExitCode: -1,
				Error:    fmt.Errorf("step %q script failed: %w (stderr: %s)", step.Name, err, stderr.String()),
			}
		}
	}

	if exitCode != 0 {
		return Result{
			Output:   stdout.String(),
			ExitCode: exitCode,
			Error:    fmt.Errorf("step %q script exited with code %d (stderr: %s)", step.Name, exitCode, stderr.String()),
		}
	}

	return Result{
		Output:   strings.TrimRight(stdout.String(), "\n\r"),
		ExitCode: 0,
	}
}
