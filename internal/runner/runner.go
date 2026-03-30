// Package runner implements the channel-driven DAG execution engine.
package runner

import (
	"context"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/JSLEEKR/dagrun/internal/dag"
	"github.com/JSLEEKR/dagrun/internal/executor"
	"github.com/JSLEEKR/dagrun/internal/model"
)

// Runner executes a DAG workflow using channel-driven scheduling.
type Runner struct {
	dag       *model.DAG
	graph     *dag.Graph
	env       map[string]string // accumulated environment (including step outputs)
	results   map[string]*model.NodeResult
	mu        sync.RWMutex
	maxActive int
	completed int // protected by mu (H3/M4)
}

// New creates a new Runner for the given DAG.
func New(d *model.DAG, g *dag.Graph) *Runner {
	env := make(map[string]string)
	// Copy DAG-level env
	for k, v := range d.Env {
		env[k] = v
	}
	// Apply params as positional variables
	for i, p := range d.Params {
		parts := strings.SplitN(p, "=", 2)
		if len(parts) == 2 {
			env[parts[0]] = parts[1]
		} else {
			env[fmt.Sprintf("PARAM_%d", i+1)] = p
		}
	}

	maxActive := d.MaxActive
	if maxActive < 0 {
		maxActive = 1 // L8: clamp negative values to 1
	}
	// maxActive == 0 means unlimited

	return &Runner{
		dag:       d,
		graph:     g,
		env:       env,
		results:   make(map[string]*model.NodeResult),
		maxActive: maxActive,
	}
}

// Run executes the DAG and returns the aggregated result.
func (r *Runner) Run(ctx context.Context) model.DAGResult {
	startTime := time.Now()
	result := model.DAGResult{
		Name:      r.dag.Name,
		StartedAt: startTime,
	}

	// Apply DAG-level timeout
	if r.dag.TimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(r.dag.TimeoutSec)*time.Second)
		defer cancel()
	}

	// Channels for the event loop
	type nodeEvent struct {
		name   string
		result model.NodeResult
	}
	readyCh := make(chan string, len(r.graph.Nodes))
	doneCh := make(chan nodeEvent, len(r.graph.Nodes))

	// Seed root nodes
	roots := r.graph.RootNodes()
	for _, root := range roots {
		readyCh <- root.Step.Name
	}

	var wg sync.WaitGroup
	r.mu.Lock()
	r.completed = 0
	r.mu.Unlock()
	active := 0
	total := len(r.graph.Nodes)

	// Pending queue for when maxActive is reached
	var pendingQueue []string

	// Event loop
	for {
		r.mu.RLock()
		done := r.completed >= total
		r.mu.RUnlock()
		if done {
			break
		}

		select {
		case <-ctx.Done():
			// Context cancelled or timed out — abort remaining
			r.mu.Lock()
			for _, n := range r.graph.Nodes {
				if _, done := r.results[n.Step.Name]; !done {
					r.results[n.Step.Name] = &model.NodeResult{
						Name:   n.Step.Name,
						Status: model.NodeAborted,
						Error:  ctx.Err(),
					}
					r.completed++
				}
			}
			r.mu.Unlock()
			result.Status = model.NodeAborted
			result.Error = ctx.Err()

		case name := <-readyCh:
			// Check concurrency limit
			if r.maxActive > 0 && active >= r.maxActive {
				pendingQueue = append(pendingQueue, name)
				continue
			}

			active++
			wg.Add(1)
			go func(stepName string) {
				defer wg.Done()
				node, _ := r.graph.GetNode(stepName)
				nr := r.executeNode(ctx, node)
				doneCh <- nodeEvent{name: stepName, result: nr}
			}(name)

		case event := <-doneCh:
			active--
			r.mu.Lock()
			r.completed++
			r.results[event.name] = &event.result

			// Capture output as env variable
			if event.result.Output != "" {
				node, _ := r.graph.GetNode(event.name)
				if node.Step.Output != "" {
					r.env[node.Step.Output] = event.result.Output
				}
			}
			r.mu.Unlock()

			// Process dependents
			dependents := r.graph.Dependents[event.name]
			for _, depName := range dependents {
				// Skip if already completed
				r.mu.RLock()
				_, alreadyDone := r.results[depName]
				r.mu.RUnlock()
				if alreadyDone {
					continue
				}

				if r.isReady(depName) {
					readyCh <- depName
				} else if r.shouldSkip(depName) {
					// Cascade skip
					r.mu.Lock()
					if _, done := r.results[depName]; !done {
						r.results[depName] = &model.NodeResult{
							Name:   depName,
							Status: model.NodeSkipped,
						}
						r.completed++
					}
					r.mu.Unlock()
					// Cascade further
					r.cascadeSkip(depName)
				}
			}

			// Drain pending queue if under limit
			for len(pendingQueue) > 0 && (r.maxActive <= 0 || active < r.maxActive) {
				next := pendingQueue[0]
				pendingQueue = pendingQueue[1:]
				active++
				wg.Add(1)
				go func(stepName string) {
					defer wg.Done()
					node, _ := r.graph.GetNode(stepName)
					nr := r.executeNode(ctx, node)
					doneCh <- nodeEvent{name: stepName, result: nr}
				}(next)
			}

			// Deadlock detection — read completed under lock (M4)
			// Also check that no pending events exist in doneCh or readyCh
			// to avoid false positives when multiple goroutines finish simultaneously.
			r.mu.RLock()
			currentCompleted := r.completed
			r.mu.RUnlock()
			if active == 0 && currentCompleted < total && len(pendingQueue) == 0 && len(doneCh) == 0 && len(readyCh) == 0 {
				r.mu.Lock()
				for _, n := range r.graph.Nodes {
					if _, done := r.results[n.Step.Name]; !done {
						r.results[n.Step.Name] = &model.NodeResult{
							Name:   n.Step.Name,
							Status: model.NodeAborted,
							Error:  fmt.Errorf("deadlock: no runnable nodes but DAG not finished"),
						}
						r.completed++
					}
				}
				r.mu.Unlock()
			}
		}
	}

	wg.Wait()

	// Build final result
	result.EndedAt = time.Now()
	overallStatus := model.NodeSucceeded
	r.mu.RLock()
	for _, n := range r.graph.Nodes {
		if nr, ok := r.results[n.Step.Name]; ok {
			result.Nodes = append(result.Nodes, *nr)
			if nr.Status == model.NodeFailed || nr.Status == model.NodeTimedOut {
				overallStatus = model.NodeFailed
			} else if nr.Status == model.NodeAborted && overallStatus != model.NodeFailed {
				overallStatus = model.NodeAborted
			}
		}
	}
	r.mu.RUnlock()
	if result.Status == 0 { // not already set by abort
		result.Status = overallStatus
	}

	// Run lifecycle handlers
	r.runHandlers(ctx, result.Status)

	return result
}

// executeNode runs a single step with preconditions, retries, and timeout.
func (r *Runner) executeNode(ctx context.Context, node *dag.Node) model.NodeResult {
	step := node.Step

	// Inherit DAG-level defaults if not set at step level
	if step.Shell == "" && r.dag.Shell != "" {
		step.Shell = r.dag.Shell
	}
	if step.WorkingDir == "" && r.dag.WorkingDir != "" {
		step.WorkingDir = r.dag.WorkingDir
	}

	nr := model.NodeResult{
		Name:      step.Name,
		StartedAt: time.Now(),
	}

	// Apply step timeout
	if step.TimeoutSec > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(step.TimeoutSec)*time.Second)
		defer cancel()
	}

	// Evaluate preconditions
	if !r.evalPreconditions(ctx, step) {
		nr.Status = model.NodeSkipped
		nr.EndedAt = time.Now()
		return nr
	}

	// Build step env (DAG env + step env + captured outputs)
	stepEnv := r.buildStepEnv(step)

	// Determine executor
	execType := step.ResolveType()
	exec, err := executor.New(execType)
	if err != nil {
		nr.Status = model.NodeFailed
		nr.Error = err
		nr.EndedAt = time.Now()
		return nr
	}

	// Execute with retries
	maxRetries := 0
	retryInterval := time.Second
	backoff := 1.0
	if step.RetryPolicy != nil {
		maxRetries = step.RetryPolicy.Limit
		if step.RetryPolicy.IntervalSec > 0 {
			retryInterval = step.RetryPolicy.Interval()
		}
		if step.RetryPolicy.Backoff > 0 {
			backoff = step.RetryPolicy.Backoff
		}
	}

	var result executor.Result
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Wait before retry
			select {
			case <-ctx.Done():
				nr.Status = model.NodeAborted
				nr.Error = ctx.Err()
				nr.EndedAt = time.Now()
				nr.Retries = attempt
				return nr
			case <-time.After(retryInterval):
			}
			retryInterval = time.Duration(float64(retryInterval) * backoff)
		}

		nr.Retries = attempt
		result = exec.Execute(ctx, step, stepEnv)

		if result.Error == nil {
			break
		}

		// Check if context was cancelled during execution
		if ctx.Err() != nil {
			if ctx.Err() == context.DeadlineExceeded {
				nr.Status = model.NodeTimedOut
			} else {
				nr.Status = model.NodeAborted
			}
			nr.Error = result.Error
			nr.Output = result.Output
			nr.ExitCode = result.ExitCode
			nr.EndedAt = time.Now()
			return nr
		}
	}

	nr.Output = result.Output
	nr.ExitCode = result.ExitCode
	nr.EndedAt = time.Now()

	if result.Error != nil {
		nr.Status = model.NodeFailed
		nr.Error = result.Error
	} else {
		nr.Status = model.NodeSucceeded
	}

	return nr
}

// evalPreconditions checks if all preconditions for a step are met.
func (r *Runner) evalPreconditions(ctx context.Context, step model.Step) bool {
	for _, pc := range step.Preconditions {
		if pc.Condition == "" {
			continue
		}

		shell := step.Shell
		if shell == "" {
			shell = r.dag.Shell
		}
		if shell == "" {
			if runtime.GOOS == "windows" {
				shell = "cmd"
			} else {
				shell = "sh"
			}
		}

		cmdExec := executor.CommandExecutor{}
		checkStep := model.Step{
			Name:    step.Name + "_precondition",
			Command: pc.Condition,
			Shell:   shell,
		}
		result := cmdExec.Execute(ctx, checkStep, r.buildStepEnv(step))

		output := strings.TrimSpace(result.Output)
		if pc.Expected != "" {
			matched, err := regexp.MatchString(pc.Expected, output)
			if err != nil || !matched {
				return false
			}
		} else if result.Error != nil {
			return false
		}
	}
	return true
}

// buildStepEnv merges DAG env, step env, and captured outputs.
func (r *Runner) buildStepEnv(step model.Step) map[string]string {
	env := make(map[string]string)
	r.mu.RLock()
	for k, v := range r.env {
		env[k] = v
	}
	r.mu.RUnlock()
	for k, v := range step.Env {
		// Expand variables in values
		env[k] = os.Expand(v, func(key string) string {
			if val, ok := env[key]; ok {
				return val
			}
			return os.Getenv(key)
		})
	}
	return env
}

// isReady checks if all dependencies of a node have succeeded.
func (r *Runner) isReady(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	deps := r.graph.Dependencies[name]
	for _, dep := range deps {
		nr, ok := r.results[dep]
		if !ok {
			return false // not yet completed
		}
		depNode, _ := r.graph.GetNode(dep)
		switch nr.Status {
		case model.NodeSucceeded:
			continue
		case model.NodeFailed, model.NodeTimedOut:
			if depNode.Step.ContinueOn.Failure {
				continue
			}
			return false
		case model.NodeSkipped:
			if depNode.Step.ContinueOn.Skipped {
				continue
			}
			return false
		default:
			return false
		}
	}
	return true
}

// shouldSkip determines if a node should be skipped because its deps failed.
func (r *Runner) shouldSkip(name string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	deps := r.graph.Dependencies[name]
	for _, dep := range deps {
		nr, ok := r.results[dep]
		if !ok {
			return false
		}
		depNode, _ := r.graph.GetNode(dep)
		switch nr.Status {
		case model.NodeSucceeded:
			continue
		case model.NodeFailed, model.NodeTimedOut:
			if !depNode.Step.ContinueOn.Failure {
				return true
			}
		case model.NodeSkipped:
			if !depNode.Step.ContinueOn.Skipped {
				return true
			}
		case model.NodeAborted:
			return true
		}
	}
	return false
}

// cascadeSkip recursively skips all downstream dependents.
// Uses r.completed protected by r.mu (H3/M4).
func (r *Runner) cascadeSkip(name string) {
	dependents := r.graph.Dependents[name]
	for _, depName := range dependents {
		r.mu.Lock()
		if _, done := r.results[depName]; !done {
			r.results[depName] = &model.NodeResult{
				Name:   depName,
				Status: model.NodeSkipped,
			}
			r.completed++
			r.mu.Unlock()
			r.cascadeSkip(depName)
		} else {
			r.mu.Unlock()
		}
	}
}

// runHandlers executes lifecycle handlers (success/failure/exit).
func (r *Runner) runHandlers(ctx context.Context, status model.NodeStatus) {
	var handler *model.Step

	switch status {
	case model.NodeSucceeded:
		handler = r.dag.HandlerOn.Success
	case model.NodeFailed:
		handler = r.dag.HandlerOn.Failure
	}

	if handler != nil {
		r.runHandler(ctx, *handler)
	}

	// Exit handler always runs
	if r.dag.HandlerOn.Exit != nil {
		r.runHandler(ctx, *r.dag.HandlerOn.Exit)
	}
}

// runHandler executes a single lifecycle handler step.
// Logs errors to stderr instead of silently swallowing them (M9).
func (r *Runner) runHandler(ctx context.Context, step model.Step) {
	execType := step.ResolveType()
	exec, err := executor.New(execType)
	if err != nil {
		fmt.Fprintf(os.Stderr, "handler %q: failed to create executor: %v\n", step.Name, err)
		return
	}
	result := exec.Execute(ctx, step, r.buildStepEnv(step))
	if result.Error != nil {
		fmt.Fprintf(os.Stderr, "handler %q: execution error: %v\n", step.Name, result.Error)
	}
}

// GetResults returns the current results snapshot.
func (r *Runner) GetResults() map[string]model.NodeResult {
	r.mu.RLock()
	defer r.mu.RUnlock()
	results := make(map[string]model.NodeResult, len(r.results))
	for k, v := range r.results {
		results[k] = *v
	}
	return results
}
