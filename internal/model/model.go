// Package model defines the core domain types for DAG workflow execution.
package model

import (
	"fmt"
	"time"
)

// NodeStatus represents the execution state of a node.
type NodeStatus int

const (
	NodePending NodeStatus = iota
	NodeRunning
	NodeSucceeded
	NodeFailed
	NodeSkipped
	NodeAborted
	NodeTimedOut
)

// String returns a human-readable status name.
func (s NodeStatus) String() string {
	switch s {
	case NodePending:
		return "pending"
	case NodeRunning:
		return "running"
	case NodeSucceeded:
		return "succeeded"
	case NodeFailed:
		return "failed"
	case NodeSkipped:
		return "skipped"
	case NodeAborted:
		return "aborted"
	case NodeTimedOut:
		return "timed_out"
	default:
		return "unknown"
	}
}

// IsTerminal returns true if this status is a final state.
func (s NodeStatus) IsTerminal() bool {
	switch s {
	case NodeSucceeded, NodeFailed, NodeSkipped, NodeAborted, NodeTimedOut:
		return true
	default:
		return false
	}
}

// RetryPolicy defines retry behavior for a step.
type RetryPolicy struct {
	Limit       int           `yaml:"limit"`
	IntervalSec int           `yaml:"interval_sec"`
	Backoff     float64       `yaml:"backoff"` // multiplier, e.g. 2.0
	MaxInterval time.Duration `yaml:"-"`
}

// Interval returns the base retry interval as a Duration.
func (r RetryPolicy) Interval() time.Duration {
	return time.Duration(r.IntervalSec) * time.Second
}

// ContinueOn defines when downstream steps should continue despite this step's failure.
type ContinueOn struct {
	Failure bool `yaml:"failure"`
	Skipped bool `yaml:"skipped"`
}

// Precondition defines a condition that must be met before a step executes.
type Precondition struct {
	Condition string `yaml:"condition"` // shell command or expression
	Expected  string `yaml:"expected"`  // expected output (regex)
}

// HandlerOn defines lifecycle handlers for the DAG.
type HandlerOn struct {
	Success *Step `yaml:"success,omitempty"`
	Failure *Step `yaml:"failure,omitempty"`
	Exit    *Step `yaml:"exit,omitempty"`
}

// HTTPConfig defines configuration for an HTTP executor step.
type HTTPConfig struct {
	Method  string            `yaml:"method"`
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty"`
	Timeout int               `yaml:"timeout,omitempty"` // seconds
}

// Step defines a single unit of work in a DAG.
type Step struct {
	Name          string            `yaml:"name"`
	Description   string            `yaml:"description,omitempty"`
	Command       string            `yaml:"command,omitempty"`
	Script        string            `yaml:"script,omitempty"`
	Shell         string            `yaml:"shell,omitempty"`
	WorkingDir    string            `yaml:"working_dir,omitempty"`
	Type          string            `yaml:"type,omitempty"` // command, http, script
	Depends       []string          `yaml:"depends,omitempty"`
	Output        string            `yaml:"output,omitempty"` // variable name to capture stdout
	Env           map[string]string `yaml:"env,omitempty"`
	ContinueOn    ContinueOn        `yaml:"continue_on,omitempty"`
	RetryPolicy   *RetryPolicy      `yaml:"retry_policy,omitempty"`
	Preconditions []Precondition    `yaml:"preconditions,omitempty"`
	TimeoutSec    int               `yaml:"timeout_sec,omitempty"`
	HTTPConfig    *HTTPConfig       `yaml:"http,omitempty"`
}

// ResolveType determines the executor type from step fields.
func (s *Step) ResolveType() string {
	if s.Type != "" {
		return s.Type
	}
	if s.HTTPConfig != nil {
		return "http"
	}
	if s.Script != "" {
		return "script"
	}
	return "command"
}

// DAG represents a directed acyclic graph workflow.
type DAG struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description,omitempty"`
	Steps       []Step            `yaml:"steps"`
	Env         map[string]string `yaml:"env,omitempty"`
	Params      []string          `yaml:"params,omitempty"`
	Shell       string            `yaml:"shell,omitempty"`
	WorkingDir  string            `yaml:"working_dir,omitempty"`
	HandlerOn   HandlerOn         `yaml:"handler_on,omitempty"`
	MaxActive   int               `yaml:"max_active_steps,omitempty"`
	TimeoutSec  int               `yaml:"timeout_sec,omitempty"`
	LogDir      string            `yaml:"log_dir,omitempty"`
}

// NodeResult holds the execution result of a single node.
type NodeResult struct {
	Name      string
	Status    NodeStatus
	Output    string // captured stdout
	Error     error
	StartedAt time.Time
	EndedAt   time.Time
	Retries   int
	ExitCode  int
}

// Duration returns how long the node took to execute.
func (r NodeResult) Duration() time.Duration {
	if r.EndedAt.IsZero() || r.StartedAt.IsZero() {
		return 0
	}
	return r.EndedAt.Sub(r.StartedAt)
}

// DAGResult holds the aggregated result of a DAG execution.
type DAGResult struct {
	Name      string
	Status    NodeStatus
	Nodes     []NodeResult
	StartedAt time.Time
	EndedAt   time.Time
	Error     error
}

// Duration returns total DAG execution time.
func (r DAGResult) Duration() time.Duration {
	if r.EndedAt.IsZero() || r.StartedAt.IsZero() {
		return 0
	}
	return r.EndedAt.Sub(r.StartedAt)
}

// Summary returns a formatted summary of the DAG execution.
func (r DAGResult) Summary() string {
	succeeded, failed, skipped, aborted := 0, 0, 0, 0
	for _, n := range r.Nodes {
		switch n.Status {
		case NodeSucceeded:
			succeeded++
		case NodeFailed, NodeTimedOut:
			failed++
		case NodeSkipped:
			skipped++
		case NodeAborted:
			aborted++
		}
	}
	return fmt.Sprintf("DAG %q: %s (total=%d succeeded=%d failed=%d skipped=%d aborted=%d duration=%s)",
		r.Name, r.Status, len(r.Nodes), succeeded, failed, skipped, aborted, r.Duration().Round(time.Millisecond))
}
