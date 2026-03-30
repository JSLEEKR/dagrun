// Package cli implements the command-line interface for dagrun.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/JSLEEKR/dagrun/internal/dag"
	"github.com/JSLEEKR/dagrun/internal/model"
	"github.com/JSLEEKR/dagrun/internal/parser"
	"github.com/JSLEEKR/dagrun/internal/runner"
)

// App represents the CLI application.
type App struct {
	stdout io.Writer
	stderr io.Writer
}

// New creates a new App with the given output writers.
func New(stdout, stderr io.Writer) *App {
	return &App{stdout: stdout, stderr: stderr}
}

// Run parses args and dispatches to the appropriate subcommand.
func (a *App) Run(args []string) int {
	if len(args) < 1 {
		a.printUsage()
		return 1
	}

	switch args[0] {
	case "run":
		return a.cmdRun(args[1:])
	case "validate":
		return a.cmdValidate(args[1:])
	case "status":
		return a.cmdStatus(args[1:])
	case "dot":
		return a.cmdDot(args[1:])
	case "version":
		return a.cmdVersion()
	case "help", "-h", "--help":
		a.printUsage()
		return 0
	default:
		fmt.Fprintf(a.stderr, "unknown command: %s\n\n", args[0])
		a.printUsage()
		return 1
	}
}

func (a *App) printUsage() {
	fmt.Fprintln(a.stdout, `dagrun - lightweight DAG workflow runner

Usage:
  dagrun <command> [options]

Commands:
  run       Execute a workflow file
  validate  Validate a workflow file without executing
  status    Show workflow execution status (dry-run)
  dot       Generate DOT graph visualization
  version   Show version information
  help      Show this help message

Use "dagrun <command> --help" for more information about a command.`)
}

// cmdRun executes a workflow.
func (a *App) cmdRun(args []string) int {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	fs.SetOutput(a.stderr)
	verbose := fs.Bool("v", false, "verbose output")
	jsonOutput := fs.Bool("json", false, "output results as JSON")
	timeout := fs.Int("timeout", 0, "global timeout in seconds (overrides YAML)")
	params := fs.String("params", "", "comma-separated key=value parameters")

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(a.stderr, "usage: dagrun run [options] <workflow.yaml>")
		return 1
	}

	filePath := fs.Arg(0)

	d, err := parser.ParseFile(filePath)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	// Apply CLI params
	if *params != "" {
		for _, p := range strings.Split(*params, ",") {
			d.Params = append(d.Params, strings.TrimSpace(p))
		}
	}

	// Apply CLI timeout override
	if *timeout > 0 {
		d.TimeoutSec = *timeout
	}

	g, err := dag.Build(d)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	if *verbose {
		fmt.Fprintf(a.stdout, "Executing %q (%d steps)\n", d.Name, len(d.Steps))
		fmt.Fprintf(a.stdout, "Topological order: %v\n", g.TopoOrder)
	}

	// Setup signal handling
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh) // M5: prevent goroutine leak
	go func() {
		<-sigCh
		if *verbose {
			fmt.Fprintln(a.stderr, "\nReceived interrupt, cancelling...")
		}
		cancel()
	}()

	r := runner.New(d, g)
	result := r.Run(ctx)

	if *jsonOutput {
		a.printJSON(result)
	} else {
		a.printResult(result, *verbose)
	}

	if result.Status == model.NodeSucceeded {
		return 0
	}
	return 1
}

// cmdValidate validates a workflow file.
func (a *App) cmdValidate(args []string) int {
	fs := flag.NewFlagSet("validate", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(a.stderr, "usage: dagrun validate <workflow.yaml>")
		return 1
	}

	filePath := fs.Arg(0)

	d, err := parser.ParseFile(filePath)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	g, err := dag.Build(d)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.stdout, "Valid workflow %q\n", d.Name)
	fmt.Fprintf(a.stdout, "  Steps:      %d\n", len(d.Steps))
	fmt.Fprintf(a.stdout, "  Root nodes: %d\n", len(g.RootNodes()))
	fmt.Fprintf(a.stdout, "  Topo order: %v\n", g.TopoOrder)

	// Check for isolated nodes
	isolated := 0
	for _, n := range g.Nodes {
		name := n.Step.Name
		if len(g.Dependencies[name]) == 0 && len(g.Dependents[name]) == 0 && len(g.Nodes) > 1 {
			isolated++
			fmt.Fprintf(a.stdout, "  Warning: step %q has no dependencies or dependents\n", name)
		}
	}

	return 0
}

// cmdStatus does a dry-run showing the execution plan.
func (a *App) cmdStatus(args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(a.stderr, "usage: dagrun status <workflow.yaml>")
		return 1
	}

	filePath := fs.Arg(0)

	d, err := parser.ParseFile(filePath)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	g, err := dag.Build(d)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	fmt.Fprintf(a.stdout, "Workflow: %s\n", d.Name)
	if d.Description != "" {
		fmt.Fprintf(a.stdout, "Description: %s\n", d.Description)
	}
	fmt.Fprintln(a.stdout)

	fmt.Fprintln(a.stdout, "Execution Plan:")
	for i, name := range g.TopoOrder {
		node, _ := g.GetNode(name)
		deps := g.Dependencies[name]
		depStr := ""
		if len(deps) > 0 {
			depStr = fmt.Sprintf(" (depends: %s)", strings.Join(deps, ", "))
		}
		execType := node.Step.ResolveType()
		fmt.Fprintf(a.stdout, "  %d. %s [%s]%s\n", i+1, name, execType, depStr)
	}

	return 0
}

// cmdDot generates a DOT graph.
func (a *App) cmdDot(args []string) int {
	fs := flag.NewFlagSet("dot", flag.ContinueOnError)
	fs.SetOutput(a.stderr)

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if fs.NArg() < 1 {
		fmt.Fprintln(a.stderr, "usage: dagrun dot <workflow.yaml>")
		return 1
	}

	filePath := fs.Arg(0)

	d, err := parser.ParseFile(filePath)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	g, err := dag.Build(d)
	if err != nil {
		fmt.Fprintf(a.stderr, "error: %v\n", err)
		return 1
	}

	fmt.Fprint(a.stdout, g.ToDot(d.Name))
	return 0
}

// cmdVersion shows version info.
func (a *App) cmdVersion() int {
	fmt.Fprintln(a.stdout, "dagrun v1.0.0")
	return 0
}

// printResult formats the execution result for humans.
func (a *App) printResult(result model.DAGResult, verbose bool) {
	fmt.Fprintln(a.stdout)
	fmt.Fprintf(a.stdout, "=== %s ===\n", result.Summary())
	fmt.Fprintln(a.stdout)

	for _, nr := range result.Nodes {
		status := statusIcon(nr.Status)
		dur := nr.Duration().Round(time.Millisecond)
		line := fmt.Sprintf("  %s %s (%s, %s)", status, nr.Name, nr.Status, dur)
		if nr.Retries > 0 {
			line += fmt.Sprintf(" [retries=%d]", nr.Retries)
		}
		fmt.Fprintln(a.stdout, line)

		if verbose && nr.Output != "" {
			fmt.Fprintf(a.stdout, "    output: %s\n", truncate(nr.Output, 200))
		}
		if nr.Error != nil {
			fmt.Fprintf(a.stdout, "    error: %v\n", nr.Error)
		}
	}
	fmt.Fprintln(a.stdout)
}

// printJSON outputs results as JSON.
func (a *App) printJSON(result model.DAGResult) {
	type jsonNode struct {
		Name     string  `json:"name"`
		Status   string  `json:"status"`
		Output   string  `json:"output,omitempty"`
		Error    string  `json:"error,omitempty"`
		Duration float64 `json:"duration_ms"`
		Retries  int     `json:"retries,omitempty"`
		ExitCode int     `json:"exit_code"`
	}
	type jsonResult struct {
		Name     string     `json:"name"`
		Status   string     `json:"status"`
		Duration float64    `json:"duration_ms"`
		Nodes    []jsonNode `json:"nodes"`
		Error    string     `json:"error,omitempty"`
	}

	jr := jsonResult{
		Name:     result.Name,
		Status:   result.Status.String(),
		Duration: float64(result.Duration().Milliseconds()),
	}
	if result.Error != nil {
		jr.Error = result.Error.Error()
	}
	for _, nr := range result.Nodes {
		jn := jsonNode{
			Name:     nr.Name,
			Status:   nr.Status.String(),
			Output:   nr.Output,
			Duration: float64(nr.Duration().Milliseconds()),
			Retries:  nr.Retries,
			ExitCode: nr.ExitCode,
		}
		if nr.Error != nil {
			jn.Error = nr.Error.Error()
		}
		jr.Nodes = append(jr.Nodes, jn)
	}

	enc := json.NewEncoder(a.stdout)
	enc.SetIndent("", "  ")
	enc.Encode(jr)
}

func statusIcon(s model.NodeStatus) string {
	switch s {
	case model.NodeSucceeded:
		return "[OK]"
	case model.NodeFailed:
		return "[FAIL]"
	case model.NodeSkipped:
		return "[SKIP]"
	case model.NodeAborted:
		return "[ABORT]"
	case model.NodeTimedOut:
		return "[TIMEOUT]"
	case model.NodeRunning:
		return "[RUN]"
	default:
		return "[?]"
	}
}

func truncate(s string, max int) string {
	// L6: Use rune count instead of byte length
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	return string(runes[:max]) + "..."
}
