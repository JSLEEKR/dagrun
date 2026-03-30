# dagrun

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
[![License](https://img.shields.io/badge/License-GPL--3.0-blue?style=for-the-badge)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-146-blue?style=for-the-badge)](https://github.com/JSLEEKR/dagrun)
[![Zero Dependencies](https://img.shields.io/badge/Dependencies-0-brightgreen?style=for-the-badge)](go.mod)

A lightweight DAG (Directed Acyclic Graph) workflow runner for defining and executing multi-step workflows with dependency resolution. Define workflows in YAML, and dagrun handles parallel execution, dependency ordering, retries, and error propagation.

> Inspired by [dagu](https://github.com/dagu-org/dagu). Reimplemented from scratch with zero external dependencies using only Go's standard library.

---

## Why This Exists

[dagu](https://github.com/dagu-org/dagu) is a powerful DAG workflow engine with 3.2K+ stars. However, it has grown to include 60+ dependencies (Docker SDK, Tailscale, LLM agents, Slack/Telegram bots, etc.), a GPL-3.0 license, and scope that extends far beyond its core value proposition.

**dagrun** strips away the complexity and delivers the essential DAG execution engine:

| Feature | dagu | dagrun |
|---------|------|--------|
| Dependencies | 60+ | **0** (stdlib only) |
| License | GPL-3.0 | **GPL-3.0** |
| Binary size | Large (Docker, Tailscale, etc.) | **Minimal** |
| Executor types | 19+ | **3** (command, HTTP, script) |
| Web UI | Yes | No (CLI-focused) |
| LLM/AI agent | Yes | No |
| Docker executor | Yes | No |
| DAG engine | Yes | **Yes** |
| Parallel execution | Yes | **Yes** |
| Retry with backoff | Yes | **Yes** |
| Output passing | Yes | **Yes** |
| Preconditions | Yes | **Yes** |
| Lifecycle handlers | Yes | **Yes** |

---

## Quick Start

### Installation

```bash
go install github.com/JSLEEKR/dagrun@latest
```

Or build from source:

```bash
git clone https://github.com/JSLEEKR/dagrun.git
cd dagrun
go build -o dagrun .
```

### Your First Workflow

Create `hello.yaml`:

```yaml
name: hello-world
description: A simple workflow demo

steps:
  - name: greet
    command: echo "Hello from dagrun!"

  - name: timestamp
    command: date

  - name: done
    command: echo "All steps completed"
    depends:
      - greet
      - timestamp
```

Run it:

```bash
dagrun run hello.yaml
```

Output:

```
=== DAG "hello-world": succeeded (total=3 succeeded=3 failed=0 skipped=0 aborted=0 duration=15ms) ===

  [OK] greet (succeeded, 5ms)
  [OK] timestamp (succeeded, 6ms)
  [OK] done (succeeded, 4ms)
```

---

## Features

### 1. YAML Workflow Definition

Define workflows declaratively with steps, dependencies, environment variables, and more:

```yaml
name: data-pipeline
description: ETL pipeline with parallel extraction
max_active_steps: 4
timeout_sec: 300

env:
  DATA_DIR: /tmp/data
  LOG_LEVEL: info

params:
  - DATE=2024-01-01

steps:
  - name: extract-users
    command: ./extract.sh users $DATE
    output: USER_COUNT

  - name: extract-orders
    command: ./extract.sh orders $DATE
    output: ORDER_COUNT

  - name: transform
    command: ./transform.sh $USER_COUNT $ORDER_COUNT
    depends:
      - extract-users
      - extract-orders

  - name: load
    command: ./load.sh
    depends:
      - transform
    retry_policy:
      limit: 3
      interval_sec: 5
      backoff: 2.0
```

### 2. DAG Dependency Resolution

dagrun uses **Kahn's algorithm** for topological sorting with cycle detection:

- Steps with no dependencies run in parallel
- Steps wait for all upstream dependencies to complete
- Cycles are detected at build time with clear error messages
- Failed steps cascade: downstream steps are automatically skipped

```yaml
steps:
  - name: build
    command: make build

  - name: test-unit
    command: make test-unit
    depends: [build]

  - name: test-integration
    command: make test-integration
    depends: [build]

  - name: deploy
    command: make deploy
    depends: [test-unit, test-integration]
```

Execution order: `build` -> `test-unit` + `test-integration` (parallel) -> `deploy`

### 3. Three Executor Types

#### Command Executor (default)

Runs shell commands:

```yaml
- name: list-files
  command: ls -la /tmp
  shell: bash  # optional, defaults to sh
  working_dir: /home/user
```

#### HTTP Executor

Makes HTTP requests:

```yaml
- name: health-check
  type: http
  http:
    method: GET
    url: http://localhost:8080/health
    headers:
      Authorization: "Bearer ${API_TOKEN}"
    timeout: 10
```

#### Script Executor

Runs multi-line scripts:

```yaml
- name: complex-task
  type: script
  script: |
    #!/bin/bash
    set -e
    echo "Starting complex operation..."
    for i in $(seq 1 5); do
      echo "Step $i"
      sleep 1
    done
    echo "Done!"
```

### 4. Output Capture and Variable Passing

Capture step output and pass it to downstream steps:

```yaml
steps:
  - name: get-version
    command: cat VERSION
    output: APP_VERSION

  - name: build
    command: docker build -t myapp:${APP_VERSION} .
    depends: [get-version]

  - name: tag
    command: echo "Tagged version ${APP_VERSION}"
    depends: [build]
```

### 5. Retry with Exponential Backoff

Configure retry policies for unreliable steps:

```yaml
- name: deploy
  command: ./deploy.sh
  retry_policy:
    limit: 5          # max retries
    interval_sec: 2   # initial wait between retries
    backoff: 2.0      # multiplier (2s, 4s, 8s, 16s, 32s)
```

### 6. Preconditions

Guard step execution with conditions:

```yaml
- name: deploy-prod
  command: ./deploy.sh production
  preconditions:
    - condition: echo $BRANCH
      expected: main
    - condition: test -f build/app.tar.gz
```

If any precondition fails, the step is **skipped** (not failed).

### 7. Continue on Failure

Allow downstream steps to run even if this step fails:

```yaml
- name: optional-step
  command: ./optional-check.sh
  continue_on:
    failure: true
    skipped: true

- name: next-step
  command: echo "runs even if optional-step fails"
  depends: [optional-step]
```

### 8. Concurrency Control

Limit parallel execution:

```yaml
name: resource-heavy
max_active_steps: 2  # at most 2 steps run simultaneously

steps:
  - name: job-a
    command: heavy-process-a
  - name: job-b
    command: heavy-process-b
  - name: job-c
    command: heavy-process-c
  - name: job-d
    command: heavy-process-d
```

### 9. Lifecycle Handlers

Run actions on workflow success, failure, or exit:

```yaml
name: monitored-workflow
handler_on:
  success:
    name: notify-success
    command: curl -X POST https://hooks.slack.com/... -d '{"text":"Workflow succeeded"}'
  failure:
    name: notify-failure
    command: curl -X POST https://hooks.slack.com/... -d '{"text":"Workflow FAILED"}'
  exit:
    name: cleanup
    command: rm -rf /tmp/work

steps:
  - name: process
    command: ./process.sh
```

### 10. Timeouts

Set timeouts at both DAG and step level:

```yaml
name: time-limited
timeout_sec: 600  # overall DAG timeout: 10 minutes

steps:
  - name: quick-check
    command: ./check.sh
    timeout_sec: 30  # step-level timeout

  - name: long-process
    command: ./process.sh
    timeout_sec: 300
    depends: [quick-check]
```

---

## CLI Reference

### `dagrun run`

Execute a workflow:

```bash
dagrun run [options] <workflow.yaml>

Options:
  -v          Verbose output (shows execution details)
  -json       Output results as JSON
  -timeout N  Override DAG timeout (seconds)
  -params     Comma-separated key=value parameters
```

Examples:

```bash
# Basic run
dagrun run pipeline.yaml

# Verbose with parameters
dagrun run -v -params "ENV=staging,VERSION=2.1" deploy.yaml

# JSON output for scripting
dagrun run -json pipeline.yaml | jq '.nodes[] | select(.status == "failed")'

# Override timeout
dagrun run -timeout 120 long-pipeline.yaml
```

### `dagrun validate`

Validate a workflow file without executing:

```bash
dagrun validate <workflow.yaml>
```

Checks for:
- Valid YAML syntax
- Step name uniqueness
- Missing dependency references
- Circular dependencies

### `dagrun status`

Show the execution plan (dry-run):

```bash
dagrun status <workflow.yaml>
```

Output:

```
Workflow: data-pipeline
Description: ETL pipeline

Execution Plan:
  1. extract-users [command]
  2. extract-orders [command]
  3. transform [command] (depends: extract-users, extract-orders)
  4. load [command] (depends: transform)
```

### `dagrun dot`

Generate DOT graph for visualization:

```bash
dagrun dot pipeline.yaml > pipeline.dot
dot -Tpng pipeline.dot -o pipeline.png  # requires graphviz
```

### `dagrun version`

Show version information:

```bash
dagrun version
```

---

## Architecture

### Core Engine

```
YAML File
  -> Parser (internal/parser)     — Minimal YAML parser, zero deps
  -> Model (internal/model)       — Domain types: DAG, Step, NodeResult
  -> DAG Builder (internal/dag)   — Kahn's algorithm, cycle detection
  -> Runner (internal/runner)     — Channel-driven parallel executor
  -> Executor (internal/executor) — Command, HTTP, Script executors
  -> CLI (internal/cli)           — User interface
```

### Channel-Driven Scheduler

The runner uses Go channels for the event loop, inspired by dagu's architecture:

```
readyCh  — nodes whose dependencies are all satisfied
doneCh   — completed nodes signaling downstream dependents
```

The event loop:
1. Seed root nodes (zero dependencies) into `readyCh`
2. For each ready node: spawn a goroutine to execute
3. On completion: check downstream dependents, send ready ones to `readyCh`
4. Detect deadlocks: if no active nodes and DAG is incomplete

### Dependency Resolution

Uses **Kahn's algorithm** (BFS-based topological sort):
1. Build in-degree map for all nodes
2. Start with zero in-degree nodes
3. Process each node, decrement dependents' in-degree
4. If all nodes processed: valid DAG with topological order
5. If not all processed: cycle exists (remaining nodes form the cycle)

### Error Propagation

When a step fails:
- All downstream dependents are **skipped** (cascade)
- Unless the failed step has `continue_on.failure: true`
- The overall DAG status reflects the worst node status

---

## YAML Schema Reference

### DAG-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Workflow name |
| `description` | string | Workflow description |
| `steps` | array | List of step definitions (required) |
| `env` | map | Environment variables for all steps |
| `params` | array | Parameters (key=value format) |
| `shell` | string | Default shell for command steps |
| `working_dir` | string | Default working directory |
| `max_active_steps` | int | Max concurrent step execution |
| `timeout_sec` | int | Overall DAG timeout |
| `log_dir` | string | Log output directory |
| `handler_on` | object | Lifecycle handlers (success/failure/exit) |

### Step-Level Fields

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Step name (unique within DAG) |
| `description` | string | Step description |
| `command` | string | Shell command to execute |
| `script` | string | Multi-line script content |
| `type` | string | Executor type: command, http, script |
| `shell` | string | Shell to use (default: sh) |
| `working_dir` | string | Working directory |
| `depends` | array | List of dependency step names |
| `output` | string | Variable name to capture stdout |
| `env` | map | Step-specific environment variables |
| `timeout_sec` | int | Step timeout in seconds |
| `continue_on` | object | Continue policy (failure/skipped) |
| `retry_policy` | object | Retry configuration |
| `preconditions` | array | Conditions to check before execution |
| `http` | object | HTTP executor configuration |

### HTTP Configuration

| Field | Type | Description |
|-------|------|-------------|
| `method` | string | HTTP method (default: GET) |
| `url` | string | Request URL |
| `headers` | map | Request headers |
| `body` | string | Request body |
| `timeout` | int | Request timeout in seconds |

### Retry Policy

| Field | Type | Description |
|-------|------|-------------|
| `limit` | int | Maximum retry attempts |
| `interval_sec` | int | Initial wait between retries |
| `backoff` | float | Backoff multiplier |

---

## JSON Output Format

When using `-json` flag, results are output as structured JSON:

```json
{
  "name": "my-workflow",
  "status": "succeeded",
  "duration_ms": 1523,
  "nodes": [
    {
      "name": "step-1",
      "status": "succeeded",
      "output": "hello world",
      "duration_ms": 45,
      "exit_code": 0
    },
    {
      "name": "step-2",
      "status": "succeeded",
      "duration_ms": 1478,
      "exit_code": 0,
      "retries": 1
    }
  ]
}
```

---

## Comparison with dagu

### What dagrun keeps from dagu
- YAML workflow definition format
- Kahn's algorithm for topological sort
- Channel-driven event loop architecture
- Node lifecycle (preconditions -> execute -> handlers)
- Retry with backoff
- Output capture and variable passing
- Precondition evaluation

### What dagrun removes
- Web UI / dashboard
- Docker executor
- SSH/SFTP executor
- LLM/AI agent integration
- Distributed worker mode
- Tailscale tunneling
- Slack/Telegram notifications
- Git sync
- 19+ executor types (keeping only 3)
- 60+ external dependencies (keeping 0)

### What dagrun improves
- **Zero dependencies**: Go stdlib only
- **Focused scope**: DAG execution, nothing else
- **Better error tracing**: Per-step timing, exit codes, retry counts
- **Smaller attack surface**: No external deps = no supply chain risk

---

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing`)
3. Run tests (`go test ./...`)
4. Commit your changes
5. Push to the branch
6. Open a Pull Request

### Development

```bash
# Build
go build -o dagrun .

# Test
go test ./... -v

# Vet
go vet ./...
```

> **Note (Windows):** Many tests invoke shell commands via `sh -c` and will be
> skipped automatically on Windows (`t.Skip`). Run the full suite on Linux/macOS
> for complete coverage.

> **Note (go.sum):** This project has zero external dependencies (stdlib only),
> so there is no `go.sum` file. This is expected.

---

## License

GPL-3.0 License - see [LICENSE](LICENSE) for details.

---

## Acknowledgments

- [dagu](https://github.com/dagu-org/dagu) - The original DAG workflow engine that inspired this project. dagrun reimplements dagu's core execution engine from scratch with a focus on simplicity and zero dependencies.
