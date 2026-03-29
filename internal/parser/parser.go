// Package parser provides YAML workflow file parsing.
package parser

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/JSLEEKR/dagrun/internal/model"
)

// ParseFile reads and parses a YAML workflow file.
func ParseFile(path string) (*model.DAG, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read file %q: %w", path, err)
	}
	return Parse(data)
}

// Parse parses YAML workflow data into a DAG.
// Uses a custom minimal YAML parser since we have zero external deps.
func Parse(data []byte) (*model.DAG, error) {
	m, err := parseYAML(data)
	if err != nil {
		return nil, fmt.Errorf("yaml parse error: %w", err)
	}
	return buildDAG(m)
}

// buildDAG constructs a DAG from a parsed YAML map.
func buildDAG(m map[string]interface{}) (*model.DAG, error) {
	dag := &model.DAG{}

	dag.Name = getString(m, "name")
	dag.Description = getString(m, "description")
	dag.Shell = getString(m, "shell")
	dag.WorkingDir = getString(m, "working_dir")
	dag.LogDir = getString(m, "log_dir")
	dag.MaxActive = getInt(m, "max_active_steps")
	dag.TimeoutSec = getInt(m, "timeout_sec")

	// Parse env
	if envRaw, ok := m["env"]; ok {
		dag.Env = parseStringMap(envRaw)
	}

	// Parse params
	if paramsRaw, ok := m["params"]; ok {
		if arr, ok := paramsRaw.([]interface{}); ok {
			for _, p := range arr {
				if s, ok := p.(string); ok {
					dag.Params = append(dag.Params, s)
				}
			}
		}
	}

	// Parse handler_on
	if handlerRaw, ok := m["handler_on"]; ok {
		if handlerMap, ok := handlerRaw.(map[string]interface{}); ok {
			if successRaw, ok := handlerMap["success"]; ok {
				if stepMap, ok := successRaw.(map[string]interface{}); ok {
					step := parseStep(stepMap)
					dag.HandlerOn.Success = &step
				}
			}
			if failureRaw, ok := handlerMap["failure"]; ok {
				if stepMap, ok := failureRaw.(map[string]interface{}); ok {
					step := parseStep(stepMap)
					dag.HandlerOn.Failure = &step
				}
			}
			if exitRaw, ok := handlerMap["exit"]; ok {
				if stepMap, ok := exitRaw.(map[string]interface{}); ok {
					step := parseStep(stepMap)
					dag.HandlerOn.Exit = &step
				}
			}
		}
	}

	// Parse steps
	stepsRaw, ok := m["steps"]
	if !ok {
		return nil, fmt.Errorf("workflow must have 'steps' field")
	}
	stepsArr, ok := stepsRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("'steps' must be an array")
	}

	for i, stepRaw := range stepsArr {
		stepMap, ok := stepRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("step %d is not a map", i)
		}
		step := parseStep(stepMap)
		if step.Name == "" {
			step.Name = fmt.Sprintf("step_%d", i+1)
		}
		dag.Steps = append(dag.Steps, step)
	}

	if len(dag.Steps) == 0 {
		return nil, fmt.Errorf("workflow must have at least one step")
	}

	return dag, nil
}

// parseStep converts a map to a Step.
func parseStep(m map[string]interface{}) model.Step {
	step := model.Step{
		Name:        getString(m, "name"),
		Description: getString(m, "description"),
		Command:     getString(m, "command"),
		Script:      getString(m, "script"),
		Shell:       getString(m, "shell"),
		WorkingDir:  getString(m, "working_dir"),
		Type:        getString(m, "type"),
		Output:      getString(m, "output"),
		TimeoutSec:  getInt(m, "timeout_sec"),
	}

	// Parse depends
	if depsRaw, ok := m["depends"]; ok {
		if arr, ok := depsRaw.([]interface{}); ok {
			for _, d := range arr {
				if s, ok := d.(string); ok {
					step.Depends = append(step.Depends, s)
				}
			}
		}
	}

	// Parse env
	if envRaw, ok := m["env"]; ok {
		step.Env = parseStringMap(envRaw)
	}

	// Parse continue_on
	if coRaw, ok := m["continue_on"]; ok {
		if coMap, ok := coRaw.(map[string]interface{}); ok {
			step.ContinueOn.Failure = getBool(coMap, "failure")
			step.ContinueOn.Skipped = getBool(coMap, "skipped")
		}
	}

	// Parse retry_policy
	if rpRaw, ok := m["retry_policy"]; ok {
		if rpMap, ok := rpRaw.(map[string]interface{}); ok {
			step.RetryPolicy = &model.RetryPolicy{
				Limit:       getInt(rpMap, "limit"),
				IntervalSec: getInt(rpMap, "interval_sec"),
				Backoff:     getFloat(rpMap, "backoff"),
			}
		}
	}

	// Parse preconditions
	if pcRaw, ok := m["preconditions"]; ok {
		if arr, ok := pcRaw.([]interface{}); ok {
			for _, p := range arr {
				if pm, ok := p.(map[string]interface{}); ok {
					step.Preconditions = append(step.Preconditions, model.Precondition{
						Condition: getString(pm, "condition"),
						Expected:  getString(pm, "expected"),
					})
				}
			}
		}
	}

	// Parse http config
	if httpRaw, ok := m["http"]; ok {
		if httpMap, ok := httpRaw.(map[string]interface{}); ok {
			step.HTTPConfig = &model.HTTPConfig{
				Method:  getString(httpMap, "method"),
				URL:     getString(httpMap, "url"),
				Body:    getString(httpMap, "body"),
				Timeout: getInt(httpMap, "timeout"),
			}
			if headersRaw, ok := httpMap["headers"]; ok {
				step.HTTPConfig.Headers = parseStringMap(headersRaw)
			}
		}
	}

	return step
}

// Helper functions for map access
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		switch s := v.(type) {
		case string:
			return s
		case bool:
			if s {
				return "true"
			}
			return "false"
		default:
			return fmt.Sprintf("%v", v)
		}
	}
	return ""
}

func getInt(m map[string]interface{}, key string) int {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case int:
			return n
		case float64:
			return int(n)
		case json.Number:
			i, _ := n.Int64()
			return int(i)
		}
	}
	return 0
}

func getFloat(m map[string]interface{}, key string) float64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case int:
			return float64(n)
		case json.Number:
			f, _ := n.Float64()
			return f
		}
	}
	return 0
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

func parseStringMap(v interface{}) map[string]string {
	result := make(map[string]string)
	if m, ok := v.(map[string]interface{}); ok {
		for k, val := range m {
			if s, ok := val.(string); ok {
				result[k] = s
			} else {
				result[k] = fmt.Sprintf("%v", val)
			}
		}
	}
	return result
}

// parseYAML is a minimal YAML parser supporting the subset needed for dagrun workflows.
// Supports: mappings, sequences, scalars, comments, multi-line strings.
// Does NOT support: anchors, tags, multi-document, complex keys.
func parseYAML(data []byte) (map[string]interface{}, error) {
	lines := strings.Split(string(data), "\n")
	result, _, err := parseMapping(lines, 0, 0)
	if err != nil {
		return nil, err
	}
	if m, ok := result.(map[string]interface{}); ok {
		return m, nil
	}
	return nil, fmt.Errorf("top-level must be a mapping")
}

// parseMapping parses a YAML mapping at the given indentation level.
func parseMapping(lines []string, start, indent int) (interface{}, int, error) {
	result := make(map[string]interface{})
	i := start

	for i < len(lines) {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		// Skip empty lines and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			i++
			continue
		}

		// Check indentation
		lineIndent := countIndent(line)
		if lineIndent < indent {
			break // back to parent
		}
		if lineIndent > indent && len(result) == 0 {
			indent = lineIndent // adjust for first key
		}
		if lineIndent != indent {
			if lineIndent < indent {
				break
			}
			// Nested content handled by recursive calls
			break
		}

		// Check if this is a sequence item (- key: val)
		if strings.HasPrefix(trimmed, "- ") {
			break // sequences handled by caller
		}

		// Parse key: value
		colonIdx := strings.Index(trimmed, ":")
		if colonIdx < 0 {
			i++
			continue
		}

		key := strings.TrimSpace(trimmed[:colonIdx])
		valueStr := ""
		if colonIdx+1 < len(trimmed) {
			valueStr = strings.TrimSpace(trimmed[colonIdx+1:])
		}

		// Remove inline comments
		valueStr = removeInlineComment(valueStr)

		if valueStr == "" {
			// Check next line for nested content
			nextNonEmpty := findNextNonEmpty(lines, i+1)
			if nextNonEmpty < len(lines) {
				nextIndent := countIndent(lines[nextNonEmpty])
				nextTrimmed := strings.TrimSpace(lines[nextNonEmpty])
				if nextIndent > indent {
					if strings.HasPrefix(nextTrimmed, "- ") {
						// Sequence
						seq, nextI, err := parseSequence(lines, nextNonEmpty, nextIndent)
						if err != nil {
							return nil, i, err
						}
						result[key] = seq
						i = nextI
						continue
					} else {
						// Nested mapping
						nested, nextI, err := parseMapping(lines, nextNonEmpty, nextIndent)
						if err != nil {
							return nil, i, err
						}
						result[key] = nested
						i = nextI
						continue
					}
				}
			}
			result[key] = ""
			i++
			continue
		}

		// Check for multi-line string indicators
		if valueStr == "|" || valueStr == "|-" || valueStr == ">" || valueStr == ">-" {
			text, nextI := parseMultiLine(lines, i+1, indent)
			result[key] = text
			i = nextI
			continue
		}

		// Parse scalar value
		result[key] = parseScalar(valueStr)
		i++
	}

	return result, i, nil
}

// parseSequence parses a YAML sequence.
func parseSequence(lines []string, start, indent int) ([]interface{}, int, error) {
	var result []interface{}
	i := start

	for i < len(lines) {
		line := lines[i]
		trimmed := strings.TrimSpace(line)

		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			i++
			continue
		}

		lineIndent := countIndent(line)
		if lineIndent < indent {
			break
		}
		if lineIndent != indent {
			break
		}

		if !strings.HasPrefix(trimmed, "- ") && trimmed != "-" {
			break
		}

		// Remove "- " prefix
		itemStr := strings.TrimPrefix(trimmed, "- ")
		itemStr = strings.TrimPrefix(itemStr, "-")
		itemStr = strings.TrimSpace(itemStr)

		// Check if item is a mapping (has colon)
		if colonIdx := strings.Index(itemStr, ":"); colonIdx >= 0 {
			// Item is a mapping - parse it
			itemMap := make(map[string]interface{})
			key := strings.TrimSpace(itemStr[:colonIdx])
			valStr := ""
			if colonIdx+1 < len(itemStr) {
				valStr = strings.TrimSpace(itemStr[colonIdx+1:])
			}
			valStr = removeInlineComment(valStr)

			if valStr == "" {
				// Check for nested content
				nextNonEmpty := findNextNonEmpty(lines, i+1)
				if nextNonEmpty < len(lines) {
					nextIndent := countIndent(lines[nextNonEmpty])
					if nextIndent > indent {
						nextTrimmed := strings.TrimSpace(lines[nextNonEmpty])
						if strings.HasPrefix(nextTrimmed, "- ") {
							seq, nextI, err := parseSequence(lines, nextNonEmpty, nextIndent)
							if err != nil {
								return nil, i, err
							}
							itemMap[key] = seq
							i = nextI
						} else {
							nested, nextI, err := parseMapping(lines, nextNonEmpty, nextIndent)
							if err != nil {
								return nil, i, err
							}
							itemMap[key] = nested
							i = nextI
						}
					} else {
						itemMap[key] = ""
						i++
					}
				} else {
					itemMap[key] = ""
					i++
				}
			} else {
				itemMap[key] = parseScalar(valStr)
				i++
			}

			// Parse remaining keys at the same item level (indent + 2)
			itemIndent := indent + 2
			for i < len(lines) {
				nextLine := lines[i]
				nextTrimmed := strings.TrimSpace(nextLine)
				if nextTrimmed == "" || strings.HasPrefix(nextTrimmed, "#") {
					i++
					continue
				}
				nextLineIndent := countIndent(nextLine)
				if nextLineIndent < itemIndent {
					break
				}
				if nextLineIndent != itemIndent {
					break
				}
				if strings.HasPrefix(nextTrimmed, "- ") {
					break
				}

				cIdx := strings.Index(nextTrimmed, ":")
				if cIdx < 0 {
					i++
					continue
				}
				k := strings.TrimSpace(nextTrimmed[:cIdx])
				v := ""
				if cIdx+1 < len(nextTrimmed) {
					v = strings.TrimSpace(nextTrimmed[cIdx+1:])
				}
				v = removeInlineComment(v)

				if v == "" {
					// Check nested
					nextNE := findNextNonEmpty(lines, i+1)
					if nextNE < len(lines) {
						neIndent := countIndent(lines[nextNE])
						if neIndent > itemIndent {
							neTrimmed := strings.TrimSpace(lines[nextNE])
							if strings.HasPrefix(neTrimmed, "- ") {
								seq, nextI, err := parseSequence(lines, nextNE, neIndent)
								if err != nil {
									return nil, i, err
								}
								itemMap[k] = seq
								i = nextI
							} else if neTrimmed == "|" || neTrimmed == "|-" {
								text, nextI := parseMultiLine(lines, nextNE+1, itemIndent)
								itemMap[k] = text
								i = nextI
							} else {
								nested, nextI, err := parseMapping(lines, nextNE, neIndent)
								if err != nil {
									return nil, i, err
								}
								itemMap[k] = nested
								i = nextI
							}
						} else {
							itemMap[k] = ""
							i++
						}
					} else {
						itemMap[k] = ""
						i++
					}
				} else if v == "|" || v == "|-" || v == ">" || v == ">-" {
					text, nextI := parseMultiLine(lines, i+1, itemIndent)
					itemMap[k] = text
					i = nextI
				} else {
					itemMap[k] = parseScalar(v)
					i++
				}
			}

			result = append(result, itemMap)
		} else {
			// Simple scalar item
			result = append(result, parseScalar(itemStr))
			i++
		}
	}

	return result, i, nil
}

// parseMultiLine parses a multi-line string block.
func parseMultiLine(lines []string, start, baseIndent int) (string, int) {
	var parts []string
	i := start
	blockIndent := -1

	for i < len(lines) {
		line := lines[i]
		if strings.TrimSpace(line) == "" {
			parts = append(parts, "")
			i++
			continue
		}
		lineIndent := countIndent(line)
		if blockIndent < 0 {
			blockIndent = lineIndent
		}
		if lineIndent <= baseIndent {
			break
		}
		if lineIndent >= blockIndent {
			parts = append(parts, line[blockIndent:])
		}
		i++
	}

	// Trim trailing empty lines
	for len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}

	return strings.Join(parts, "\n"), i
}

// parseScalar converts a string to appropriate Go type.
func parseScalar(s string) interface{} {
	// Remove quotes
	if (strings.HasPrefix(s, "\"") && strings.HasSuffix(s, "\"")) ||
		(strings.HasPrefix(s, "'") && strings.HasSuffix(s, "'")) {
		return s[1 : len(s)-1]
	}

	// Boolean (only strict true/false, not yes/no to avoid the "Norway problem")
	lower := strings.ToLower(s)
	if lower == "true" {
		return true
	}
	if lower == "false" {
		return false
	}

	// Null
	if lower == "null" || lower == "~" {
		return nil
	}

	// Integer
	if isInteger(s) {
		n := 0
		negative := false
		start := 0
		if s[0] == '-' {
			negative = true
			start = 1
		}
		for _, c := range s[start:] {
			n = n*10 + int(c-'0')
		}
		if negative {
			n = -n
		}
		return n
	}

	// Float
	if isFloat(s) {
		f := 0.0
		fmt.Sscanf(s, "%f", &f)
		return f
	}

	return s
}

func isInteger(s string) bool {
	if s == "" {
		return false
	}
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	if start >= len(s) {
		return false
	}
	for _, c := range s[start:] {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func isFloat(s string) bool {
	if s == "" {
		return false
	}
	hasDot := false
	start := 0
	if s[0] == '-' || s[0] == '+' {
		start = 1
	}
	if start >= len(s) {
		return false
	}
	for _, c := range s[start:] {
		if c == '.' {
			if hasDot {
				return false
			}
			hasDot = true
		} else if c < '0' || c > '9' {
			return false
		}
	}
	return hasDot
}

func countIndent(line string) int {
	n := 0
	for _, c := range line {
		if c == ' ' {
			n++
		} else if c == '\t' {
			n += 2
		} else {
			break
		}
	}
	return n
}

func findNextNonEmpty(lines []string, start int) int {
	for i := start; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != "" && !strings.HasPrefix(strings.TrimSpace(lines[i]), "#") {
			return i
		}
	}
	return len(lines)
}

func removeInlineComment(s string) string {
	// Don't remove # inside quotes
	inSingle := false
	inDouble := false
	for i, c := range s {
		switch c {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '#':
			if !inSingle && !inDouble {
				return strings.TrimSpace(s[:i])
			}
		}
	}
	return s
}
