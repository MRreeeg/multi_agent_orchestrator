package orchestrator

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ParseReviewDecision extracts a ReviewDecision from the reviewer's output text.
// It looks for the last complete JSON object in the output that contains a
// top-level "decision" field. Reviewer output may contain tool traces before
// the final answer, so this parser deliberately ignores command/tool JSON and
// also understands braces inside JSON strings.
func ParseReviewDecision(output string) (ReviewDecision, error) {
	output = strings.TrimSpace(output)
	if output == "" {
		return ReviewDecision{}, fmt.Errorf("empty review output")
	}

	// Try to find JSON in the output
	var jsonStr string

	// First, try direct JSON parse
	var direct ReviewDecision
	if err := json.Unmarshal([]byte(output), &direct); err == nil && direct.Decision != "" {
		return direct, nil
	}

	// Scan for all top-level JSON objects and pick the last parseable object with
	// a real decision field. A simple brace counter is not sufficient here:
	// review summaries and evidence often contain snippets such as "{}" or
	// PowerShell commands with braces.
	for i := 0; i < len(output); i++ {
		if output[i] != '{' {
			continue
		}
		end := scanJSONObjectEnd(output, i)
		if end < 0 {
			continue
		}
		candidate := output[i : end+1]
		var raw map[string]json.RawMessage
		if err := json.Unmarshal([]byte(candidate), &raw); err == nil {
			if _, ok := raw["decision"]; ok {
				jsonStr = candidate
			}
		}
		i = end
	}

	if jsonStr == "" {
		return ReviewDecision{}, fmt.Errorf("no JSON with 'decision' field found in review output")
	}

	var decision ReviewDecision
	if err := json.Unmarshal([]byte(jsonStr), &decision); err != nil {
		return ReviewDecision{}, fmt.Errorf("failed to parse review JSON: %w", err)
	}

	return decision, nil
}

// scanJSONObjectEnd returns the closing brace for the JSON object beginning
// at start. It ignores braces inside quoted strings and escaped quotes.
func scanJSONObjectEnd(s string, start int) int {
	if start < 0 || start >= len(s) || s[start] != '{' {
		return -1
	}
	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(s); i++ {
		c := s[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if c == '\\' {
				escaped = true
				continue
			}
			if c == '"' {
				inString = false
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// ValidateReviewDecision checks that a ReviewDecision conforms to the loop-review-v1 protocol.
func ValidateReviewDecision(d ReviewDecision) error {
	// Check schema version
	if d.SchemaVersion != "loop-review-v1" {
		return fmt.Errorf("invalid schemaVersion %q: must be loop-review-v1", d.SchemaVersion)
	}

	// Check decision field
	if d.Decision == "" {
		return fmt.Errorf("decision field is missing")
	}
	switch d.Decision {
	case "pass", "revise", "blocked":
		// valid
	default:
		return fmt.Errorf("invalid decision %q: must be pass, revise, or blocked", d.Decision)
	}

	// Check confidence range
	if d.Confidence < 0 || d.Confidence > 1 {
		return fmt.Errorf("confidence %f is out of range [0, 1]", d.Confidence)
	}

	// Decision-specific validation
	switch d.Decision {
	case "pass":
		if len(d.BlockingIssues) > 0 {
			return fmt.Errorf("pass decision must not have blockingIssues")
		}
		if len(d.RequiredChanges) > 0 {
			return fmt.Errorf("pass decision must not have requiredChanges")
		}
		if strings.TrimSpace(d.NextTask) != "" {
			return fmt.Errorf("pass decision must not have nextTask")
		}

	case "revise":
		if strings.TrimSpace(d.NextTask) == "" {
			return fmt.Errorf("revise decision requires non-empty nextTask")
		}
		if len(d.RequiredChanges) == 0 {
			return fmt.Errorf("revise decision requires non-empty requiredChanges")
		}

	case "blocked":
		if len(d.BlockingIssues) == 0 {
			return fmt.Errorf("blocked decision requires non-empty blockingIssues")
		}
	}

	return nil
}
