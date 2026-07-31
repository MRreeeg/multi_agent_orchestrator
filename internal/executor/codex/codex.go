// Package codex implements a Codex CLI executor for the orchestrator.
//
// This package provides an isolated, testable executor that can start Codex CLI
// processes (`codex exec`), send prompts, read outputs, and handle cancellation.
// It supports both fresh execution and session resumption via `codex exec resume`.
//
// One-shot orchestration uses `codex exec` (and `codex exec resume`). Retained
// orchestration uses `codex app-server` over a loopback WebSocket; this executor
// package itself does not expose an HTTP service.
package codex

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"
)

// ExecutorResult holds the result of a Codex CLI execution.
type ExecutorResult struct {
	Output    string `json:"output"`
	RawStdout string `json:"rawStdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exitCode"`
	// ThreadID is the Codex thread/session ID from thread.started event.
	// Used for `codex exec resume` in subsequent executions.
	ThreadID string `json:"threadID,omitempty"`
	// HasError indicates if any Codex error event was found in JSONL.
	HasError bool `json:"hasError,omitempty"`
	// FatalError indicates a fatal error event was found (not just a warning).
	FatalError bool `json:"fatalError,omitempty"`
	// ErrorCount is the number of Codex error events found.
	ErrorCount int `json:"errorCount,omitempty"`
	// Warnings holds non-fatal warning messages from error events.
	Warnings []string `json:"warnings,omitempty"`
	// Usage holds token usage from turn.completed event.
	Usage map[string]any `json:"usage,omitempty"`
}

// ExecOptions configures a Codex exec execution.
type ExecOptions struct {
	Model           string
	ReasoningEffort string // "high" | "medium" | "low"
	Timeout         time.Duration
	Workspace       string
	Sandbox         string // "read-only", "workspace-write", "danger-full-access"
	BypassAll       bool   // --dangerously-bypass-approvals-and-sandbox
	Ephemeral       bool   // --ephemeral (don't save session)
	JSON            bool   // --json (JSONL output)
	OutputFile      string // -o <file> (write last message to file)
	ResumeSessionID string // codex exec resume <id>
}

// CodexExecutor executes tasks via the Codex CLI (`codex exec`).
type CodexExecutor struct {
	// CodexBin is the path to the codex binary. If empty, uses "codex".
	CodexBin string
}

// New creates a CodexExecutor with default settings.
func New() *CodexExecutor {
	return &CodexExecutor{}
}

// Exec runs `codex exec` or `codex exec resume` with the given prompt and options.
func (e *CodexExecutor) Exec(ctx context.Context, prompt string, opts ExecOptions) (*ExecutorResult, error) {
	// Mutual exclusion: ephemeral and resume cannot coexist
	if opts.Ephemeral && opts.ResumeSessionID != "" {
		return nil, fmt.Errorf("codex: cannot use --ephemeral with exec resume (session would be discarded)")
	}

	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	// Codex accepts "-" as the prompt argument and reads the prompt from stdin.
	// Always use that form instead of appending prompt to argv: on Windows the
	// combined command line is limited, while Loop prompts can contain the full
	// pipeline context, skills, previous iteration output, and review protocol.
	args := e.buildArgs(opts)
	args = append(args, "-")

	return e.executeWithInput(ctx, args, strings.NewReader(prompt))
}

// buildArgs builds the non-prompt portion of a `codex exec` command.
// The caller is responsible for appending the prompt selector (normally "-").
func (e *CodexExecutor) buildArgs(opts ExecOptions) []string {
	args := []string{"exec"}

	if opts.ResumeSessionID != "" {
		args = append(args, "resume", opts.ResumeSessionID)
	}

	if opts.Model != "" {
		args = append(args, "-m", opts.Model)
	}
	if opts.ReasoningEffort != "" {
		args = append(args, "-c", "model_reasoning_effort="+opts.ReasoningEffort)
	}
	if opts.Workspace != "" {
		args = append(args, "-C", opts.Workspace)
	}
	// The orchestrator accepts arbitrary workspace directories, including
	// freshly-created projects and extracted packs without a .git directory.
	// Codex otherwise rejects those directories before it reads the prompt.
	args = append(args, "--skip-git-repo-check")
	if opts.Sandbox != "" {
		args = append(args, "-s", opts.Sandbox)
	}
	if opts.BypassAll {
		args = append(args, "--dangerously-bypass-approvals-and-sandbox")
	}
	if opts.Ephemeral {
		args = append(args, "--ephemeral")
	}
	if opts.JSON {
		args = append(args, "--json")
	}
	if opts.OutputFile != "" {
		args = append(args, "-o", opts.OutputFile)
	}

	return args
}

// execute runs the codex binary with the given args and captures output.
func (e *CodexExecutor) execute(ctx context.Context, args []string) (*ExecutorResult, error) {
	return e.executeWithInput(ctx, args, nil)
}

// executeWithInput runs the codex binary with optional stdin and captures output.
func (e *CodexExecutor) executeWithInput(ctx context.Context, args []string, input io.Reader) (*ExecutorResult, error) {
	bin := e.codexBin()
	cmd := exec.CommandContext(ctx, bin, args...)
	if input != nil {
		cmd.Stdin = input
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	rawStdout := stdout.String()
	exitCode := 0

	if err != nil {
		// Distinguish timeout from cancellation
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return &ExecutorResult{
					RawStdout: rawStdout,
					Stderr:    stderr.String(),
					ExitCode:  -1,
				}, fmt.Errorf("%w: %v", ErrExecutorTimeout, ctx.Err())
			}
			return &ExecutorResult{
				RawStdout: rawStdout,
				Stderr:    stderr.String(),
				ExitCode:  -1,
			}, fmt.Errorf("%w: %v", ErrExecutorCanceled, ctx.Err())
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			return &ExecutorResult{
				RawStdout: rawStdout,
				Stderr:    stderr.String(),
				ExitCode:  -1,
			}, fmt.Errorf("%w: %v", ErrExecutorStart, err)
		}
	}

	result := &ExecutorResult{
		RawStdout: rawStdout,
		Stderr:    stderr.String(),
		ExitCode:  exitCode,
	}

	if exitCode != 0 {
		// Still try to parse for partial output and thread ID
		e.parseOutput(result)
		return result, fmt.Errorf("%w: exit code %d", ErrExecutorExit, exitCode)
	}

	// Parse JSONL events first
	e.parseOutput(result)

	// After parsing, check if we have a valid assistant message
	if result.Output == "" {
		return result, ErrExecutorEmptyOutput
	}

	// Fatal error events cause failure even when output exists.
	// Warning events are non-fatal and logged to Stderr.
	if result.FatalError {
		return result, fmt.Errorf("%w: fatal error event(s) with output", ErrExecutorExit)
	}

	return result, nil
}

// parseOutput extracts structured fields from Codex JSONL output.
//
// Real Codex JSONL format (verified with codex exec --json):
//
//	{"type":"thread.started","thread_id":"<uuid>"}
//	{"type":"turn.started"}
//	{"type":"item.completed","item":{"type":"agent_message","text":"<output>"}}
//	{"type":"turn.completed","usage":{"input_tokens":N,"output_tokens":N}}
//
// Error events:
//
//	{"type":"item.completed","item":{"type":"error","message":"<error>"}}
func (e *CodexExecutor) parseOutput(result *ExecutorResult) {
	raw := strings.TrimSpace(result.RawStdout)
	lines := strings.Split(raw, "\n")

	var textParts []string
	var lastUsage map[string]any

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		eventType, _ := event["type"].(string)

		switch eventType {
		case "thread.started":
			if tid, _ := event["thread_id"].(string); tid != "" {
				result.ThreadID = tid
			}

		case "item.completed":
			if item, ok := event["item"].(map[string]any); ok {
				itemType, _ := item["type"].(string)
				switch itemType {
				case "agent_message":
					if text, _ := item["text"].(string); text != "" {
						textParts = append(textParts, text)
					}
				case "error":
					// Classify error events as warning or fatal
					result.HasError = true
					result.ErrorCount++
					msg, _ := item["message"].(string)
					if msg == "" {
						msg = "unknown error"
					}
					// Heuristic: known non-fatal warnings
					lowerMsg := strings.ToLower(msg)
					isWarning := strings.Contains(lowerMsg, "warning") ||
						strings.Contains(lowerMsg, "budget") ||
						strings.Contains(lowerMsg, "shortened") ||
						strings.Contains(lowerMsg, "skill") ||
						strings.Contains(lowerMsg, "truncated")
					if isWarning {
						result.Warnings = append(result.Warnings, msg)
						result.Stderr += "[codex warning] " + msg + "\n"
					} else {
						result.FatalError = true
						result.Stderr += "[codex error] " + msg + "\n"
					}
				}
			}

		case "turn.completed":
			if usage, ok := event["usage"].(map[string]any); ok {
				lastUsage = usage
			}
		}
	}

	// Assemble final output from all agent_message items
	if len(textParts) > 0 {
		result.Output = strings.Join(textParts, "\n")
	}

	if lastUsage != nil {
		result.Usage = lastUsage
	}
}

// codexBin returns the configured codex binary path.
func (e *CodexExecutor) codexBin() string {
	if e.CodexBin != "" {
		return e.CodexBin
	}
	return "codex"
}
