package claude

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"reasonix/internal/proc"
)

// ExecutorResult holds the result of a Claude CLI execution.
type ExecutorResult struct {
	Output    string `json:"output"`
	RawStdout string `json:"rawStdout"`
	Stderr    string `json:"stderr"`
	ExitCode  int    `json:"exitCode"`
	// SessionID is the Claude conversation session id from the result line.
	// Used for `--resume` in subsequent executions.
	SessionID string `json:"sessionID,omitempty"`
	// HasError indicates the CLI reported an error result subtype.
	HasError bool `json:"hasError,omitempty"`
	// Errors holds error messages from a failed result line.
	Errors []string `json:"errors,omitempty"`
	// Usage holds token/cost metadata from the result line.
	Usage map[string]any `json:"usage,omitempty"`
	// Interrupted is true when the result reports an interruption.
	Interrupted bool `json:"interrupted,omitempty"`
}

// ExecOptions configures a Claude Code execution.
type ExecOptions struct {
	Model string
	// ConfigDir overrides CLAUDE_CONFIG_DIR for the child process. This lets a
	// node pin its own settings.json (for example ~/.claude-deepseek with the
	// DeepSeek official base URL + API key) without touching the default
	// ~/.claude profile used by ccs/proxy routing. Never store keys in the
	// repository; the directory lives on the machine.
	ConfigDir          string
	Workspace          string
	Timeout            time.Duration
	ResumeSessionID    string
	PermissionMode     string // bypassPermissions | dontAsk | auto
	AppendSystemPrompt string // skill/system instructions appended via --append-system-prompt
	JSON               bool   // --output-format json
	// Effort is the reasoning effort level ("high" | "medium" | "low"),
	// passed through as `--effort` (claude -p supports it).
	Effort string
}

// ClaudeExecutor executes tasks via the Claude CLI (`claude -p`).
type ClaudeExecutor struct {
	// ClaudeBin is the path to the claude binary. If empty, the executor
	// discovers it (npm global install, PATH, or Claude Code CLI install dir).
	ClaudeBin string
}

// New creates a ClaudeExecutor with default settings.
func New() *ClaudeExecutor {
	return &ClaudeExecutor{}
}

// Exec runs `claude -p --output-format json` (optionally with `--resume`) and
// parses the single JSON result.
func (e *ClaudeExecutor) Exec(ctx context.Context, prompt string, opts ExecOptions) (*ExecutorResult, error) {
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}
	bin := e.claudeBin()
	args := []string{"-p", "--output-format", "json"}
	if opts.ResumeSessionID != "" {
		args = append(args, "--resume", opts.ResumeSessionID)
	}
	if opts.Model != "" {
		args = append(args, "--model", opts.Model)
	}
	if strings.TrimSpace(opts.Effort) != "" {
		args = append(args, "--effort", strings.TrimSpace(opts.Effort))
	}
	if mode := permissionModeFlag(opts.PermissionMode); mode != "" {
		args = append(args, "--permission-mode", mode)
	}
	if opts.AppendSystemPrompt != "" {
		args = append(args, "--append-system-prompt", opts.AppendSystemPrompt)
	}

	cmd := exec.CommandContext(ctx, bin, args...)
	// One-shot claude -p must not flash a console window on Windows (the
	// desktop app has no console to inherit).
	proc.HideWindow(cmd)
	if strings.TrimSpace(opts.Workspace) != "" {
		cmd.Dir = opts.Workspace
	}
	if strings.TrimSpace(opts.ConfigDir) != "" {
		cmd.Env = append(os.Environ(), "CLAUDE_CONFIG_DIR="+opts.ConfigDir)
	}
	// Windows command lines are length limited and Loop prompts can carry the
	// full pipeline context, skills, previous iteration output and review
	// protocol. Short prompts travel as the positional argument; long prompts
	// are piped over stdin (`claude -p` reads the prompt from stdin when the
	// positional prompt is absent).
	shortPrompt := len(prompt) <= 2000 && !strings.ContainsAny(prompt, "\r\n")
	var stdin io.Reader
	if shortPrompt {
		args = append(args, prompt)
	} else {
		stdin = strings.NewReader(prompt)
	}
	cmd.Stdin = stdin

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	rawStdout := stdout.String()
	exitCode := 0
	if err != nil {
		if ctx.Err() != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return &ExecutorResult{RawStdout: rawStdout, Stderr: stderr.String(), ExitCode: -1},
					fmt.Errorf("%w: %v", ErrExecutorTimeout, ctx.Err())
			}
			return &ExecutorResult{RawStdout: rawStdout, Stderr: stderr.String(), ExitCode: -1},
				fmt.Errorf("%w: %v", ErrExecutorCanceled, ctx.Err())
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return &ExecutorResult{RawStdout: rawStdout, Stderr: stderr.String(), ExitCode: -1},
				fmt.Errorf("%w: %v", ErrExecutorStart, err)
		}
	}

	result := &ExecutorResult{RawStdout: rawStdout, Stderr: stderr.String(), ExitCode: exitCode}
	parseJSONResult(result)

	if exitCode != 0 {
		return result, fmt.Errorf("%w: exit code %d", ErrExecutorExit, exitCode)
	}
	if result.HasError {
		return result, fmt.Errorf("%w: %s", ErrExecutorExit, strings.Join(result.Errors, "; "))
	}
	if strings.TrimSpace(result.Output) == "" {
		return result, ErrExecutorEmptyOutput
	}
	return result, nil
}

// parseJSONResult extracts fields from the single JSON object produced by
// `claude -p --output-format json`. The documented shape is a result envelope
// carrying the assistant text in `result` and the conversation id in
// `session_id`; older builds may nest the text inside `content`.
func parseJSONResult(result *ExecutorResult) {
	raw := strings.TrimSpace(result.RawStdout)
	if raw == "" {
		return
	}
	var body struct {
		Type        string   `json:"type"`
		Subtype     string   `json:"subtype"`
		IsError     bool     `json:"is_error"`
		Result      string   `json:"result"`
		SessionID   string   `json:"session_id"`
		Errors      []string `json:"errors"`
		StopReason  string   `json:"stop_reason"`
		TotalCost   float64  `json:"total_cost_usd"`
		TotalInput  int64    `json:"total_input_tokens"`
		TotalOutput int64    `json:"total_output_tokens"`
		Usage       any      `json:"usage"`
		Content     []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(raw), &body); err != nil {
		result.Stderr += "[claude] non-JSON stdout: " + raw + "\n"
		return
	}
	result.SessionID = body.SessionID
	result.HasError = body.IsError || strings.HasPrefix(body.Subtype, "error")
	result.Interrupted = body.StopReason == "interrupted" || strings.Contains(body.Subtype, "interrupt")
	if body.Errors != nil {
		result.Errors = append(result.Errors, body.Errors...)
	}
	text := strings.TrimSpace(body.Result)
	if text == "" {
		for _, block := range body.Content {
			if block.Type == "text" && strings.TrimSpace(block.Text) != "" {
				if text != "" {
					text += "\n"
				}
				text += block.Text
			}
		}
	}
	result.Output = text
	if body.Usage != nil {
		result.Usage = map[string]any{"usage": body.Usage}
	}
	if body.TotalCost > 0 || body.TotalInput > 0 || body.TotalOutput > 0 {
		result.Usage = map[string]any{
			"total_cost_usd":      body.TotalCost,
			"total_input_tokens":  body.TotalInput,
			"total_output_tokens": body.TotalOutput,
		}
	}
}

// permissionModeFlag maps the orchestrator approval mode onto the Claude CLI
// permission mode. ask maps to dontAsk (non-interactive: never prompt, deny
// tool calls that need approval), auto maps to bypassPermissions so the
// retained agent can work without an operator sitting at the console.
func permissionModeFlag(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "auto", "yolo":
		return "bypassPermissions"
	case "ask", "manual":
		return "dontAsk"
	case "bypasspermissions", "dontask", "acceptedits", "plan", "auto-mode":
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return ""
	}
}

// PermissionModeForApproval exposes the mapping for tests and the runtime.
func PermissionModeForApproval(approvalMode string) string {
	return permissionModeFlag(approvalMode)
}

// claudeBin discovers the native Claude Code CLI binary. The npm shims
// (claude / claude.cmd / claude.ps1) cannot be spawned reliably as
// subprocesses on Windows, so the native node_modules binary is preferred.
func (e *ClaudeExecutor) claudeBin() string {
	if e.ClaudeBin != "" {
		return e.ClaudeBin
	}
	if bin := discoverClaudeBin(); bin != "" {
		return bin
	}
	return "claude"
}

// DiscoverBin returns the native claude binary path found on this machine,
// or "" when nothing is installed.
func DiscoverBin() string { return discoverClaudeBin() }

func discoverClaudeBin() string {
	// npm global install layout: <prefix>/node_modules/@anthropic-ai/claude-code/bin/claude.exe
	candidates := []string{
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "@anthropic-ai", "claude-code", "bin", "claude.exe"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "@anthropic-ai", "claude-code", "bin", "claude.exe"),
		"/usr/local/bin/claude",
		"/usr/bin/claude",
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	// Fall back to PATH resolution (works on Unix and for direct installs).
	if path, err := exec.LookPath("claude"); err == nil {
		return path
	}
	return ""
}
