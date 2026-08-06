// Package opencode implements the opencode CLI executor for the orchestrator.
//
// run mode executes `opencode run -m <provider/model> --format json` as a
// one-shot process and parses the JSON event stream. Retained orchestration
// uses `opencode serve` over loopback HTTP (see client.go) and is owned by
// OpenCodeRuntimeManager in the orchestrator package.
package opencode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"reasonix/internal/proc"
)

// ExecutorResult is the outcome of one `opencode run` invocation.
type ExecutorResult struct {
	Output      string  `json:"output"`
	RawStdout   string  `json:"rawStdout"`
	Stderr      string  `json:"stderr"`
	ExitCode    int     `json:"exitCode"`
	SessionID   string  `json:"sessionID,omitempty"`
	TotalTokens int64   `json:"totalTokens,omitempty"`
	Cost        float64 `json:"cost,omitempty"`
}

// ExecOptions configures an opencode run execution.
type ExecOptions struct {
	Model           string // provider/model, e.g. opencode/deepseek-v4-flash-free
	Workspace       string
	ResumeSessionID string
}

// Executor executes tasks via the opencode CLI (`opencode run`).
type Executor struct {
	OpencodeBin string // optional override
}

func (e *Executor) opencodeBin() string {
	if e.OpencodeBin != "" {
		return e.OpencodeBin
	}
	if bin := DiscoverBin(); bin != "" {
		return bin
	}
	return "opencode"
}

// DiscoverBin returns the native opencode binary path found on this machine,
// or "" when nothing is installed. npm shims (.ps1/.cmd) are skipped because
// they cannot be spawned directly by os/exec on Windows.
func DiscoverBin() string {
	candidates := []string{
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	if path, err := exec.LookPath("opencode"); err == nil {
		return path
	}
	return ""
}

// Execute runs one `opencode run` call and parses the JSON event stream.
func (e *Executor) Execute(ctx context.Context, opts ExecOptions, prompt string) (*ExecutorResult, error) {
	args := []string{"run"}
	if opts.ResumeSessionID != "" {
		args = append(args, "--session", opts.ResumeSessionID)
	}
	if opts.Model != "" {
		args = append(args, "-m", opts.Model)
	}
	args = append(args, "--format", "json", prompt)

	bin := e.opencodeBin()
	cmd := exec.CommandContext(ctx, bin, args...)
	// One-shot opencode run must not flash a console window on Windows.
	proc.HideWindow(cmd)
	if opts.Workspace != "" {
		cmd.Dir = opts.Workspace
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("opencode run: %w", err)
		}
	}
	res := ParseRunOutput(stdout.Bytes())
	res.Stderr = strings.TrimSpace(stderr.String())
	res.ExitCode = exitCode
	res.RawStdout = stdout.String()
	if exitCode != 0 && strings.TrimSpace(res.Output) == "" {
		return res, fmt.Errorf("opencode run failed (exit %d): %s", exitCode, res.Stderr)
	}
	return res, nil
}

type runTokens struct {
	Total  int64   `json:"total"`
	Input  int64   `json:"input"`
	Output int64   `json:"output"`
	Cost   float64 `json:"cost"`
}

type runPart struct {
	Type   string     `json:"type"`
	Text   string     `json:"text"`
	Tokens *runTokens `json:"tokens,omitempty"`
}

type runEvent struct {
	Type      string  `json:"type"`
	SessionID string  `json:"sessionID,omitempty"`
	Error     string  `json:"error,omitempty"`
	Part      runPart `json:"part,omitempty"`
}

// ParseRunOutput parses the newline-delimited JSON event stream emitted by
// `opencode run --format json`. Assistant text parts are concatenated; the
// step_finish event carries token usage.
func ParseRunOutput(data []byte) *ExecutorResult {
	res := &ExecutorResult{}
	var text []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev runEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.SessionID != "" && res.SessionID == "" {
			res.SessionID = ev.SessionID
		}
		switch ev.Type {
		case "text":
			if ev.Part.Text != "" {
				text = append(text, ev.Part.Text)
			}
		case "step_finish":
			if ev.Part.Tokens != nil {
				res.TotalTokens = ev.Part.Tokens.Total
				res.Cost = ev.Part.Tokens.Cost
			}
		case "error":
			if ev.Error != "" {
				res.Output = ev.Error
			} else if ev.Part.Text != "" {
				res.Output = ev.Part.Text
			}
		}
	}
	res.Output = strings.TrimSpace(strings.Join(text, ""))
	return res
}
