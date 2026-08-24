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
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

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
	// MaxSteps bounds the tool-call rounds for one run. Zero keeps the CLI
	// default; provisioned in the orchestrator for free/streaming models so a
	// "let me explore" loop cannot run forever.
	MaxSteps int
	// AutoApprove adds --auto: opencode approves every permission request
	// that is not explicitly denied. The orchestrator runs nodes
	// non-interactively (there is no TTY and no programmatic reply channel
	// for opencode permission prompts), so this is the only way to keep a
	// run from hanging on an ask.
	AutoApprove bool
	// PermissionConfig is injected as OPENCODE_CONFIG_CONTENT (inline config,
	// highest non-managed precedence) so the run can hard-deny the tools that
	// would otherwise block an unattended pipeline — most importantly the
	// "question" tool, whose whole purpose is to wait for a human answer that
	// a one-shot subprocess can never receive.
	PermissionConfig string
	// Variant is the model variant (provider-specific reasoning effort,
	// e.g. high / medium / low / max / minimal), passed as `--variant`.
	Variant string
	// OnLine, when non-nil, is called with each non-empty trimmed stdout line
	// as the subprocess streams it (used to show live thinking progress).
	OnLine func(string)
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
// they cannot be spawned directly by os/exec on Windows, and npm placeholder
// files (opencode-ai's postinstall stub) are skipped because they are shell
// scripts, not native executables.
func DiscoverBin() string {
	candidates := []string{
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
		// When opencode-ai was installed with --ignore-scripts (or via pnpm),
		// bin/opencode.exe is a shell placeholder and the real binary lives in
		// the platform optional dependency package.
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "opencode-ai", "node_modules", "opencode-windows-x64", "bin", "opencode.exe"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "node_modules", "opencode-windows-x64", "bin", "opencode.exe"),
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "opencode-ai", "node_modules", "opencode-windows-x64-baseline", "bin", "opencode.exe"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "node_modules", "opencode-windows-x64-baseline", "bin", "opencode.exe"),
	}
	for _, candidate := range candidates {
		if candidate != "" && isExecutableCandidate(candidate) {
			return candidate
		}
	}
	if path, err := exec.LookPath("opencode"); err == nil {
		return path
	}
	return ""
}

// isExecutableCandidate reports whether path exists and, on Windows, starts
// with the PE magic "MZ". opencode-ai leaves a small shell placeholder at
// bin/opencode.exe when its postinstall script was skipped; spawning that
// placeholder yields a confusing ERROR_BAD_EXE_FORMAT ("This version of %1 is
// not compatible with the version of Windows you're running").
func isExecutableCandidate(path string) bool {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return false
	}
	if runtime.GOOS != "windows" {
		return true
	}
	f, err := os.Open(path)
	if err != nil {
		return false
	}
	defer f.Close()
	header := make([]byte, 2)
	if _, err := io.ReadFull(f, header); err != nil {
		return false
	}
	return header[0] == 'M' && header[1] == 'Z'
}

// maxStepsProbes caches the per-binary result of the --max-steps support
// probe. opencode v1.x removed the flag; passing it to a newer CLI makes
// yargs print the run help text and exit non-zero, which surfaced as node
// attempts "failing" with the whole help message as the error text.
var (
	maxStepsMu     sync.Mutex
	maxStepsProbes = map[string]bool{}
)

// SupportsMaxSteps reports whether the opencode binary at bin still accepts
// --max-steps (`opencode run --help` lists it). Probed once per binary path
// and cached for the process lifetime; on probe failure (CLI missing, help
// unavailable) the flag is treated as unsupported — losing the tool-round
// bound is recoverable via the caller's turn timeout, whereas passing an
// unknown flag fails every single call.
func SupportsMaxSteps(bin string) bool {
	maxStepsMu.Lock()
	defer maxStepsMu.Unlock()
	if supported, ok := maxStepsProbes[bin]; ok {
		return supported
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, "run", "--help")
	proc.HideWindow(cmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	supported := cmd.Run() == nil && strings.Contains(out.String(), "--max-steps")
	maxStepsProbes[bin] = supported
	return supported
}

// Execute runs one `opencode run` call and parses the JSON event stream.
func (e *Executor) Execute(ctx context.Context, opts ExecOptions, prompt string) (*ExecutorResult, error) {
	bin := e.opencodeBin()
	args := []string{"run"}
	if opts.ResumeSessionID != "" {
		args = append(args, "--session", opts.ResumeSessionID)
	}
	if opts.Model != "" {
		args = append(args, "-m", opts.Model)
	}
	if opts.MaxSteps > 0 && SupportsMaxSteps(bin) {
		args = append(args, "--max-steps", fmt.Sprint(opts.MaxSteps))
	}
	if opts.AutoApprove {
		args = append(args, "--auto")
	}
	if strings.TrimSpace(opts.Variant) != "" {
		args = append(args, "--variant", strings.TrimSpace(opts.Variant))
	}
	args = append(args, "--format", "json", prompt)

	cmd := exec.CommandContext(ctx, bin, args...)
	// One-shot opencode run must not flash a console window on Windows.
	proc.HideWindow(cmd)
	if opts.Workspace != "" {
		cmd.Dir = opts.Workspace
	}
	if opts.PermissionConfig != "" {
		cmd.Env = append(os.Environ(), "OPENCODE_CONFIG_CONTENT="+opts.PermissionConfig)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stderr = &stderr
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("opencode run: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("opencode run: %w", err)
	}
	sc := bufio.NewScanner(stdoutPipe)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := sc.Text()
		stdout.WriteString(line + "\n")
		if opts.OnLine != nil {
			if trimmed := strings.TrimSpace(line); trimmed != "" {
				opts.OnLine(trimmed)
			}
		}
	}
	err = cmd.Wait()
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
