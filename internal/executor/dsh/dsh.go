package dsh

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"reasonix/internal/proc"
)

// ExecutorResult holds the result of a DSH headless execution.
type ExecutorResult struct {
	// Output is the final assistant message text (stdout of the one-shot run).
	Output string `json:"output"`
	// RawStdout is the unmodified process stdout.
	RawStdout string `json:"rawStdout"`
	// Stderr is the process stderr (boot diagnostics, error messages).
	Stderr string `json:"stderr"`
	// ExitCode is the process exit code; 0 means the turn completed.
	ExitCode int `json:"exitCode"`
}

// ExecOptions configures a DSH headless execution.
type ExecOptions struct {
	// Model is the DSH model id (e.g. deepseek-v4-flash). DSH has no --model
	// flag: the value is applied as a temporary `--patch` overlay on the
	// `agent-default-model` composition row. A saved `agent-default-model`
	// section in the harness settings document ($DSH_HOME/settings.yaml) takes
	// precedence over any composition value, so strict per-node routing needs a
	// dedicated DSH_HOME (see DshHome); an empty Model leaves DSH's own default
	// untouched.
	Model string
	// Workspace is the working directory the one-shot agent operates in. DSH
	// takes the workspace from the process cwd, so this is applied via cmd.Dir.
	Workspace string
	// Timeout bounds the whole run. Zero keeps the caller's context.
	Timeout time.Duration
	// PermissionMode maps onto DSH_PERMISSION_MODE: "read-only" (read-only
	// sandbox + ask), "workspace-write" (workspace sandbox + ask) or
	// "danger-full-access" (full sandbox + never). Headless has no interactive
	// approval surface, so orchestrator nodes should use danger-full-access
	// (trusted) or read-only (architect/reviewer exploration).
	PermissionMode string
	// DshHome overrides DSH_HOME for the child process. Useful for per-model /
	// per-agent dedicated harness homes whose settings.yaml pins a model.
	DshHome string
	// AgentPreset names a locally authored DSH agent preset (a directory
	// under $DSH_HOME/.agent-presets). When set, the node runs under that
	// customized agent instead of the stock headless persona: the preset's
	// headless.patch.yml is appended as a --patch overlay that carries the
	// persona override and prunes tool rows the preset does not provide.
	// An unknown preset or a preset without a headless patch fails the run.
	AgentPreset string
	// ReasoningEffort is the reasoning effort for the model
	// ("high" | "medium" | "low"), written into the temporary
	// agent-default-model patch (DSH's own model route honors it).
	ReasoningEffort string
	// Bin overrides the discovered dsh entry (binary or "node <bin.js>" prefix).
	Bin string
}

// DshExecutor executes tasks via the DeepSeek Harness one-shot profile
// (`dsh --profile headless "<task>"`).
type DshExecutor struct {
	// DshBin overrides binary discovery. A plain executable name is run
	// directly; an absolute path to a .js file is run through node.
	DshBin string
}

// New creates a DshExecutor with default settings.
func New() *DshExecutor {
	return &DshExecutor{}
}

// Exec runs `dsh --profile headless "<task>"` and captures stdout/stderr.
func (e *DshExecutor) Exec(ctx context.Context, prompt string, opts ExecOptions) (*ExecutorResult, error) {
	if opts.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
	}

	bin, prefix, err := e.command()
	if err != nil {
		return &ExecutorResult{ExitCode: -1, Stderr: err.Error()}, fmt.Errorf("%w: %v", ErrExecutorStart, err)
	}

	args := make([]string, 0, len(prefix)+4)
	args = append(args, prefix...)
	args = append(args, "--profile", "headless")

	// Per-node model (+ optional reasoning effort): write a temporary
	// patch-list overlay that replaces the `agent-default-model` composition
	// row. Removed after the run; the file only ever carries a model id and an
	// effort level, never credentials.
	var patchPath string
	var patchCleanup func()
	if strings.TrimSpace(opts.Model) != "" || strings.TrimSpace(opts.ReasoningEffort) != "" {
		patchPath, patchCleanup, err = writeModelPatch(strings.TrimSpace(opts.Model), strings.TrimSpace(opts.ReasoningEffort))
		if err != nil {
			return &ExecutorResult{ExitCode: -1, Stderr: err.Error()}, fmt.Errorf("%w: %v", ErrExecutorStart, err)
		}
		defer patchCleanup()
		args = append(args, "--patch", patchPath)
	}

	// Customized agent: append the preset's headless.patch.yml overlay, which
	// carries the preset persona and prunes tool rows the preset omits. It is
	// resolved under the same DSH_HOME the child will use; a missing preset or
	// patch fails loudly rather than silently running the stock persona.
	if strings.TrimSpace(opts.AgentPreset) != "" {
		presetPatch, presetErr := ResolvePresetPatch(opts.AgentPreset, opts.DshHome)
		if presetErr != nil {
			return &ExecutorResult{ExitCode: -1, Stderr: presetErr.Error()}, fmt.Errorf("%w: %v", ErrExecutorStart, presetErr)
		}
		args = append(args, "--patch", presetPatch)
	}

	// The task is a positional argument of the headless app. DSH has no stdin
	// task seam, so very long prompts are bounded by the OS command line
	// (~32K on Windows); the orchestrator keeps node prompts below that.
	args = append(args, prompt)

	cmd := exec.CommandContext(ctx, bin, args...)
	// One-shot headless must not flash a console window on Windows (the
	// desktop app has no console to inherit).
	proc.HideWindow(cmd)
	if strings.TrimSpace(opts.Workspace) != "" {
		cmd.Dir = opts.Workspace
	}
	cmd.Env = buildEnv(opts)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err = cmd.Run()

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

	result := &ExecutorResult{
		Output:    cleanHeadlessOutput(rawStdout),
		RawStdout: rawStdout,
		Stderr:    strings.TrimSpace(stderr.String()),
		ExitCode:  exitCode,
	}
	if exitCode != 0 {
		return result, fmt.Errorf("%w: exit code %d", ErrExecutorExit, exitCode)
	}
	if result.Output == "" {
		return result, ErrExecutorEmptyOutput
	}
	return result, nil
}

// cleanHeadlessOutput strips the `cmd.exe` "Active code page: NNNN" banner
// that a `.cmd` shim (npm dsh shim) prints to stdout on Windows, so the final
// assistant text stays clean even when the CLI is reached through a shim.
func cleanHeadlessOutput(raw string) string {
	lines := strings.Split(strings.ReplaceAll(raw, "\r\n", "\n"), "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		if strings.HasPrefix(strings.TrimSpace(line), "Active code page:") {
			continue
		}
		out = append(out, line)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// buildEnv constructs the child environment: the inherited environment plus
// the DSH permission boundary and optional harness home override.
func buildEnv(opts ExecOptions) []string {
	env := os.Environ()
	perm := strings.TrimSpace(opts.PermissionMode)
	if perm == "" {
		// DSH's own default boundary. Headless cannot answer "ask" approvals,
		// so orchestrated nodes should pick an explicit mode.
		perm = "workspace-write"
	}
	env = append(env, "DSH_PERMISSION_MODE="+perm)
	if strings.TrimSpace(opts.DshHome) != "" {
		env = append(env, "DSH_HOME="+strings.TrimSpace(opts.DshHome))
	}
	return env
}

// writeModelPatch writes a temporary cordis patch-list overlay that replaces
// the `agent-default-model` row with the requested model on the
// `deepseek-official` provider route, plus an optional reasoning effort
// level. The caller owns the returned cleanup.
func writeModelPatch(model string, reasoningEffort string) (string, func(), error) {
	f, err := os.CreateTemp("", "dsh-model-*.yml")
	if err != nil {
		return "", func() {}, err
	}
	path := f.Name()
	cfg := map[string]any{
		"provider": "deepseek-official",
		"model":    model,
	}
	if reasoningEffort != "" {
		cfg["reasoningEffort"] = reasoningEffort
	}
	patch := []any{
		map[string]any{
			"id":     "agent-default-model",
			"config": cfg,
		},
	}
	if err := yaml.NewEncoder(f).Encode(patch); err != nil {
		f.Close()
		os.Remove(path)
		return "", func() {}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(path)
		return "", func() {}, err
	}
	cleanup := func() { _ = os.Remove(path) }
	return path, cleanup, nil
}

// command resolves the dsh entry point. It returns the executable and any
// fixed prefix arguments (a node script for `node <bin.js>`).
func (e *DshExecutor) command() (bin string, prefix []string, err error) {
	if strings.TrimSpace(e.DshBin) != "" {
		return resolveCommand(strings.TrimSpace(e.DshBin))
	}
	if env := strings.TrimSpace(os.Getenv("DSH_BIN")); env != "" {
		return resolveCommand(env)
	}
	if js := discoverDshBinJS(); js != "" {
		node, nodeErr := exec.LookPath("node")
		if nodeErr != nil {
			node = "node"
		}
		return node, []string{js}, nil
	}
	// Fall back to PATH resolution (npm global shim or direct install).
	if path, lookErr := exec.LookPath("dsh"); lookErr == nil {
		return path, nil, nil
	}
	return "dsh", nil, nil
}

// resolveCommand decides whether an explicit Bin is a node script (absolute
// path ending in .js/.cjs/.mjs) or a plain executable.
func resolveCommand(bin string) (string, []string, error) {
	lower := strings.ToLower(bin)
	if strings.HasSuffix(lower, ".js") || strings.HasSuffix(lower, ".cjs") || strings.HasSuffix(lower, ".mjs") {
		node, err := exec.LookPath("node")
		if err != nil {
			node = "node"
		}
		return node, []string{bin}, nil
	}
	return bin, nil, nil
}

// Command resolves the dsh entry point as a process invocation: an executable
// and any fixed prefix arguments (a node script for `node <bin.js>`). Exported
// for health probes that must actually execute the version command.
func Command() (bin string, prefix []string, err error) {
	return (&DshExecutor{}).command()
}

// DiscoverBin returns the dsh entry found on this machine as a display string
// (e.g. "dsh" or "node <…>/bin.js"), or "" when nothing is installed.
func DiscoverBin() string {
	bin, prefix, err := (&DshExecutor{}).command()
	if err != nil {
		return ""
	}
	if len(prefix) > 0 {
		return strings.TrimSpace(bin + " " + strings.Join(prefix, " "))
	}
	return bin
}

// discoverDshBinJS locates the shipped `lib/bin.js` of an installed
// @deepseek-ai/dsh package (npm global layout). Running node directly against
// it skips the npm .cmd/.ps1 shims, which are unreliable as subprocesses.
func discoverDshBinJS() string {
	candidates := []string{
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "@deepseek-ai", "dsh", "lib", "bin.js"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "@deepseek-ai", "dsh", "lib", "bin.js"),
		"/usr/local/lib/node_modules/@deepseek-ai/dsh/lib/bin.js",
		"/usr/lib/node_modules/@deepseek-ai/dsh/lib/bin.js",
	}
	for _, candidate := range candidates {
		if candidate == "" {
			continue
		}
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	// npx cache layout: %LOCALAPPDATA%\npm-cache\_npx\<hash>\node_modules\@deepseek-ai\dsh\lib\bin.js
	if matches, err := filepath.Glob(filepath.Join(os.Getenv("LOCALAPPDATA"), "npm-cache", "_npx", "*", "node_modules", "@deepseek-ai", "dsh", "lib", "bin.js")); err == nil {
		for _, candidate := range matches {
			if info, statErr := os.Stat(candidate); statErr == nil && !info.IsDir() {
				return candidate
			}
		}
	}
	return ""
}
