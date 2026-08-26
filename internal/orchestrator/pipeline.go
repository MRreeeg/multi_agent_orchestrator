package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"sync"
	"time"

	"reasonix/internal/agent"
	"reasonix/internal/config"
	"reasonix/internal/event"
	"reasonix/internal/proc"
	"reasonix/internal/provider"
)

//go:embed prompts/*.md
var promptFS embed.FS

var detectWorkspaceForTest func() string

var mimoModelsCache struct {
	mu     sync.Mutex
	loaded bool
	models []string
}

// PipelineExecutor is the interface for executing a pipeline node.
type PipelineExecutor interface {
	Name() string
	// Execute runs the node. onStart is called once the serve process starts
	// (port allocated, cmd.Start succeeded) but before waiting for readiness,
	// so the frontend can show the port badge immediately.
	Execute(ctx context.Context, spec ExecSpec, onStart func(endpoint string, port int)) (*ExecResult, error)
}

// LineStreamingExecutor is optionally implemented by executors that can
// stream subprocess stdout lines while executing (used by the requirement
// analysis to show live thinking progress).
type LineStreamingExecutor interface {
	ExecuteWithProgress(ctx context.Context, spec ExecSpec, onStart func(endpoint string, port int), onLine func(line string)) (*ExecResult, error)
}

// RuntimeManager is the interface for managing serve runtime instances.
type RuntimeManager interface {
	Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error)
	Release(runtimeID string) error
	Stop(runtimeID string) error
	Get(runtimeID string) (*RuntimeState, bool)
	List() []*RuntimeState
}

type reasonixRuntime struct {
	ID         string
	Port       int
	Endpoint   string
	ModelRef   string
	Workspace  string
	RuntimeDir string
	Skill      string
	Cmd        *exec.Cmd
	Stderr     *bytes.Buffer
	done       chan struct{}
	exitErr    error
	StartedAt  time.Time
	LastUsedAt time.Time
}

type ReasonixRuntimeManager struct {
	mu       sync.Mutex
	runtimes map[string]*reasonixRuntime
}

func newReasonixRuntimeManager() *ReasonixRuntimeManager {
	return &ReasonixRuntimeManager{runtimes: make(map[string]*reasonixRuntime)}
}

// cleanupAll kills all managed serve processes and clears the registry.

// Borrow acquires or creates a runtime for the given spec.

// Release marks a runtime as idle (task finished, process still alive).

// Stop kills a runtime process and marks it as stopped.

// Get returns a runtime by ID.

// List returns all managed runtimes.

// runtimeAccessMode describes how the browser should reach a retained runtime.
// Codex app-server is an internal WebSocket protocol and must be proxied through
// the Orchestrator Runtime Console rather than opened directly by the browser.
func runtimeAccessMode(executor ExecutorType, mode string) string {
	if executor == ExecutorCodex && strings.EqualFold(mode, "serve") {
		return "runtime_console"
	}
	if executor == ExecutorMimo && strings.EqualFold(mode, "serve") {
		// mimo acp is a retained provider runtime proxied through the
		// Orchestrator Runtime Console; the legacy `mimo serve` HTTP flow is no
		// longer spawned by this build.
		return "runtime_console"
	}
	if executor == ExecutorClaude && strings.EqualFold(mode, "serve") {
		// claude stream-json is a retained provider runtime proxied through
		// the Orchestrator Runtime Console; the browser never dials the CLI.
		return "runtime_console"
	}
	if executor == ExecutorOpencode && strings.EqualFold(mode, "serve") {
		// opencode serve is driven purely over its HTTP API (no bound UI
		// conversation thread); the browser must use the Runtime Console.
		return "runtime_console"
	}
	return "browser"
}

// emitRuntimeEvent sends a runtime status event for a node. output is a short
// summary of the latest assistant answer so the canvas can show it without
// opening the Runtime Console; it is JSON-escaped and truncated.
func (s *Store) emitRuntimeEvent(nodeID string, endpoint string, port int, status string, executor string, accessMode string, output string) {
	outJSON := ""
	if strings.TrimSpace(output) != "" {
		summary := truncateRune(output, 200)
		raw, _ := json.Marshal(summary)
		outJSON = `,"output":` + string(raw)
	}
	detail := fmt.Sprintf(`{"endpoint":"%s","port":%d,"status":"%s","nodeID":"%s","executor":"%s","accessMode":"%s"%s}`, endpoint, port, status, nodeID, executor, accessMode, outJSON)
	s.emit(event.Event{Kind: event.PipelineNodeRuntime, Text: nodeID, Detail: detail})
}

// truncateRune truncates s to at most n runes (UTF-8 safe).
func truncateRune(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}

// managedRuntimeState returns the live runtime manager view when the runtime is
// still attached to this process. Persisted RuntimeState records remain the
// history source after a process restart, so callers must tolerate no live view.
func managedRuntimeState(runtimeID string) (*RuntimeState, bool) {
	if runtimeID == "" {
		return nil, false
	}
	if rt, ok := mimoRuntimeMgr.Get(runtimeID); ok {
		return rt, true
	}
	if rt, ok := reasonixRuntimeMgr.Get(runtimeID); ok {
		return rt, true
	}
	if rt, ok := codexRuntimeMgr.Get(runtimeID); ok {
		return rt, true
	}
	if rt, ok := claudeRuntimeMgr.Get(runtimeID); ok {
		return rt, true
	}
	if rt, ok := opencodeRuntimeMgr.Get(runtimeID); ok {
		return rt, true
	}
	return nil, false
}

// applyLiveRuntimeState overlays provider-owned lifecycle fields while keeping
// orchestration identifiers (session, binding, run and node) owned by the Store.
func applyLiveRuntimeState(target *RuntimeState) {
	if target == nil {
		return
	}
	live, ok := managedRuntimeState(target.RuntimeID)
	if !ok {
		return
	}
	if live.Executor != "" {
		target.Executor = live.Executor
	}
	if live.Model != "" {
		target.Model = live.Model
	}
	if live.Endpoint != "" {
		target.Endpoint = live.Endpoint
	}
	if live.Port != 0 {
		target.Port = live.Port
	}
	if live.PID != 0 {
		target.PID = live.PID
	}
	if live.Status != "" {
		target.Status = live.Status
	}
	target.Error = live.Error
	if !live.CreatedAt.IsZero() {
		target.CreatedAt = live.CreatedAt
	}
	if !live.LastActiveAt.IsZero() {
		target.LastActiveAt = live.LastActiveAt
	}
	if live.AccessMode != "" {
		target.AccessMode = live.AccessMode
	}
	if live.ApprovalMode != "" {
		target.ApprovalMode = live.ApprovalMode
	}
	if live.ExecutionMode != "" {
		target.ExecutionMode = live.ExecutionMode
	}
	target.ThreadID = live.ThreadID
	target.TurnID = live.TurnID
}

func waitForHTTPStatus(url string, timeout time.Duration, cmd *exec.Cmd) error {
	return waitForHTTPStatusContext(context.Background(), url, timeout, cmd)
}

func waitForHTTPStatusContext(ctx context.Context, url string, timeout time.Duration, cmd *exec.Cmd) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Check if process has exited early
		if cmd != nil && cmd.Process != nil {
			if cmd.ProcessState != nil || !processAlive(cmd) {
				errMsg := ""
				if cmd.Stderr != nil {
					errMsg = cmd.Stderr.(*bytes.Buffer).String()
				}
				return fmt.Errorf("mimo serve exited before ready; stderr: %s", errMsg)
			}
		}
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode < 500 {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	// Timeout: include stderr in error
	errMsg := ""
	if cmd != nil && cmd.Stderr != nil {
		errMsg = cmd.Stderr.(*bytes.Buffer).String()
	}
	return fmt.Errorf("endpoint %s not ready after %v; serve stderr: %s", url, timeout, errMsg)
}

func waitForServeReady(endpoint string, timeout time.Duration, cmd *exec.Cmd, stderr *bytes.Buffer) error {
	return waitForServeReadyContext(context.Background(), endpoint, timeout, cmd, stderr)
}

func waitForServeReadyContext(ctx context.Context, endpoint string, timeout time.Duration, cmd *exec.Cmd, stderr *bytes.Buffer) error {
	deadline := time.Now().Add(timeout)
	hadTCPListener := false
	lastHTTPStatus := 0
	lastHTTPError := ""

	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if cmd != nil && cmd.Process != nil {
			if cmd.ProcessState != nil || !processAlive(cmd) {
				return fmt.Errorf("serve exited before ready; %s", readinessDiagnostics(endpoint, hadTCPListener, lastHTTPStatus, lastHTTPError, stderr))
			}
		}

		if waitForTCPListener(endpoint, 1200*time.Millisecond) {
			hadTCPListener = true
			status, httpErr := probeHTTPStatus(endpoint + "/")
			if httpErr == nil {
				if status < 500 {
					return nil
				}
				if status == http.StatusServiceUnavailable {
					// Newer mimocode builds can keep `/` on 503 while the headless
					// server is warming up, but still accept `mimo run --attach` shortly
					// after. Treat a listening port plus 503 as ready enough and let the
					// attach request be the real capability check.
					return nil
				}
				lastHTTPStatus = status
				lastHTTPError = ""
			} else {
				lastHTTPStatus = 0
				lastHTTPError = httpErr.Error()
				// For mimocode serve, a listening port is a strong enough readiness
				// signal. Some versions accept connections before serving `/`.
				return nil
			}
		} else {
			lastHTTPStatus = 0
			lastHTTPError = "tcp not listening yet"
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}

	return fmt.Errorf("endpoint %s not ready after %v; %s", endpoint, timeout, readinessDiagnostics(endpoint, hadTCPListener, lastHTTPStatus, lastHTTPError, stderr))
}

func waitForTCPListener(endpoint string, timeout time.Duration) bool {
	addr := strings.TrimPrefix(endpoint, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

func probeHTTPStatus(url string) (int, error) {
	client := &http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Get(url)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func readinessDiagnostics(endpoint string, hadTCPListener bool, lastHTTPStatus int, lastHTTPError string, stderr *bytes.Buffer) string {
	parts := []string{fmt.Sprintf("tcp_listening=%t", hadTCPListener)}
	if lastHTTPStatus > 0 {
		parts = append(parts, fmt.Sprintf("last_http_status=%d", lastHTTPStatus))
	}
	if lastHTTPError != "" {
		parts = append(parts, fmt.Sprintf("last_http_error=%q", lastHTTPError))
	}
	if stderr != nil {
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			parts = append(parts, fmt.Sprintf("serve_stderr=%q", msg))
		}
	}
	return strings.Join(parts, "; ")
}

// newRetainedRuntimeCommand deliberately ignores the per-node context. A serve
// process is shared by multiple task requests/Loop iterations and must only be
// terminated by RuntimeManager.Stop or cleanupAll. The context is still passed
// at the call site to make this lifecycle distinction explicit and testable.
func newRetainedRuntimeCommand(_ context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	// Retained runtimes (codex app-server / mimo acp / claude serve / reasonix
	// serve) must not pop console windows on Windows; the desktop app has no
	// console of its own to inherit.
	proc.HideWindow(cmd)
	return cmd
}

// runtimeExited reports whether the manager's Wait watcher observed process
// termination. A retained runtime must not be reused after an external crash or
// self-exit; otherwise the next Loop iteration keeps sending requests to a dead
// endpoint and the embedded Agent remains stuck in reconnecting.
func runtimeExited(done <-chan struct{}) bool {
	if done == nil {
		return false
	}
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// stopRuntimeProcess is the explicit lifecycle boundary for a retained serve
// process. The watcher owns cmd.Wait, so callers only kill and wait for the
// watcher to observe the exit. It is safe to call for an already exited process.
func stopRuntimeProcess(cmd *exec.Cmd, done <-chan struct{}) {
	if cmd != nil && cmd.Process != nil && !runtimeExited(done) {
		killProcessTree(cmd)
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
}

// killProcessTree terminates a provider process and its descendants. Mimo is a
// Node-based CLI and may leave a child worker alive when the parent is killed;
// killing only cmd.Process is therefore not sufficient to stop token usage after
// a reviewer timeout or permission failure.
func killProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	if runtime.GOOS == "windows" {
		pid := strconv.Itoa(cmd.Process.Pid)
		kill := exec.Command("taskkill", "/T", "/F", "/PID", pid)
		proc.HideWindow(kill)
		if err := kill.Run(); err == nil {
			return
		}
	}
	_ = cmd.Process.Kill()
}

// processAlive checks if a process is still running by checking ProcessState.
func processAlive(cmd *exec.Cmd) bool {
	if cmd == nil || cmd.Process == nil {
		return false
	}
	// ProcessState is non-nil after Wait() returns
	return cmd.ProcessState == nil
}

func (m *ReasonixRuntimeManager) watchRuntime(key string, rt *reasonixRuntime) {
	err := rt.Cmd.Wait()
	m.mu.Lock()
	rt.exitErr = err
	close(rt.done)
	if current := m.runtimes[key]; current == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
}

func (m *ReasonixRuntimeManager) runtimeKey(spec ExecSpec) string {
	workspace := spec.Workspace
	if workspace == "" {
		workspace = "."
	}
	return spec.NodeID + "|" + spec.ModelRef + "|" + workspace + "|" + spec.Skill
}

// cleanupAll kills all managed serve processes and clears the registry.
func (m *ReasonixRuntimeManager) cleanupAll() {
	m.mu.Lock()
	runtimes := make([]*reasonixRuntime, 0, len(m.runtimes))
	for key, rt := range m.runtimes {
		delete(m.runtimes, key)
		runtimes = append(runtimes, rt)
	}
	m.mu.Unlock()
	for _, rt := range runtimes {
		stopRuntimeProcess(rt.Cmd, rt.done)
	}
}

// Borrow acquires or creates a runtime for the given spec.
func (m *ReasonixRuntimeManager) Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error) {
	rt, err := m.ensure(ctx, spec, nil)
	if err != nil {
		return nil, err
	}
	pid := 0
	if rt.Cmd != nil && rt.Cmd.Process != nil {
		pid = rt.Cmd.Process.Pid
	}
	return &RuntimeState{
		RuntimeID:     rt.ID,
		NodeID:        spec.NodeID,
		Executor:      "reasonix",
		Model:         spec.ModelRef,
		Endpoint:      rt.Endpoint,
		Port:          rt.Port,
		PID:           pid,
		Status:        RuntimeReady,
		CreatedAt:     rt.StartedAt,
		LastActiveAt:  time.Now(),
		CleanupPolicy: policy,
	}, nil
}

// Release marks a runtime as idle.
func (m *ReasonixRuntimeManager) Release(runtimeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			rt.LastUsedAt = time.Now()
			return nil
		}
	}
	return fmt.Errorf("runtime %s not found", runtimeID)
}

// Stop kills a runtime process.
func (m *ReasonixRuntimeManager) Stop(runtimeID string) error {
	m.mu.Lock()
	for key, rt := range m.runtimes {
		if rt.ID == runtimeID {
			delete(m.runtimes, key)
			m.mu.Unlock()
			stopRuntimeProcess(rt.Cmd, rt.done)
			return nil
		}
	}
	m.mu.Unlock()
	return fmt.Errorf("runtime %s not found", runtimeID)
}

// Get returns a runtime by ID.
func (m *ReasonixRuntimeManager) Get(runtimeID string) (*RuntimeState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			pid := 0
			if rt.Cmd != nil && rt.Cmd.Process != nil {
				pid = rt.Cmd.Process.Pid
			}
			return &RuntimeState{
				RuntimeID: rt.ID, Executor: "reasonix",
				Model: rt.ModelRef, Endpoint: rt.Endpoint, Port: rt.Port,
				PID: pid, Status: RuntimeReady, CreatedAt: rt.StartedAt,
				LastActiveAt: rt.LastUsedAt, CleanupPolicy: CleanupRetained,
				AccessMode: "browser",
			}, true
		}
	}
	return nil, false
}

// List returns all managed runtimes.
func (m *ReasonixRuntimeManager) List() []*RuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []*RuntimeState
	for _, rt := range m.runtimes {
		pid := 0
		if rt.Cmd != nil && rt.Cmd.Process != nil {
			pid = rt.Cmd.Process.Pid
		}
		out = append(out, &RuntimeState{
			RuntimeID: rt.ID, Executor: "reasonix",
			Model: rt.ModelRef, Endpoint: rt.Endpoint, Port: rt.Port,
			PID: pid, Status: RuntimeReady, CreatedAt: rt.StartedAt,
			LastActiveAt: rt.LastUsedAt, CleanupPolicy: CleanupRetained,
			AccessMode: "browser",
		})
	}
	return out
}

func (m *ReasonixRuntimeManager) ensure(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*reasonixRuntime, error) {
	key := m.runtimeKey(spec)
	m.mu.Lock()
	if rt := m.runtimes[key]; rt != nil {
		if !runtimeExited(rt.done) {
			rt.LastUsedAt = time.Now()
			m.mu.Unlock()
			return rt, nil
		}
		delete(m.runtimes, key)
	}
	port := findFreePort()
	rt := &reasonixRuntime{
		ID:         fmt.Sprintf("reasonix_rt_%d", time.Now().UnixNano()),
		Port:       port,
		Endpoint:   fmt.Sprintf("http://127.0.0.1:%d", port),
		ModelRef:   spec.ModelRef,
		Workspace:  spec.Workspace,
		RuntimeDir: reasonixRuntimeDir(spec.Workspace, key),
		Skill:      spec.Skill,
		StartedAt:  time.Now(),
		LastUsedAt: time.Now(),
		done:       make(chan struct{}),
	}
	m.runtimes[key] = rt
	m.mu.Unlock()

	bin := findReasonixBin()
	args := reasonixServeArgs(port, spec.ModelRef)
	// Reasonix serve does not support a --skill flag. The skill remains part
	// of the node metadata and is applied by the prompt construction layer.
	// Keep the long-lived serve process independent from the current node
	// context; Stop() is the explicit runtime lifecycle boundary.
	cmd := newRetainedRuntimeCommand(ctx, bin, args...)
	if err := os.MkdirAll(rt.RuntimeDir, 0755); err != nil {
		m.mu.Lock()
		delete(m.runtimes, key)
		m.mu.Unlock()
		return nil, err
	}
	cmd.Dir = rt.RuntimeDir
	cmd.Stdout = io.Discard
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		m.mu.Lock()
		delete(m.runtimes, key)
		m.mu.Unlock()
		return nil, err
	}
	rt.Cmd = cmd
	rt.Stderr = stderr
	go m.watchRuntime(key, rt)
	// Notify caller immediately — process started, port known.
	if onStart != nil {
		onStart(rt.Endpoint, rt.Port)
	}
	if err := waitForHTTPStatusContext(ctx, rt.Endpoint+"/", 20*time.Second, cmd); err != nil {
		m.mu.Lock()
		if current := m.runtimes[key]; current == rt {
			delete(m.runtimes, key)
		}
		m.mu.Unlock()
		stopRuntimeProcess(cmd, rt.done)
		if msg := strings.TrimSpace(stderr.String()); msg != "" {
			return nil, fmt.Errorf("%w; serve stderr: %s", err, msg)
		}
		return nil, err
	}
	return rt, nil
}

func reasonixServeArgs(port int, model string) []string {
	args := []string{"serve", "--addr", fmt.Sprintf("127.0.0.1:%d", port)}
	if model != "" {
		args = append(args, "--model", model)
	}
	return args
}

// buildExecutorPrompt adds the resolved Skill instructions exactly once for
// executors that receive a raw prompt (Reasonix/MiMo). Codex injects the same
// content in its own adapter, so it deliberately does not call this helper.
func buildExecutorPrompt(spec ExecSpec) string {
	prompt := spec.Prompt
	if strings.TrimSpace(spec.SkillContent) == "" {
		return prompt
	}
	return fmt.Sprintf("# SYSTEM-LEVEL SKILL INSTRUCTIONS\n\n以下是本节点必须遵守的 Skill 指令。\nSkill 名称：%s\n\n<skill>\n%s\n</skill>\n\n# TASK\n\n%s",
		spec.Skill, spec.SkillContent, prompt)
}

// ReasonixExecutor executes nodes via `reasonix serve`.
type ReasonixExecutor struct{}

func (e *ReasonixExecutor) Name() string { return "reasonix" }

var reasonixRuntimeMgr = newReasonixRuntimeManager()

func (e *ReasonixExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if strings.EqualFold(strings.TrimSpace(spec.Mode), "run") {
		return executeReasonixRun(ctx, spec)
	}
	rt, err := reasonixRuntimeMgr.ensure(ctx, spec, onStart)
	if err != nil {
		return &ExecResult{ExitCode: -1}, err
	}
	// Configure serve BEFORE submitting task — approvalMode and goal must be set first.
	executorPrompt := buildExecutorPrompt(spec)
	configureReasonixRuntime(rt.Endpoint, spec.ApprovalMode, spec.ExecutionMode, executorPrompt)
	start := time.Now()
	output, usage, err := submitTask(ctx, rt.Port, executorPrompt)
	duration := time.Since(start).Milliseconds()
	exitCode := 0
	if err != nil {
		exitCode = -1
	}

	return &ExecResult{
		FinalText:  strings.TrimSpace(output),
		RawStdout:  output,
		RawStderr:  "",
		ExitCode:   exitCode,
		DurationMs: duration,
		RuntimeID:  rt.ID,
		Endpoint:   rt.Endpoint,
		TokenUsage: usage,
	}, err
}

// ExecuteWithProgress is a no-streaming delegation: the requirement analysis
// never routes reasonix through the executor registry (it uses
// spawnReasonixAnalysis), so the subprocess run path keeps its buffers.
func (e *ReasonixExecutor) ExecuteWithProgress(ctx context.Context, spec ExecSpec, onStart func(string, int), onLine func(line string)) (*ExecResult, error) {
	return e.Execute(ctx, spec, onStart)
}

// MimoExecutor executes nodes via `mimo run`.
type MimoExecutor struct{}

func (e *MimoExecutor) Name() string { return "mimo" }

// Package-level functions for runtime API access.
func ListReasonixRuntimes() []*RuntimeState              { return reasonixRuntimeMgr.List() }
func GetReasonixRuntime(id string) (*RuntimeState, bool) { return reasonixRuntimeMgr.Get(id) }
func StopReasonixRuntime(id string) error                { return reasonixRuntimeMgr.Stop(id) }

func (e *MimoExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	return e.ExecuteWithProgress(ctx, spec, onStart, nil)
}

// ExecuteWithProgress runs a Mimo node, forwarding each non-empty stdout line
// to onLine when set (run mode only; serve mode delegates without streaming).
func (e *MimoExecutor) ExecuteWithProgress(ctx context.Context, spec ExecSpec, onStart func(string, int), onLine func(line string)) (*ExecResult, error) {
	if strings.EqualFold(strings.TrimSpace(spec.Mode), "run") {
		return executeMimoRun(ctx, spec, onLine)
	}
	return mimoRuntimeMgr.Execute(ctx, spec, onStart)
}

type synchronizedOutput struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (o *synchronizedOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.Write(p)
}

func (o *synchronizedOutput) String() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.b.String()
}

// runMimoCommand executes a single Mimo client request and watches stderr while
// it is running. Mimo may repeatedly ask for an external_directory approval;
// in a non-interactive orchestrator there is nobody who can answer it, so
// waiting for cmd.Run() would burn tokens until the outer 30-minute timeout.
func runMimoCommand(ctx context.Context, args []string, workspace string, onLine func(line string)) (string, string, error) {
	cmd := exec.Command("mimo", args...)
	proc.HideWindow(cmd)
	if strings.TrimSpace(workspace) != "" {
		cmd.Dir = workspace
	}
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", "", err
	}
	if err := cmd.Start(); err != nil {
		return "", "", err
	}

	var stdout, stderr synchronizedOutput
	permissionCh := make(chan string, 1)
	terminalCh := make(chan struct{}, 1)
	copyDone := make(chan struct{}, 2)
	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = stdout.Write([]byte(line + "\n"))
			// Permission events are part of the JSON stdout stream in some Mimo
			// versions (and only appear on stderr in others). Watch both streams
			// so a non-interactive Reviewer cannot wait for an approval forever.
			if reason := mimoFastFailureReason(line); reason != "" {
				select {
				case permissionCh <- reason:
				default:
				}
			}
			// `session.idle` is emitted by some Mimo versions even when the
			// attached client has not received an assistant message yet. Do not
			// kill the client on that event unless the accumulated stream already
			// contains assistant text; otherwise the first iteration can be
			// reported as an empty/failed Review while Mimo is still delivering
			// the actual response.
			if mimoOutputLineTerminal(line) && mimoTerminalStreamHasOutput(line, stdout.String()) {
				select {
				case terminalCh <- struct{}{}:
				default:
				}
			}
			if onLine != nil {
				if trimmed := strings.TrimSpace(line); trimmed != "" {
					onLine(trimmed)
				}
			}
		}
		copyDone <- struct{}{}
	}()
	go func() {
		scanner := bufio.NewScanner(stderrPipe)
		scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Text()
			_, _ = stderr.Write([]byte(line + "\n"))
			if reason := mimoFastFailureReason(line); reason != "" {
				select {
				case permissionCh <- reason:
				default:
				}
			}
		}
		copyDone <- struct{}{}
	}()

	waitCh := make(chan error, 1)
	go func() { waitCh <- cmd.Wait() }()

	var forcedReason string
	var waitErr error
	select {
	case forcedReason = <-permissionCh:
		killProcessTree(cmd)
		waitErr = <-waitCh
		_, _ = stderr.Write([]byte(forcedReason + "\n"))
		if waitErr == nil {
			waitErr = fmt.Errorf("mimo execution stopped: %s", forcedReason)
		} else {
			waitErr = fmt.Errorf("mimo execution stopped: %s: %w", forcedReason, waitErr)
		}
	case <-terminalCh:
		// `mimo run --attach` streams the result from the retained serve
		// process. Some Mimo versions emit the terminal event but keep the
		// client alive while the server remains idle. The server must stay
		// retained for the next Loop iteration; only terminate this one-shot
		// client so the reviewer hand-off can continue immediately.
		killProcessTree(cmd)
		waitErr = <-waitCh
	case waitErr = <-waitCh:
	case <-ctx.Done():
		killProcessTree(cmd)
		waitErr = <-waitCh
		if waitErr == nil {
			waitErr = ctx.Err()
		}
	}
	<-copyDone
	<-copyDone
	return stdout.String(), stderr.String(), waitErr
}

func mimoTerminalStreamHasOutput(line, stream string) bool {
	line = strings.TrimSpace(stripANSIEscapeCodes(line))
	var evt map[string]any
	if json.Unmarshal([]byte(line), &evt) == nil {
		typ, _ := evt["type"].(string)
		if typ == "final" || typ == "result" {
			return nonEmptyMimoEventText(evt)
		}
		if typ == "message.part.updated" {
			props, _ := evt["properties"].(map[string]any)
			part, _ := props["part"].(map[string]any)
			if text, _ := part["text"].(string); strings.TrimSpace(text) != "" {
				return true
			}
		}
	}
	parsed, _ := parseMimoJSONOutput(stream)
	return strings.TrimSpace(parsed) != ""
}

// mimoOutputLineTerminal reports whether one JSON event proves that the
// current `mimo run` request has reached its terminal response. This is
// intentionally separate from parseMimoOutput: the former controls process
// lifetime, while the latter extracts the final text after all pipes drain.
func mimoOutputLineTerminal(line string) bool {
	line = strings.TrimSpace(stripANSIEscapeCodes(line))
	if line == "" {
		return false
	}
	var evt map[string]any
	if err := json.Unmarshal([]byte(line), &evt); err != nil {
		// A reviewer is required to emit one JSON object. If Mimo prints that
		// object as a bare line instead of wrapping it in an event, it is safe
		// to stop the client as soon as the complete decision is present.
		return validLoopReviewOutput(line)
	}
	typ, _ := evt["type"].(string)
	switch typ {
	case "session.idle":
		return true
	case "final", "result":
		return nonEmptyMimoEventText(evt)
	case "text":
		// Mimo v2.5 can emit the assistant response as a top-level text
		// event whose payload is stored in part.text. A complete review JSON
		// is safe to treat as terminal; ordinary executor text still waits
		// for step_finish/session.idle below.
		return validLoopReviewOutput(mimoEventText(evt))
	case "step_finish":
		// step_finish is the terminal event used by Mimo's JSON event stream.
		// The caller still requires accumulated assistant text before it
		// terminates the attached client.
		return true
	case "message.updated":
		props, _ := evt["properties"].(map[string]any)
		info, _ := props["info"].(map[string]any)
		if role, _ := info["role"].(string); role == "assistant" {
			if _, ok := info["finish"]; ok {
				return true
			}
			if timing, ok := info["time"].(map[string]any); ok {
				_, completed := timing["completed"]
				return completed
			}
		}
	case "message.part.updated":
		props, _ := evt["properties"].(map[string]any)
		part, _ := props["part"].(map[string]any)
		if partType, _ := part["type"].(string); partType == "text" {
			text, _ := part["text"].(string)
			return validLoopReviewOutput(text)
		}
	case "message.part.delta":
		// A delta does not prove that the assistant has finished. The
		// following message.updated/session.idle event does.
		return false
	}
	return false
}

func nonEmptyMimoEventText(evt map[string]any) bool {
	return strings.TrimSpace(mimoEventText(evt)) != ""
}

// mimoEventText extracts assistant text from the event shapes emitted by the
// supported Mimo/OpenCode clients. Mimo v2.5 uses a top-level `text` event
// with `part.text`, while older clients put text under properties.part or
// directly on final/result/message events.
func mimoEventText(evt map[string]any) string {
	if text, _ := evt["text"].(string); strings.TrimSpace(text) != "" {
		return text
	}
	if content, _ := evt["content"].(string); strings.TrimSpace(content) != "" {
		return content
	}
	if part, ok := evt["part"].(map[string]any); ok {
		if text, _ := part["text"].(string); strings.TrimSpace(text) != "" {
			return text
		}
	}
	if props, ok := evt["properties"].(map[string]any); ok {
		if text, _ := props["text"].(string); strings.TrimSpace(text) != "" {
			return text
		}
		if part, ok := props["part"].(map[string]any); ok {
			if text, _ := part["text"].(string); strings.TrimSpace(text) != "" {
				return text
			}
		}
	}
	return ""
}

// stallMaintenanceEnabled resolves whether reviewer-driven stall repair runs
// for a node: the global REASONIX_STALL_MAINTENANCE=off env switch disables
// everything; otherwise the node-level switch applies (nil means enabled).
func stallMaintenanceEnabled(nodeSwitch *bool) bool {
	if v := strings.TrimSpace(os.Getenv("REASONIX_STALL_MAINTENANCE")); v != "" {
		switch strings.ToLower(v) {
		case "off", "false", "0", "disable", "disabled":
			return false
		}
	}
	return nodeSwitch == nil || *nodeSwitch
}

func mimoFastFailureReason(line string) string {
	clean := strings.ToLower(stripANSIEscapeCodes(strings.TrimSpace(line)))
	switch {
	case strings.Contains(clean, `"type":"permission.asked"`) || strings.Contains(clean, `"type": "permission.asked"`):
		return "permission denied: permission.asked"
	case strings.Contains(clean, "permission requested") && strings.Contains(clean, "external_directory"):
		return "permission denied: external_directory"
	case strings.Contains(clean, "auto-rejecting") && strings.Contains(clean, "external_directory"):
		return "permission denied: external_directory"
	case strings.Contains(clean, "file not found:"):
		return "invalid attachment: file not found"
	case strings.Contains(clean, "command line is too long"):
		return "invalid command: command line is too long"
	case strings.Contains(clean, "unknown option") || strings.Contains(clean, "unknown argument"):
		return "invalid command option"
	default:
		return ""
	}
}

func isMimoHardFailure(err error, stderr string) bool {
	if err == nil {
		return false
	}
	if strings.Contains(strings.ToLower(err.Error()), "context canceled") || strings.Contains(strings.ToLower(err.Error()), "deadline exceeded") {
		return true
	}
	return mimoFastFailureReason(stderr) != ""
}

func mimoModelArg(modelRef string) string {
	modelRef = strings.TrimSpace(modelRef)
	if modelRef == "" {
		return ""
	}
	if !strings.Contains(modelRef, "/") {
		return ""
	}
	return modelRef
}

func parseMimoJSONOutput(stdout string) (string, error) {
	lines := strings.Split(stdout, "\n")
	finalText := ""
	var errs []string
	// Mimo's JSON stream follows the OpenCode event protocol. Text can arrive
	// as message.part.updated (full part) or message.part.delta (incremental
	// delta), while older builds use direct final/result/message events. Keep
	// one buffer per part so repeated part.updated events do not duplicate the
	// assistant response.
	partText := make(map[string]string)
	partOrder := make([]string, 0)

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var evt map[string]any
		if err := json.Unmarshal([]byte(line), &evt); err != nil {
			continue
		}
		typ, _ := evt["type"].(string)
		switch typ {
		case "error":
			if e, ok := evt["error"].(map[string]any); ok {
				if data, ok := e["data"].(map[string]any); ok {
					if msg, ok := data["message"].(string); ok && strings.TrimSpace(msg) != "" {
						errs = append(errs, strings.TrimSpace(msg))
						continue
					}
				}
				if msg, ok := e["message"].(string); ok && strings.TrimSpace(msg) != "" {
					errs = append(errs, strings.TrimSpace(msg))
					continue
				}
			}
			errs = append(errs, line)
		case "final", "result", "message":
			if text := strings.TrimSpace(mimoEventText(evt)); text != "" {
				finalText = text
			}
		case "text":
			// Mimo v2.5 emits the complete assistant message in this event
			// shape: {"type":"text","part":{"text":"..."}}.
			// Do not discard it merely because it is not an OpenCode
			// message.part.updated event.
			if text := strings.TrimSpace(mimoEventText(evt)); text != "" {
				finalText = text
			}
		case "step_finish":
			// Terminal marker only; the response was captured by a preceding
			// text/message event.
		case "message.part.updated":
			props, _ := evt["properties"].(map[string]any)
			part, _ := props["part"].(map[string]any)
			if partType, _ := part["type"].(string); partType == "text" {
				id, _ := part["id"].(string)
				text, _ := part["text"].(string)
				if id == "" {
					id = "__default__"
				}
				if _, seen := partText[id]; !seen {
					partOrder = append(partOrder, id)
				}
				partText[id] = text
			}
		case "message.part.delta":
			props, _ := evt["properties"].(map[string]any)
			id, _ := props["partID"].(string)
			if id == "" {
				id = "__default__"
			}
			delta, _ := props["delta"].(string)
			if _, seen := partText[id]; !seen {
				partOrder = append(partOrder, id)
			}
			partText[id] += delta
		}
	}
	if len(partOrder) > 0 {
		var parts []string
		for _, id := range partOrder {
			if text := strings.TrimSpace(partText[id]); text != "" {
				parts = append(parts, text)
			}
		}
		if len(parts) > 0 {
			finalText = strings.Join(parts, "")
		}
	}

	if len(errs) > 0 && strings.TrimSpace(finalText) == "" {
		return finalText, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return finalText, nil
}

func parseMimoOutput(stdout, stderr string) (string, error) {
	finalText, err := parseMimoJSONOutput(stdout)
	if strings.TrimSpace(finalText) != "" {
		return strings.TrimSpace(finalText), nil
	}
	if err != nil {
		return finalText, err
	}
	if trimmed := strings.TrimSpace(stripANSIEscapeCodes(stdout)); trimmed != "" {
		return trimmed, nil
	}
	if strings.TrimSpace(stderr) == "" {
		return "", nil
	}
	finalText, err = parseMimoJSONOutput(stderr)
	if strings.TrimSpace(finalText) != "" {
		return strings.TrimSpace(finalText), nil
	}
	if err != nil {
		return finalText, err
	}
	return "", nil
}

func buildMimoPromptArtifacts(spec ExecSpec) (string, string, func(), error) {
	if !shouldUseMimoPromptAttachment(spec.Prompt) {
		return "", strings.TrimSpace(spec.Prompt), func() {}, nil
	}
	if spec.Workspace == "" {
		return "", "", func() {}, fmt.Errorf("workspace is required for mimo executor")
	}
	if err := os.MkdirAll(spec.Workspace, 0755); err != nil {
		return "", "", func() {}, err
	}
	tmpDir := filepath.Join(spec.Workspace, ".mimo-orchestrator")
	if err := os.MkdirAll(tmpDir, 0755); err != nil {
		return "", "", func() {}, err
	}
	path := filepath.Join(tmpDir, fmt.Sprintf("%s-%d.md", sanitizeFilename(spec.NodeID), time.Now().UnixNano()))
	if err := os.WriteFile(path, []byte(spec.Prompt), 0644); err != nil {
		return "", "", func() {}, err
	}
	msg := "请优先阅读附件中的完整上下文，然后在当前工作目录中执行任务，并直接返回最终结果。"
	cleanup := func() { _ = os.Remove(path) }
	return path, msg, cleanup, nil
}

func shouldUseMimoPromptAttachment(prompt string) bool {
	prompt = strings.TrimSpace(prompt)
	if prompt == "" {
		return false
	}
	if len([]rune(prompt)) <= 240 && !strings.Contains(prompt, "\n") {
		return false
	}
	return true
}

func executeReasonixRun(ctx context.Context, spec ExecSpec) (*ExecResult, error) {
	bin := findReasonixBin()
	args := []string{"run"}
	sessionPath := ""
	if spec.ModelRef != "" {
		args = append(args, "--model", spec.ModelRef)
	}
	// reasonix run 不支持 --skill 参数，不拼接。
	// 不传 --dir，reasonix run 不支持此参数。用 cmd.Dir 设置工作目录。
	if spec.MaxSteps > 0 {
		args = append(args, "--max-steps", fmt.Sprint(spec.MaxSteps))
	}
	executorPrompt := buildExecutorPrompt(spec)
	args = append(args, executorPrompt)

	// The serve process must outlive the current node request so a successful
	// Loop iteration can reuse the same Agent in the next iteration. The node
	// context is still used for startup readiness and task HTTP requests; the
	// process itself is stopped explicitly by RuntimeManager.Stop.
	cmd := exec.CommandContext(ctx, bin, args...)
	proc.HideWindow(cmd)
	if spec.Workspace != "" {
		cmd.Dir = spec.Workspace
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	start := time.Now()
	err := cmd.Run()
	duration := time.Since(start).Milliseconds()

	exitCode := 0
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	finalText := strings.TrimSpace(stdout.String())
	if finalText == "" && sessionPath != "" {
		finalText = loadReasonixRunFinalText(sessionPath)
	}
	if finalText == "" {
		finalText = extractReasonixVisibleText(stderr.String())
	}
	if finalText == "" {
		finalText = strings.TrimSpace(stderr.String())
	}

	return &ExecResult{
		FinalText:  finalText,
		RawStdout:  stdout.String(),
		RawStderr:  fmt.Sprintf("cmd=%s %s\n%s", bin, strings.Join(args, " "), stderr.String()),
		ExitCode:   exitCode,
		DurationMs: duration,
	}, err
}

func newReasonixRunSessionPath(spec ExecSpec) (string, func(), error) {
	workspace := strings.TrimSpace(spec.Workspace)
	if workspace == "" {
		return "", func() {}, nil
	}
	sessionDir := filepath.Join(workspace, ".reasonix-orchestrator", "run-sessions")
	if err := os.MkdirAll(sessionDir, 0755); err != nil {
		return "", func() {}, err
	}
	path := filepath.Join(sessionDir, fmt.Sprintf("%s-%d.jsonl", sanitizeFilename(spec.NodeID), time.Now().UnixNano()))
	cleanup := func() {
		_ = os.Remove(path)
		_ = os.Remove(path + ".meta")
	}
	return path, cleanup, nil
}

func loadReasonixRunFinalText(sessionPath string) string {
	sess, err := agent.LoadSession(sessionPath)
	if err != nil {
		return ""
	}
	return extractAssistantFinalText(sess.Snapshot())
}

func extractAssistantFinalText(history []provider.Message) string {
	for i := len(history) - 1; i >= 0; i-- {
		msg := history[i]
		if msg.Role != provider.RoleAssistant {
			continue
		}
		if text := strings.TrimSpace(msg.Content); text != "" {
			return text
		}
		if text := strings.TrimSpace(msg.ReasoningContent); text != "" {
			return text
		}
	}
	return ""
}

func extractReasonixVisibleText(s string) string {
	if strings.TrimSpace(s) == "" {
		return ""
	}
	lines := strings.Split(strings.ReplaceAll(s, "\r\n", "\n"), "\n")
	visible := make([]string, 0, len(lines))
	for _, line := range lines {
		trimmed := strings.TrimSpace(stripANSIEscapeCodes(line))
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "warning:") || strings.HasPrefix(trimmed, "error:") {
			continue
		}
		if trimmed == "▎ thinking" || strings.EqualFold(trimmed, "thinking") {
			continue
		}
		if strings.HasPrefix(trimmed, "[") && strings.HasSuffix(trimmed, "]") {
			continue
		}
		if strings.HasPrefix(trimmed, "-> ") || strings.HasPrefix(trimmed, "⊘ ") || strings.HasPrefix(trimmed, "!") || strings.HasPrefix(trimmed, "·") {
			continue
		}
		if strings.Contains(trimmed, " tok ") {
			continue
		}
		visible = append(visible, trimmed)
	}
	return strings.TrimSpace(strings.Join(visible, "\n"))
}

func stripANSIEscapeCodes(s string) string {
	if s == "" {
		return ""
	}
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] != 0x1b {
			b.WriteByte(s[i])
			continue
		}
		i++
		if i >= len(s) {
			break
		}
		if s[i] != '[' {
			continue
		}
		for i+1 < len(s) {
			i++
			c := s[i]
			if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') {
				break
			}
		}
	}
	return b.String()
}

func executeMimoRun(ctx context.Context, spec ExecSpec, onLine func(line string)) (*ExecResult, error) {
	args, cleanup, err := buildMimoRunArgs(spec, "")
	if err != nil {
		return &ExecResult{ExitCode: -1}, err
	}
	defer cleanup()

	start := time.Now()
	stdoutText, stderrText, runErr := runMimoCommand(ctx, args, spec.Workspace, onLine)
	duration := time.Since(start).Milliseconds()

	exitCode := 0
	if runErr != nil {
		if exitErr, ok := runErr.(*exec.ExitError); ok {
			exitCode = exitErr.ExitCode()
		} else {
			exitCode = -1
		}
	}

	finalText, eventErr := parseMimoOutput(stdoutText, stderrText)
	if strings.TrimSpace(finalText) != "" && !isMimoHardFailure(runErr, stderrText) {
		runErr = nil
		eventErr = nil
	}
	if runErr == nil && eventErr != nil && strings.TrimSpace(finalText) == "" {
		runErr = eventErr
		exitCode = -1
	}

	return &ExecResult{
		FinalText:  finalText,
		RawStdout:  stdoutText,
		RawStderr:  formatMimoCommandDiagnostic(args, stderrText),
		ExitCode:   exitCode,
		DurationMs: duration,
	}, runErr
}

// buildMimoRunArgs keeps the full orchestration prompt out of the Windows
// command line. Mimo's --file option attaches the prompt as a local file while
// the positional message stays short enough for Windows CreateProcess.
func buildMimoRunArgs(spec ExecSpec, attachEndpoint string) ([]string, func(), error) {
	// Put the short positional message immediately after `run`. Mimo declares
	// both `message` and `--file` as array-valued arguments; placing a
	// positional message after `--file` makes yargs consume it as a second
	// attachment path ("File not found: <message>"). Keeping the message first
	// and the attachment last makes the argument boundary unambiguous.
	executorPrompt := buildExecutorPrompt(spec)
	promptSpec := spec
	promptSpec.Prompt = executorPrompt
	promptSpec.SkillContent = ""
	promptPath, promptMessage, cleanup, err := buildMimoPromptArtifacts(promptSpec)
	if err != nil {
		return nil, func() {}, err
	}

	args := []string{"run", promptMessage}
	if attachEndpoint != "" {
		args = append(args, "--attach", attachEndpoint)
	}
	args = append(args, "--format", "json")
	if spec.Agent != "" {
		args = append(args, "--agent", spec.Agent)
	}
	if spec.Workspace != "" {
		args = append(args, "--dir", spec.Workspace)
	}
	if mimoModelArg(spec.ModelRef) != "" {
		args = append(args, "--model", mimoModelArg(spec.ModelRef))
	}
	// 思考等级：mimo run 通过 --variant 指定（high/medium/low 等 provider 特定档位）。
	if spec.ReasoningEffort != "" {
		args = append(args, "--variant", spec.ReasoningEffort)
	}
	// Orchestrator nodes are non-interactive by definition. Never leave Mimo
	// waiting for a human permission prompt: that is the source of the
	// external_directory auto-reject loop seen in Reviewer runs.
	if spec.NeverAsk || spec.ApprovalMode == "auto" || spec.ApprovalMode == "yolo" {
		args = append(args, "--dangerously-skip-permissions")
	}
	// Keep --file as the final option/value pair. There must be no positional
	// arguments after it because Mimo's array parser treats them as files.
	if promptPath != "" {
		args = append(args, "--file", promptPath)
	}
	return args, cleanup, nil
}

func formatMimoCommandDiagnostic(args []string, stderr string) string {
	command := "cmd=mimo " + strings.Join(args, " ")
	const maxDiagnosticCommand = 2048
	if len(command) > maxDiagnosticCommand {
		command = command[:maxDiagnosticCommand] + "… [command truncated]"
	}
	if strings.TrimSpace(stderr) == "" {
		return command
	}
	return command + "\n" + stderr
}

func sanitizeFilename(s string) string {
	if s == "" {
		return "node"
	}
	repl := strings.NewReplacer("\\", "-", "/", "-", ":", "-", "*", "-", "?", "-", "\"", "-", "<", "-", ">", "-", "|", "-")
	return repl.Replace(s)
}

// executors is the registry of available executors, protected by executorsMu.
var (
	executors = map[ExecutorType]PipelineExecutor{
		ExecutorReasonix: &ReasonixExecutor{},
		ExecutorMimo:     &MimoExecutor{},
		ExecutorCodex:    &CodexPipelineExecutor{},
		ExecutorClaude:   &ClaudePipelineExecutor{},
		ExecutorOpencode: &OpenCodePipelineExecutor{},
		ExecutorDsh:      &DshPipelineExecutor{},
	}
	executorsMu sync.Mutex
)

func getExecutor(key ExecutorType) PipelineExecutor {
	executorsMu.Lock()
	defer executorsMu.Unlock()
	return executors[key]
}

// GetExecutor returns the registered executor for a key, or nil when unknown.
func GetExecutor(key ExecutorType) PipelineExecutor {
	return getExecutor(key)
}

// GetExecutorForTest is kept for existing test callers.
func GetExecutorForTest(key ExecutorType) PipelineExecutor {
	return GetExecutor(key)
}

func SetExecutorForTest(key ExecutorType, exec PipelineExecutor) {
	executorsMu.Lock()
	defer executorsMu.Unlock()
	executors[key] = exec
}

// Store manages pipelines and runs in memory.
type Store struct {
	mu        sync.RWMutex
	pipelines map[string]*Pipeline
	runs      map[string]*PipelineRun
	sessions  map[string]*Session
	agents    map[string]*AgentInstance
	nextID    int
	emitter   event.Sink // optional: emits pipeline events to SSE stream

	// P0: Orchestration session data
	orchSessions      map[string]*OrchestrationSession
	pipelineRevisions map[string]*PipelineRevision
	attempts          map[string]*NodeAttempt
	bindings          map[string]*AgentBinding
	providerSessions  map[string]*ProviderSession
	runtimeStates     map[string]*RuntimeState
	iterations        map[string]*LoopIteration

	// orchAddr 是编排服务对外地址（host:port），注入辅助手委派协议（执行者
	// curl 目标）。用 atomic.Value 承载，因为 assistHint 可能在被调用方持有
	// s.mu 时读取（gatherInput 上游分支），不能再加锁。由 serve 启动时经
	// SetOrchestratorAddr 设置。
	orchAddr atomic.Value // string
}

// NewStore creates an empty pipeline store.
func NewStore() *Store {
	return &Store{
		pipelines:         make(map[string]*Pipeline),
		runs:              make(map[string]*PipelineRun),
		sessions:          make(map[string]*Session),
		agents:            make(map[string]*AgentInstance),
		orchSessions:      make(map[string]*OrchestrationSession),
		pipelineRevisions: make(map[string]*PipelineRevision),
		attempts:          make(map[string]*NodeAttempt),
		bindings:          make(map[string]*AgentBinding),
		providerSessions:  make(map[string]*ProviderSession),
		runtimeStates:     make(map[string]*RuntimeState),
		iterations:        make(map[string]*LoopIteration),
		nextID:            1,
	}
}

// SetEmitter sets the event emitter for pipeline progress events.
func (s *Store) SetEmitter(em event.Sink) {
	s.emitter = em
	mirrorRuntimeState := func(runtime RuntimeState) {
		// Runtime manager state is process-local. Mirror its lifecycle fields into
		// the persisted session record when it exists, then use that record for
		// SSE so the frontend can still resolve the owning node and session.
		_ = s.UpdateRuntimeState(runtime.RuntimeID, func(persisted *RuntimeState) {
			persisted.Endpoint = runtime.Endpoint
			persisted.Port = runtime.Port
			persisted.PID = runtime.PID
			persisted.Status = runtime.Status
			persisted.Error = runtime.Error
			persisted.ThreadID = runtime.ThreadID
			persisted.TurnID = runtime.TurnID
			if runtime.AccessMode != "" {
				persisted.AccessMode = runtime.AccessMode
			}
		})
		if persisted, ok := s.GetRuntimeState(runtime.RuntimeID); ok {
			runtime = persisted
		}
		detail, err := json.Marshal(runtime)
		if err != nil {
			return
		}
		s.emit(event.Event{Kind: event.PipelineNodeRuntime, Text: runtime.RuntimeID, Detail: string(detail)})
	}
	codexRuntimeMgr.SetUpdateSink(mirrorRuntimeState)
	mimoRuntimeMgr.SetUpdateSink(mirrorRuntimeState)
	opencodeRuntimeMgr.SetUpdateSink(mirrorRuntimeState)
}

// emit sends an event if an emitter is configured; otherwise no-op.
func (s *Store) emit(e event.Event) {
	if s.emitter != nil {
		s.emitter.Emit(e)
	}
}

// nextPipeID returns a unique pipeline ID.
func (s *Store) nextPipeID() string {
	s.nextID++
	return fmt.Sprintf("pipe_%d_%d", time.Now().UnixMilli(), s.nextID)
}

// nextRunID returns a unique run ID.
func (s *Store) nextRunID() string {
	s.nextID++
	return fmt.Sprintf("run_%d_%d", time.Now().UnixMilli(), s.nextID)
}

// ListPipelines returns all saved pipelines.
func (s *Store) ListPipelines() []Pipeline {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Pipeline, 0, len(s.pipelines))
	for _, p := range s.pipelines {
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt > out[j].UpdatedAt
	})
	return out
}

// GetPipeline returns a pipeline by ID.
func (s *Store) GetPipeline(id string) (*Pipeline, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.pipelines[id]
	if !ok {
		return nil, false
	}
	return p, true
}

// SavePipeline creates a new pipeline.
func (s *Store) SavePipeline(payload PipelinePayload) (*Pipeline, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339)
	id := s.nextPipeID()

	pipe := &Pipeline{
		ID:        id,
		Name:      payload.Name,
		Nodes:     payload.Nodes,
		Edges:     payload.Edges,
		CreatedAt: now,
		UpdatedAt: now,
	}
	if pipe.Name == "" {
		pipe.Name = "Unnamed Pipeline"
	}
	s.pipelines[id] = pipe
	return pipe, nil
}

// UpdatePipeline replaces a pipeline by ID.
func (s *Store) UpdatePipeline(id string, payload PipelinePayload) (*Pipeline, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.pipelines[id]
	if !ok {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	existing.Name = payload.Name
	existing.Nodes = payload.Nodes
	existing.Edges = payload.Edges
	existing.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	return existing, nil
}

// DeletePipeline removes a pipeline.
func (s *Store) DeletePipeline(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.pipelines[id]; !ok {
		return fmt.Errorf("pipeline %q not found", id)
	}
	delete(s.pipelines, id)
	return nil
}

// ExecutePipeline starts a pipeline run with a deep copy of the pipeline to
// prevent data races during async execution. task is the user's initial prompt.
// This is the legacy path for old-style pipelines (not OrchestrationSession-based).
func (s *Store) ExecutePipeline(ctx context.Context, id string, task string) (*PipelineRun, error) {
	s.mu.Lock()
	pipe, ok := s.pipelines[id]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	// Deep copy to avoid data race with async goroutine (C-2 fix).
	pipeCopy := clonePipeline(pipe)
	now := time.Now().UTC().Format(time.RFC3339)
	runID := s.nextRunID()
	nodeStates := make(map[string]RunState)
	for _, n := range pipeCopy.Nodes {
		nodeStates[n.ID] = RunState{
			Status:     NodePending,
			TokenUsage: TokenUsage{},
		}
	}

	// Find or create session for this task.
	session := s.findOrCreateSession(task, now)

	run := &PipelineRun{
		ID:         runID,
		PipelineID: id,
		SessionID:  session.ID,
		Task:       task,
		Status:     "running",
		NodeStates: nodeStates,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.runs[runID] = run
	session.RunIDs = append(session.RunIDs, runID)
	session.UpdatedAt = now
	s.mu.Unlock()

	// Create a cancellable context for this run
	ctx, cancel := context.WithCancel(ctx)
	run.Cancel = cancel

	go s.executePipeline(ctx, run, pipeCopy)
	return run, nil
}

// ExecutePipelineV2 starts a pipeline run using the OrchestrationSession model.
// It creates a PipelineRun, NodeAttempts, AgentBindings, and ProviderSessions.
func (s *Store) ExecutePipelineV2(ctx context.Context, sessionID, pipelineRevID, task, rewrittenTask string, opts ExecutionOptions) (*PipelineRun, error) {
	// Resume is a continuation of the existing run, never a new Run record.
	if opts.ResumeRunID != "" {
		return s.resumePipelineV2(ctx, sessionID, opts.ResumeRunID)
	}

	// Create the cancel function before publishing the run in the store. This
	// closes the race where a fast Cancel API call observed a run whose Cancel
	// field had not been assigned yet.
	runCtx, cancel := context.WithCancel(ctx)

	s.mu.Lock()

	sess, ok := s.orchSessions[sessionID]
	if !ok {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	rev, ok := s.pipelineRevisions[pipelineRevID]
	if !ok {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("pipeline revision %q not found", pipelineRevID)
	}
	if rev.SessionID != sessionID {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("pipeline revision %q does not belong to session %q", pipelineRevID, sessionID)
	}

	// Validate context policy before creating run
	if err := validateContextPolicy(opts.ContextPolicy); err != nil {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("invalid context policy: %w", err)
	}

	// Snapshot the session workspace into the Run. The request may explicitly
	// provide one (for API callers), otherwise the session setting is the
	// authoritative source. If both are empty, bind an existing absolute
	// project path mentioned by the task before falling back to the process
	// workspace. This prevents the Reviewer from silently running in the
	// console checkout while the task targets another project.
	opts.Workspace = resolveRunWorkspaceForTask(opts.Workspace, sess.Workspace, task)
	if strings.TrimSpace(sess.Workspace) == "" && strings.TrimSpace(opts.Workspace) != "" && opts.Workspace != detectWorkspace() {
		sess.Workspace = opts.Workspace
	}

	// Deep copy the pipeline revision's nodes/edges for async execution.
	pipeCopy := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: make([]AgentNode, len(rev.Nodes)),
		Edges: make([]Edge, len(rev.Edges)),
	}
	copy(pipeCopy.Nodes, rev.Nodes)
	copy(pipeCopy.Edges, rev.Edges)

	// Create the run via CreateRun (which handles persistence).
	now := time.Now().UTC().Format(time.RFC3339)
	runID := fmt.Sprintf("run_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	nodeStates := make(map[string]RunState)
	for _, n := range pipeCopy.Nodes {
		nodeStates[n.ID] = RunState{Status: NodePending, TokenUsage: TokenUsage{}}
	}

	loopConfig := rev.LoopConfig
	if normalized, err := NormalizeLoopConfig(&loopConfig); err == nil {
		loopConfig = normalized
	}

	run := &PipelineRun{
		ID:                 runID,
		PipelineID:         pipelineRevID,
		PipelineRevisionID: pipelineRevID,
		SessionID:          sessionID,
		Task:               task,
		RewrittenTask:      rewrittenTask,
		Status:             "running",
		Trigger:            opts.Trigger,
		ParentRunID:        opts.ParentRunID,
		ExecOptions:        opts,
		LoopConfig:         loopConfig,
		NodeStates:         nodeStates,
		NodeAttemptIDs:     []string{},
		Images:             append([]ImageRef(nil), opts.Images...),
		CreatedAt:          now,
		StartedAt:          now,
		UpdatedAt:          now,
		Cancel:             cancel,
	}

	s.runs[runID] = run
	sess.RunIDs = append(sess.RunIDs, runID)
	sess.CurrentRunID = runID
	if task != "" {
		sess.ActiveTask = task
	}
	if rewrittenTask != "" {
		sess.RewrittenTask = rewrittenTask
	}
	sess.UpdatedAt = now

	// Persist run
	runDir := filepath.Join(sessionDir(sessionID), "runs")
	if err := saveSessionJSON(runDir, runID+".json", run); err != nil {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("persist run: %w", err)
	}
	if err := saveSessionJSON(sessionDir(sessionID), "session.json", sess); err != nil {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("persist session: %w", err)
	}
	if err := s.saveIndex(); err != nil {
		s.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("persist index: %w", err)
	}

	s.mu.Unlock()

	// Node-level retry: seed the new run with the source run's completed
	// upstream outputs BEFORE execution starts, so seeded nodes are skipped
	// and downstream inputs come from the seeded attempts.
	if opts.RetryFromNodeID != "" && opts.SeedSourceRunID != "" {
		if src, ok := s.runs[opts.SeedSourceRunID]; ok {
			if err := s.SeedRetryRun(run, src, pipeCopy, opts.RetryFromNodeID); err != nil {
				cancel()
				return nil, fmt.Errorf("seed retry run: %w", err)
			}
			runDir := filepath.Join(sessionDir(sessionID), "runs")
			if err := saveSessionJSON(runDir, runID+".json", run); err != nil {
				cancel()
				return nil, fmt.Errorf("persist seeded run: %w", err)
			}
		}
	}

	go s.ExecuteLoop(runCtx, run, pipeCopy, sessionID)
	return run, nil
}

// resumePipelineV2 resumes an interrupted Loop run using the run's persisted
// revision, ExecOptions, and LoopConfig. Request options are deliberately not
// allowed to replace the historical run configuration.
func (s *Store) resumePipelineV2(ctx context.Context, sessionID, runID string) (*PipelineRun, error) {
	s.mu.Lock()
	run, ok := s.runs[runID]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("run %q not found", runID)
	}
	if run.SessionID != sessionID {
		s.mu.Unlock()
		return nil, fmt.Errorf("run %q does not belong to session %q", runID, sessionID)
	}
	if run.Status != "interrupted" {
		s.mu.Unlock()
		return nil, fmt.Errorf("cannot resume run %s: status is %q, not interrupted", runID, run.Status)
	}
	if !run.LoopConfig.Enabled || (run.LoopConfig.Mode != "review_decides" && run.LoopConfig.Mode != "fixed") {
		s.mu.Unlock()
		return nil, fmt.Errorf("run %q is not a Loop run", runID)
	}
	rev, ok := s.pipelineRevisions[run.PipelineRevisionID]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("pipeline revision %q not found for run %q", run.PipelineRevisionID, runID)
	}
	if rev.SessionID != sessionID {
		s.mu.Unlock()
		return nil, fmt.Errorf("pipeline revision %q does not belong to session %q", rev.ID, sessionID)
	}
	sess := s.orchSessions[sessionID]
	if sess == nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	// Preserve every historical execution option except the trigger, which is
	// the explicit signal consumed by the UI and audit trail for a resume.
	execOpts := run.ExecOptions
	execOpts.Trigger = "resume"
	run.ExecOptions = execOpts
	run.Trigger = "resume"
	run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	sess.CurrentRunID = run.ID
	sess.UpdatedAt = run.UpdatedAt
	ctx, cancel := context.WithCancel(ctx)
	run.Cancel = cancel
	s.mu.Unlock()

	if err := s.persistRun(run, sessionID); err != nil {
		cancel()
		return nil, fmt.Errorf("persist resume run: %w", err)
	}
	if err := saveSessionJSON(sessionDir(sessionID), "session.json", sess); err != nil {
		cancel()
		return nil, fmt.Errorf("persist resumed session: %w", err)
	}
	go func() {
		if err := s.ResumeLoop(ctx, run, rev, sessionID); err != nil {
			// ResumeLoop has already persisted terminal loop states. If it fails
			// before reaching one, retain a useful failure state for the UI.
			s.mu.Lock()
			if run.Status == "running" {
				run.Status = "failed"
				run.Error = err.Error()
				run.FinishedAt = time.Now().UTC().Format(time.RFC3339)
				run.UpdatedAt = run.FinishedAt
			}
			s.mu.Unlock()
			_ = s.persistRun(run, sessionID)
		}
	}()
	return run, nil
}

// findOrCreateSession finds an existing session for the task or creates a new one.
func (s *Store) findOrCreateSession(task string, now string) *Session {
	// Look for an existing session with the same task.
	for _, sess := range s.sessions {
		if sess.Task == task {
			return sess
		}
	}
	// Create new session.
	sess := &Session{
		ID:        fmt.Sprintf("sess_%d_%d", time.Now().UnixMilli(), s.nextID),
		Task:      task,
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.nextID++
	s.sessions[sess.ID] = sess
	return sess
}

// executePipeline runs the DAG topologically.
func (s *Store) executePipeline(ctx context.Context, run *PipelineRun, pipe *Pipeline) {
	// Panic recovery (S-3 fix).
	defer func() {
		if r := recover(); r != nil {
			s.mu.Lock()
			run.Status = "failed"
			run.Error = fmt.Sprintf("panic: %v", r)
			run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			s.mu.Unlock()
		}
	}()
	defer func() {
		s.mu.Lock()
		run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		if run.Status != "canceled" && run.Status != "failed" {
			run.Status = "complete"
		}
		// Keep only lightweight session metadata here. Aggregated session stats are
		// derived from run records on read so reloads and legacy data do not
		// double-count stale estimates.
		if sess, ok := s.sessions[run.SessionID]; ok {
			sess.UpdatedAt = run.UpdatedAt
		}
		s.mu.Unlock()
		// Release all runtimes (mark as idle, don't kill processes)
		for _, rt := range mimoRuntimeMgr.List() {
			_ = mimoRuntimeMgr.Release(rt.RuntimeID)
		}
		for _, rt := range reasonixRuntimeMgr.List() {
			_ = reasonixRuntimeMgr.Release(rt.RuntimeID)
		}
		// Notify frontend: retained runtimes are now idle (not stopped!)
		// "stopped" means the process was killed. "idle" means alive but not busy.
		for _, rt := range mimoRuntimeMgr.List() {
			s.emit(event.Event{
				Kind:   event.PipelineNodeRuntime,
				Text:   rt.RuntimeID,
				Detail: fmt.Sprintf(`{"endpoint":"%s","port":%d,"status":"idle","nodeID":"%s"}`, rt.Endpoint, rt.Port, rt.NodeID),
			})
		}
		for _, rt := range reasonixRuntimeMgr.List() {
			s.emit(event.Event{
				Kind:   event.PipelineNodeRuntime,
				Text:   rt.RuntimeID,
				Detail: fmt.Sprintf(`{"endpoint":"%s","port":%d,"status":"idle","nodeID":"%s"}`, rt.Endpoint, rt.Port, rt.NodeID),
			})
		}
	}()

	// Topological sort into parallel levels using Kahn's algorithm.
	levels := topologicalLevels(pipe)
	if levels == nil {
		s.mu.Lock()
		run.Status = "failed"
		run.Error = "pipeline contains a cycle"
		s.mu.Unlock()
		return
	}

	// Execute each level concurrently; wait for all nodes in a level before proceeding.
	for _, level := range levels {
		s.mu.Lock()
		if run.Status == "canceled" || run.Status == "failed" {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		var wg sync.WaitGroup
		for _, nodeID := range level {
			nodeID := nodeID
			wg.Add(1)
			go func() {
				defer wg.Done()
				s.mu.Lock()
				if run.Status == "canceled" || run.Status == "failed" {
					s.mu.Unlock()
					return
				}
				node := findNode(pipe, nodeID)
				if node == nil {
					s.mu.Unlock()
					return
				}
				state := run.NodeStates[nodeID]
				state.Status = NodeRunning
				state.StartedAt = time.Now().UTC().Format(time.RFC3339)
				run.NodeStates[nodeID] = state
				s.mu.Unlock()

				// Gather input from upstream edges.
				input := s.gatherInput(pipe, run, nodeID)
				// Auto vision: 注入附件路径，无视觉模型自动委派辅助手识图。
				input = s.autoVisionInject(ctx, run, node, input)

				// Execute the agent via reasonix serve subprocess.
				output, nodeStderr, realUsage, _, _, _, err := s.executeNode(ctx, node, input, "", "")

				s.mu.Lock()
				state.DoneAt = time.Now().UTC().Format(time.RFC3339)
				state.Stderr = nodeStderr
				if err != nil {
					state.Status = NodeFailed
					state.Error = err.Error()
					if ctx.Err() == context.Canceled {
						run.Status = "canceled"
					} else {
						run.Status = "failed"
					}
					run.Error = fmt.Sprintf("node %s failed: %s", node.Label, err)
				} else {
					state.Status = NodeComplete
					state.Output = output
					state.TokenUsage = TokenUsage{DurationMs: time.Since(mustParseTime(state.StartedAt)).Milliseconds()}
					if realUsage != nil {
						state.TokenUsage = *realUsage
						state.TokenUsage.DurationMs = time.Since(mustParseTime(state.StartedAt)).Milliseconds()
					}
				}
				run.NodeStates[nodeID] = state
				run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
				s.mu.Unlock()
			}()
		}
		wg.Wait()
	}

	return
}

// gatherInput collects output from upstream nodes.
// assistHint returns the auxiliary helper-agent section for a node when its
// Assist config is enabled. The hint is an explicit delegation protocol: the
// node curls the Orchestrator's assist dispatch endpoint instead of inventing
// its own sub-agents; the endpoint runs a vision-capable model on a retained
// runtime. The hint always says to keep going when the command is unavailable,
// so a pipeline without a configured orchestrator address never blocks.
func (s *Store) assistHint(node *AgentNode) string {
	if node == nil || node.Assist == nil || !node.Assist.Enabled {
		return ""
	}
	duty := strings.TrimSpace(node.Assist.Role)
	if duty == "" {
		duty = "识图：描述截图/设计稿/报错图等图像内容，或完成交给你的独立小任务"
	}
	model := strings.TrimSpace(node.Assist.Model)
	if model == "" {
		model = "mimo-v2.5"
	}
	driver := strings.TrimSpace(node.Assist.Driver)
	if driver == "" {
		driver = "opencode"
	}
	addr, _ := s.orchAddr.Load().(string)

	base := "## 辅助手（Helper Agent）— 委派协议\n" +
		"当前模型很可能无视觉能力：read_image 等图像工具必然失败，禁止尝试直接读图，禁止编造图像内容。\n" +
		"若任务文本中已含「辅助手自动识图结果」区块：直接使用其结果，无需再委派。\n"
	if strings.TrimSpace(addr) == "" {
		return base + "当前编排服务地址未配置，你无法委派识图：遇到需要识图的任务时，如实声明「无法识图（辅助手端点未配置）」并继续其余工作，禁止编造图像内容。"
	}
	return base +
		"需要识图（截图/设计稿/报错图）或适合交给视觉辅助 agent 的独立小任务时，运行以下命令委派（辅助手=" + driver + "/" + model + "，职责：" + duty + "）：\n" +
		"curl -s -m 150 -X POST http://" + addr + "/orchestrator/api/orch-assist/dispatch -H \"Content-Type: application/json\" -d '{\"task\":\"<识图问题>\",\"images\":[\"<图片绝对路径>\"]}'\n" +
		"返回 JSON ok=true → 把 result 纳入交付物；ok=false、超时或无法执行 curl → 声明「无法识图:原因」并继续其余工作，禁止编造。\n" +
		"图片绝对路径见任务中「Orchestrator 图片定位」清单；无法确定路径则不委派，如实声明。"
}

func (s *Store) gatherInput(pipe *Pipeline, run *PipelineRun, nodeID string) string {
	node := findNode(pipe, nodeID)
	upstream := upstreamEdges(pipe, nodeID)
	if len(upstream) == 0 {
		if run.Task != "" {
			if hint := s.assistHint(node); hint != "" {
				if node != nil && node.RoleDesc != "" {
					return fmt.Sprintf("## 节点职责 / Node duty\n%s\n\n## 原始任务 / Original task\n%s\n\n%s", node.RoleDesc, run.Task, hint)
				}
				return fmt.Sprintf("%s\n\n%s", run.Task, hint)
			}
			if node != nil && node.RoleDesc != "" {
				return fmt.Sprintf("## 节点职责 / Node duty\n%s\n\n## 原始任务 / Original task\n%s", node.RoleDesc, run.Task)
			}
			return run.Task
		}
		if node != nil && node.RoleDesc != "" {
			if hint := s.assistHint(node); hint != "" {
				return fmt.Sprintf("你是一个%s / You are a(n) %s。你的任务是：%s。请开始工作 / Begin.\n\n%s", node.Label, node.Label, node.RoleDesc, hint)
			}
			return fmt.Sprintf("你是一个%s / You are a(n) %s。你的任务是：%s。请开始工作 / Begin.", node.Label, node.Label, node.RoleDesc)
		}
		if hint := s.assistHint(node); hint != "" {
			return fmt.Sprintf("请完成你的角色任务 / Complete your role task。角色 / Role：%s\n\n%s", node.Label, hint)
		}
		return fmt.Sprintf("请完成你的角色任务 / Complete your role task。角色 / Role：%s", node.Label)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var parts []string
	if node != nil && node.RoleDesc != "" {
		parts = append(parts, "## 节点职责 / Node duty\n"+node.RoleDesc)
	}
	if hint := s.assistHint(node); hint != "" {
		parts = append(parts, hint)
	}
	if run.Task != "" {
		parts = append(parts, "## 原始任务 / Original task\n"+run.Task)
	}
	parts = append(parts, "## 上游节点输出 / Upstream output")
	for _, e := range upstream {
		st, ok := run.NodeStates[e.FromID]
		if ok && st.Output != "" {
			label := e.FromID
			if fromNode := findNode(pipe, e.FromID); fromNode != nil && fromNode.Label != "" {
				label = fromNode.Label
			}
			parts = append(parts, fmt.Sprintf("### %s\n%s", label, st.Output))
		}
	}
	return strings.Join(parts, "\n\n---\n\n")
}

// executePipelineV2 runs the DAG using the OrchestrationSession model.
// It creates NodeAttempts for each node, uses AgentBinding, and reads upstream
// output from Attempts in the current Run.
func (s *Store) executePipelineV2(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID string) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			s.mu.Lock()
			run.Status = "failed"
			run.Error = fmt.Sprintf("panic: %v", r)
			run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			run.FinishedAt = run.UpdatedAt
			s.mu.Unlock()
		}
	}()
	defer func() {
		// Phase 1: Update run state and collect data under lock, then deep copy.
		s.mu.Lock()
		run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		// Only transition from "running" to "complete" — never overwrite a terminal status
		// set by a goroutine (failed/canceled).
		if run.Status == "running" {
			run.Status = "complete"
			run.FinishedAt = run.UpdatedAt
		} else if run.FinishedAt == "" {
			run.FinishedAt = run.UpdatedAt
		}
		// Deep copy run for persistence (release lock before I/O)
		runCopy := *run
		if run.NodeStates != nil {
			runCopy.NodeStates = make(map[string]RunState, len(run.NodeStates))
			for k, v := range run.NodeStates {
				runCopy.NodeStates[k] = v
			}
		}
		if run.NodeAttemptIDs != nil {
			runCopy.NodeAttemptIDs = make([]string, len(run.NodeAttemptIDs))
			copy(runCopy.NodeAttemptIDs, run.NodeAttemptIDs)
		}
		if run.IterationIDs != nil {
			runCopy.IterationIDs = make([]string, len(run.IterationIDs))
			copy(runCopy.IterationIDs, run.IterationIDs)
		}
		if run.FinalReview != nil {
			frCopy := *run.FinalReview
			runCopy.FinalReview = &frCopy
		}
		// Deep copy session
		var sessCopy *OrchestrationSession
		if sess, ok := s.orchSessions[sessionID]; ok {
			sessCopy = &OrchestrationSession{}
			*sessCopy = *sess
			sessCopy.UpdatedAt = run.UpdatedAt
		}

		// Collect runtime IDs from attempts under lock.
		type rtInfo struct {
			runtimeID string
			endpoint  string
			port      int
			nodeID    string
		}
		var runtimesToRelease []rtInfo
		seen := make(map[string]bool)
		for _, attID := range runCopy.NodeAttemptIDs {
			if att, ok := s.attempts[attID]; ok && att.RuntimeID != "" && !seen[att.RuntimeID] {
				seen[att.RuntimeID] = true
				if rt, ok := s.runtimeStates[att.RuntimeID]; ok {
					runtimesToRelease = append(runtimesToRelease, rtInfo{
						runtimeID: rt.RuntimeID,
						endpoint:  rt.Endpoint,
						port:      rt.Port,
						nodeID:    rt.NodeID,
					})
				} else {
					runtimesToRelease = append(runtimesToRelease, rtInfo{
						runtimeID: att.RuntimeID,
						nodeID:    att.NodeID,
					})
				}
			}
		}

		// Persist index under lock (fast: JSON marshal + file write).
		// This prevents concurrent goroutines from mutating s.orchSessions during serialization.
		if err := s.saveIndex(); err != nil {
			run.Error = fmt.Sprintf("index persistence failure: %v", err)
		}
		s.mu.Unlock()

		// Phase 1b: Persist run and session OUTSIDE lock (using deep copies).
		runDir := filepath.Join(sessionDir(sessionID), "runs")
		if err := saveSessionJSON(runDir, runCopy.ID+".json", &runCopy); err != nil {
			s.mu.Lock()
			run.Error = fmt.Sprintf("persistence failure: %v", err)
			s.mu.Unlock()
		}
		if sessCopy != nil {
			if err := saveSessionJSON(sessionDir(sessionID), "session.json", sessCopy); err != nil {
				s.mu.Lock()
				run.Error = fmt.Sprintf("session persistence failure: %v", err)
				s.mu.Unlock()
			}
		}

		// Phase 2: Release runtimes and update states (no lock held).
		for _, ri := range runtimesToRelease {
			if _, ok := mimoRuntimeMgr.Get(ri.runtimeID); ok {
				_ = mimoRuntimeMgr.Release(ri.runtimeID)
			}
			if _, ok := reasonixRuntimeMgr.Get(ri.runtimeID); ok {
				_ = reasonixRuntimeMgr.Release(ri.runtimeID)
			}
			if _, ok := codexRuntimeMgr.Get(ri.runtimeID); ok {
				_ = codexRuntimeMgr.Release(ri.runtimeID)
			}
			_ = s.UpdateRuntimeState(ri.runtimeID, func(rt *RuntimeState) {
				rt.Status = RuntimeIdle
			})
			s.emit(event.Event{
				Kind:   event.PipelineNodeRuntime,
				Text:   ri.runtimeID,
				Detail: fmt.Sprintf(`{"endpoint":"%s","port":%d,"status":"idle","nodeID":"%s"}`, ri.endpoint, ri.port, ri.nodeID),
			})
		}

		// Return error if pipeline failed or was canceled — eliminates
		// the need for the caller to read run.Status after the call.
		s.mu.RLock()
		finalStatus, finalError := run.Status, run.Error
		s.mu.RUnlock()
		if finalStatus == "failed" || finalStatus == "canceled" {
			retErr = fmt.Errorf("pipeline execution: %s", finalError)
		}
	}()

	// Topological sort into parallel levels.
	levels := topologicalLevels(pipe)
	if levels == nil {
		s.mu.Lock()
		run.Status = "failed"
		run.Error = "pipeline contains a cycle"
		run.FinishedAt = time.Now().UTC().Format(time.RFC3339)
		run.UpdatedAt = run.FinishedAt
		s.mu.Unlock()
		return
	}

	// Execute each level concurrently.
	for _, level := range levels {
		s.mu.Lock()
		if run.Status == "canceled" || run.Status == "failed" {
			s.mu.Unlock()
			return
		}
		s.mu.Unlock()

		var wg sync.WaitGroup
		for _, nodeID := range level {
			nodeID := nodeID
			wg.Add(1)
			go func() {
				defer wg.Done()

				// Phase 1: Check run status and copy node under lock.
				s.mu.Lock()
				if run.Status == "canceled" || run.Status == "failed" {
					s.mu.Unlock()
					return
				}
				node := findNode(pipe, nodeID)
				if node == nil {
					s.mu.Unlock()
					return
				}
				// Node-level retry seed: the output was copied from a source
				// run — mark complete without executing (zero tokens).
				if run.SeededNodes[nodeID] {
					delete(run.SeededNodes, nodeID)
					state := run.NodeStates[nodeID]
					if state.Status != NodeComplete {
						state.Status = NodeComplete
						run.NodeStates[nodeID] = state
					}
					run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
					s.mu.Unlock()
					return
				}
				nodeCopy := *node // deep copy — safe to use after unlock
				state := run.NodeStates[nodeID]
				state.Status = NodeRunning
				state.StartedAt = time.Now().UTC().Format(time.RFC3339)
				run.NodeStates[nodeID] = state
				run.CurrentNodeID = nodeID
				s.mu.Unlock()

				// Phase 2: Store CRUD operations — NO lock held.
				// Helper to mark node as failed and persist. Returns error if persistence fails.
				failNode := func(err error) error {
					now := time.Now().UTC().Format(time.RFC3339)
					s.mu.Lock()
					st := run.NodeStates[nodeID]
					st.Status = NodeFailed
					st.Error = err.Error()
					st.DoneAt = now
					run.NodeStates[nodeID] = st
					run.Status = "failed"
					run.Error = fmt.Sprintf("node %s failed: %s", nodeCopy.Label, err)
					run.FinishedAt = now
					run.UpdatedAt = now
					// Deep copy for persistence (save outside lock without race)
					runCopy := *run
					if run.NodeStates != nil {
						runCopy.NodeStates = make(map[string]RunState, len(run.NodeStates))
						for k, v := range run.NodeStates {
							runCopy.NodeStates[k] = v
						}
					}
					if run.NodeAttemptIDs != nil {
						runCopy.NodeAttemptIDs = make([]string, len(run.NodeAttemptIDs))
						copy(runCopy.NodeAttemptIDs, run.NodeAttemptIDs)
					}
					s.mu.Unlock()
					runDir := filepath.Join(sessionDir(sessionID), "runs")
					if perr := saveSessionJSON(runDir, run.ID+".json", &runCopy); perr != nil {
						return fmt.Errorf("node %q failed: %w; persist failed: %v", nodeCopy.Label, err, perr)
					}
					return nil
				}

				// Atomically find/create binding and ProviderSession (prevents concurrent race)
				contextPolicy := run.ExecOptions.ContextPolicy
				binding, ps, psErr := s.FindOrCreateBindingAndProviderSession(sessionID, nodeID, nodeCopy, string(nodeCopy.Executor), runWorkspace(run), contextPolicy, run.ExecOptions.ReuseAgentSessions)
				if psErr != nil {
					failNode(fmt.Errorf("binding/provider session creation failed: %w", psErr))
					return
				}
				providerSessionID := ps.ID

				attempt, err := s.CreateAttempt(run.ID, nodeID, binding.ID)
				if err != nil {
					failNode(fmt.Errorf("attempt creation failed: %w", err))
					return
				}

				if err := s.UpdateAttempt(attempt.ID, func(a *NodeAttempt) {
					a.Executor = string(nodeCopy.Executor)
					a.Model = nodeCopy.Model
					a.Mode = nodeCopy.Mode
					a.Agent = nodeCopy.Agent
					a.Skill = nodeCopy.Skill
					a.ProviderSessionID = providerSessionID
				}); err != nil {
					failNode(fmt.Errorf("attempt metadata update: %w", err))
					return
				}

				// Gather input from upstream Attempts.
				input := s.gatherInputV2(pipe, run, nodeID)
				// Auto vision: 注入附件路径，无视觉模型自动委派辅助手识图。
				input = s.autoVisionInject(ctx, run, &nodeCopy, input)

				if err := s.UpdateAttempt(attempt.ID, func(a *NodeAttempt) {
					a.Input = input
				}); err != nil {
					failNode(fmt.Errorf("attempt input update: %w", err))
					return
				}

				// Look up ProviderSession for context policy and external session ID
				var externalSessionID string
				if providerSessionID != "" {
					if ps, ok := s.GetProviderSession(providerSessionID); ok {
						externalSessionID = ps.ExternalSessionID
					}
				}

				// Phase 3: Execute the node.
				output, nodeStderr, realUsage, nodeRuntimeID, nodeEndpoint, execExternalSessionID, execErr := s.executeNodeAtWorkspace(ctx, &nodeCopy, input, contextPolicy, externalSessionID, runWorkspace(run))

				// Phase 4: Update attempt with results.
				doneAt := time.Now().UTC().Format(time.RFC3339)
				if err := s.UpdateAttempt(attempt.ID, func(a *NodeAttempt) {
					a.Output = output
					a.Stderr = nodeStderr
					a.FinishedAt = doneAt
					if nodeRuntimeID != "" {
						a.RuntimeID = nodeRuntimeID
					}
					if realUsage != nil {
						a.TokenUsage = *realUsage
						a.TokenUsage.DurationMs = time.Since(mustParseTime(a.StartedAt)).Milliseconds()
					}
					if execErr != nil {
						a.Status = "failed"
						a.Error = execErr.Error()
					} else if strings.TrimSpace(output) == "" {
						a.Status = "failed"
						a.Error = "agent completed without assistant output"
					} else {
						a.Status = "complete"
					}
				}); err != nil {
					failNode(fmt.Errorf("attempt result update: %w", err))
					return
				}

				// Phase 4b: Write back Codex session ID to ProviderSession
				// This MUST happen before the goroutine returns, so the next execution
				// can read the updated ExternalSessionID from ProviderSession.
				if execExternalSessionID != "" && providerSessionID != "" {
					if err := s.UpdateProviderSession(providerSessionID, func(ps *ProviderSession) {
						ps.ExternalSessionID = execExternalSessionID
						ps.Status = "active"
						ps.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
					}); err != nil {
						failNode(fmt.Errorf("provider session update: %w", err))
						return
					}
					// Debug: verify the update
					if verifyPS, ok := s.GetProviderSession(providerSessionID); ok {
						_ = verifyPS // update confirmed
					}
				}

				// Phase 4c: Create RuntimeState if executor returned a runtime.
				if nodeRuntimeID != "" {
					port := portFromEndpoint(nodeEndpoint)
					executorName := string(nodeCopy.Executor)
					model := nodeCopy.Model
					pid := 0
					createdAt := time.Now()

					rtState := RuntimeState{
						RuntimeID:      nodeRuntimeID,
						SessionID:      sessionID,
						AgentBindingID: binding.ID,
						NodeID:         nodeID,
						RunID:          run.ID,
						Executor:       executorName,
						Model:          model,
						Endpoint:       nodeEndpoint,
						Port:           port,
						PID:            pid,
						Status:         RuntimeReady,
						CreatedAt:      createdAt,
						LastActiveAt:   time.Now(),
						CleanupPolicy:  CleanupRetained,
						AccessMode:     runtimeAccessMode(nodeCopy.Executor, nodeCopy.Mode),
						ApprovalMode:   nodeCopy.ApprovalMode,
						ExecutionMode:  nodeCopy.ExecutionMode,
					}
					applyLiveRuntimeState(&rtState)
					if err := s.CreateRuntimeState(rtState); err != nil {
						failNode(fmt.Errorf("runtime state creation failed: %w", err))
						return
					}
				}

				// Update binding and provider session with runtime info.
				if nodeRuntimeID != "" {
					if err := s.UpdateBinding(binding.ID, func(b *AgentBinding) {
						b.CurrentRuntimeID = nodeRuntimeID
					}); err != nil {
						failNode(fmt.Errorf("binding update failed: %w", err))
						return
					}
					if providerSessionID != "" {
						if err := s.UpdateProviderSession(providerSessionID, func(ps *ProviderSession) {
							ps.LastKnownRuntimeID = nodeRuntimeID
							ps.LastKnownEndpoint = nodeEndpoint
						}); err != nil {
							failNode(fmt.Errorf("provider session update failed: %w", err))
							return
						}
					}
				}

				if err := s.UpdateBinding(binding.ID, func(b *AgentBinding) {
					b.LastRunID = run.ID
					b.LastAttemptID = attempt.ID
				}); err != nil {
					failNode(fmt.Errorf("binding last run update: %w", err))
					return
				}

				// Phase 5: Update run state.
				s.mu.Lock()
				state.DoneAt = doneAt
				state.Stderr = nodeStderr
				if execErr != nil {
					state.Status = NodeFailed
					state.Error = execErr.Error()
					if ctx.Err() == context.Canceled {
						run.Status = "canceled"
					} else {
						run.Status = "failed"
					}
					run.Error = fmt.Sprintf("node %s failed: %s", nodeCopy.Label, execErr)
					run.FinishedAt = doneAt
				} else if strings.TrimSpace(output) == "" {
					state.Status = NodeFailed
					state.Error = "agent completed without assistant output"
					run.Status = "failed"
					run.Error = fmt.Sprintf("node %s: agent completed without assistant output", nodeCopy.Label)
					run.FinishedAt = doneAt
				} else {
					state.Status = NodeComplete
					state.Output = output
					state.TokenUsage = TokenUsage{DurationMs: time.Since(mustParseTime(state.StartedAt)).Milliseconds()}
					if realUsage != nil {
						state.TokenUsage = *realUsage
						state.TokenUsage.DurationMs = time.Since(mustParseTime(state.StartedAt)).Milliseconds()
					}
				}
				run.NodeStates[nodeID] = state
				run.UpdatedAt = doneAt
				// Deep copy for persistence (save outside lock without race)
				runCopy := *run
				if run.NodeStates != nil {
					runCopy.NodeStates = make(map[string]RunState, len(run.NodeStates))
					for k, v := range run.NodeStates {
						runCopy.NodeStates[k] = v
					}
				}
				if run.NodeAttemptIDs != nil {
					runCopy.NodeAttemptIDs = make([]string, len(run.NodeAttemptIDs))
					copy(runCopy.NodeAttemptIDs, run.NodeAttemptIDs)
				}
				s.mu.Unlock()

				// Persist run state (using copy — safe outside lock).
				runDir := filepath.Join(sessionDir(sessionID), "runs")
				if perr := saveSessionJSON(runDir, run.ID+".json", &runCopy); perr != nil {
					// The live run is shared by all level goroutines. Publish
					// persistence failures under the store lock instead of writing
					// the shared pointer after the lock was released.
					s.mu.Lock()
					run.Error = fmt.Sprintf("node %s persist failed: %v", nodeCopy.Label, perr)
					run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
					s.mu.Unlock()
				}
			}()
		}
		wg.Wait()
	}
	return retErr
}

// gatherInputV2 collects output from upstream node Attempts in the current Run.
func (s *Store) gatherInputV2(pipe *Pipeline, run *PipelineRun, nodeID string) string {
	node := findNode(pipe, nodeID)
	upstream := upstreamEdges(pipe, nodeID)
	if len(upstream) == 0 {
		if run.Task != "" {
			if node != nil && node.RoleDesc != "" {
				return fmt.Sprintf("## 节点职责\n%s\n\n## 原始任务\n%s", node.RoleDesc, run.Task)
			}
			return run.Task
		}
		if node != nil && node.RoleDesc != "" {
			return fmt.Sprintf("你是一个%s。你的任务是：%s。请开始工作。", node.Label, node.RoleDesc)
		}
		return fmt.Sprintf("请完成你的角色任务。角色：%s", node.Label)
	}

	// Read upstream output from NodeAttempts in the current Run.
	s.mu.Lock()
	defer s.mu.Unlock()

	var parts []string
	if node != nil && node.RoleDesc != "" {
		parts = append(parts, "## 节点职责\n"+node.RoleDesc)
	}
	if run.Task != "" {
		parts = append(parts, "## 原始任务\n"+run.Task)
	}
	parts = append(parts, "## 上游节点输出")

	// Find the latest complete attempt for each upstream node.
	for _, e := range upstream {
		var latestOutput string
		for _, attID := range run.NodeAttemptIDs {
			if att, ok := s.attempts[attID]; ok && att.NodeID == e.FromID && att.Status == "complete" {
				latestOutput = att.Output
			}
		}
		// V2 runs: no fallback to NodeStates — upstream must have completed Attempt.
		if latestOutput != "" {
			label := e.FromID
			if fromNode := findNode(pipe, e.FromID); fromNode != nil && fromNode.Label != "" {
				label = fromNode.Label
			}
			parts = append(parts, fmt.Sprintf("### %s\n%s", label, latestOutput))
		}
	}
	return strings.Join(parts, "\n\n---\n\n")
}

// clonePipelineRun returns a detached snapshot of a run. PipelineRun is
// updated by background execution goroutines, so a shallow struct copy is not
// safe to hand to JSON encoders or callers: maps, slices, and FinalReview
// would still point at the live run.
func cloneSession(sess *Session) Session {
	if sess == nil {
		return Session{}
	}
	cp := *sess
	cp.RunIDs = append([]string(nil), sess.RunIDs...)
	return cp
}

func clonePipelineRun(r *PipelineRun) PipelineRun {
	if r == nil {
		return PipelineRun{}
	}
	cp := *r
	if r.NodeStates != nil {
		cp.NodeStates = make(map[string]RunState, len(r.NodeStates))
		for k, v := range r.NodeStates {
			cp.NodeStates[k] = v
		}
	}
	if r.IterationIDs != nil {
		cp.IterationIDs = append([]string(nil), r.IterationIDs...)
	}
	if r.NodeAttemptIDs != nil {
		cp.NodeAttemptIDs = append([]string(nil), r.NodeAttemptIDs...)
	}
	if r.FinalReview != nil {
		fr := *r.FinalReview
		fr.BlockingIssues = append([]string(nil), r.FinalReview.BlockingIssues...)
		fr.RequiredChanges = append([]string(nil), r.FinalReview.RequiredChanges...)
		fr.Evidence = append([]string(nil), r.FinalReview.Evidence...)
		cp.FinalReview = &fr
	}
	return cp
}

// GetRun returns a deep copy of a run by ID.
func (s *Store) GetRun(id string) (PipelineRun, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.runs[id]
	if !ok {
		return PipelineRun{}, false
	}
	return clonePipelineRun(r), true
}

// CancelRun cancels a running pipeline.
func (s *Store) CancelRun(id string) error {
	s.mu.Lock()
	r, ok := s.runs[id]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("run %q not found", id)
	}
	if r.Status != "running" {
		s.mu.Unlock()
		return fmt.Errorf("run %q is not running (status: %s)", id, r.Status)
	}
	cancel := r.Cancel
	if cancel != nil {
		cancel()
	}
	r.Status = "canceled"
	r.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	sessionID := r.SessionID
	s.mu.Unlock()

	if sessionID != "" {
		if err := s.persistRun(r, sessionID); err != nil {
			return fmt.Errorf("persist canceled run: %w", err)
		}
	}
	return nil
}

// ListRuns returns all runs, newest first.
func (s *Store) ListRuns() []PipelineRun {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]PipelineRun, 0, len(s.runs))
	for _, r := range s.runs {
		out = append(out, clonePipelineRun(r))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt > out[j].CreatedAt
	})
	return out
}

// ListSessions returns all sessions, newest first.
func (s *Store) ListSessions() []Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		clone := cloneSession(sess)
		clone.Stats = s.aggregateSessionStatsLocked(sess.RunIDs)
		out = append(out, clone)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].UpdatedAt > out[j].UpdatedAt
	})
	return out
}

// GetSession returns a session by ID with its runs.
func (s *Store) GetSession(id string) (*Session, []PipelineRun, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.sessions[id]
	if !ok {
		return nil, nil, false
	}
	var runs []PipelineRun
	for _, runID := range sess.RunIDs {
		if r, ok := s.runs[runID]; ok {
			runs = append(runs, clonePipelineRun(r))
		}
	}
	clone := cloneSession(sess)
	clone.Stats = aggregateSessionStats(runs)
	return &clone, runs, true
}

// RegisterAgent records a running agent instance.
func (s *Store) RegisterAgent(a AgentInstance) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.agents[a.ID] = &a
}

// UpdateAgentStatus updates the transient UI-facing status of a managed agent.
// Serve runtimes are intentionally retained after a successful task, so their
// agent entry must become idle instead of remaining forever in running state.
func (s *Store) UpdateAgentStatus(id, status, errMsg string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a, ok := s.agents[id]; ok {
		a.Status = status
		a.Error = errMsg
	}
}

// UnregisterAgent removes an agent instance.
func (s *Store) UnregisterAgent(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.agents, id)
}

// ListAgents returns all registered agent instances.
func (s *Store) ListAgents() []AgentInstance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AgentInstance, 0, len(s.agents))
	for _, a := range s.agents {
		out = append(out, *a)
	}
	return out
}

// GetStats aggregates statistics across all runs with per-node-type breakdown.
func (s *Store) GetStats() Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var st Stats
	perNode := make(map[NodeType]*NodeStat)
	for _, r := range s.runs {
		st.TotalRuns++
		for nodeID, ns := range r.NodeStates {
			t := sanitizeTokenUsage(ns.TokenUsage)
			st.TotalTokens += t.TotalTokens
			st.TotalCostYuan += t.CostYuan
			st.TotalDurationMs += t.DurationMs

			// Find node type from the owning pipeline (C-3 fix).
			nt := NodeReviewer
			if p, ok := s.pipelines[r.PipelineID]; ok {
				for _, n := range p.Nodes {
					if n.ID == nodeID {
						nt = n.Type
						break
					}
				}
			}
			if _, ok := perNode[nt]; !ok {
				perNode[nt] = &NodeStat{Type: nt}
			}
			perNode[nt].RunCount++
			perNode[nt].TokenSum += t.TotalTokens
			perNode[nt].CostYuan += t.CostYuan
		}
	}
	for _, ns := range perNode {
		st.PerNode = append(st.PerNode, *ns)
	}
	return st
}

func sanitizeTokenUsage(t TokenUsage) TokenUsage {
	// Legacy orchestrator runs stored self-estimated costs without a provenance
	// marker. Keep their token/duration counters visible, but exclude those old
	// costs from every aggregate so billing reflects only authoritative
	// `reasonix run --metrics` data.
	if t.Source == "" {
		t.CostYuan = 0
	}
	return t
}

func (s *Store) aggregateSessionStatsLocked(runIDs []string) SessionStats {
	runs := make([]PipelineRun, 0, len(runIDs))
	for _, runID := range runIDs {
		if r, ok := s.runs[runID]; ok {
			runs = append(runs, *r)
		}
	}
	return aggregateSessionStats(runs)
}

func aggregateSessionStats(runs []PipelineRun) SessionStats {
	var st SessionStats
	for _, run := range runs {
		st.TotalRuns++
		for _, ns := range run.NodeStates {
			t := sanitizeTokenUsage(ns.TokenUsage)
			st.TotalTokens += t.TotalTokens
			st.TotalCostYuan += t.CostYuan
			st.TotalDuration += t.DurationMs
		}
	}
	return st
}

// Presets returns pre-built pipeline templates.
func Presets() []PipelinePreset {
	return []PipelinePreset{
		{
			ID: "default-three-agent", Name: "三 Agent 流水线",
			Desc: "Pro 架构 → MiMo 实现 → Flash 审查",
			Pipeline: Pipeline{
				Nodes: []AgentNode{
					{ID: "n1", Type: NodeArchitect, Label: "架构师", Model: "deepseek-pro", Skill: "brainstorming", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "设计系统架构和方案", X: 100, Y: 200},
					{ID: "n2", Type: NodeExecutor, Label: "执行者", Model: "mimo-v2.5", Agent: "", Executor: ExecutorMimo, Mode: "serve", RoleDesc: "根据架构实现代码", X: 400, Y: 200},
					{ID: "n3", Type: NodeReviewer, Label: "审查者", Model: "deepseek-flash", Skill: "review", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "审查代码质量和安全性", X: 700, Y: 200},
				},
				Edges: []Edge{
					{ID: "e1", FromID: "n1", ToID: "n2", Label: "架构设计"},
					{ID: "e2", FromID: "n2", ToID: "n3", Label: "代码审查"},
				},
			},
		},
		{
			ID: "parallel-research", Name: "并行调研",
			Desc: "拆解任务 → 并行调研 → 汇总结果",
			Pipeline: Pipeline{
				Nodes: []AgentNode{
					{ID: "n1", Type: NodeReviewer, Label: "任务拆解", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "将大任务拆解为可并行的子任务", X: 100, Y: 200},
					{ID: "n2", Type: NodeArchitect, Label: "调研 A", Model: "deepseek-pro", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "调研方向 A", X: 400, Y: 100},
					{ID: "n3", Type: NodeExecutor, Label: "调研 B", Model: "mimo-v2.5", Agent: "", Executor: ExecutorMimo, Mode: "serve", RoleDesc: "调研方向 B", X: 400, Y: 300},
					{ID: "n4", Type: NodeReviewer, Label: "汇总", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "汇总两路调研结果", X: 700, Y: 200},
				},
				Edges: []Edge{
					{ID: "e1", FromID: "n1", ToID: "n2", Label: "方向 A"},
					{ID: "e1b", FromID: "n1", ToID: "n3", Label: "方向 B"},
					{ID: "e2", FromID: "n2", ToID: "n4", Label: "结果 A"},
					{ID: "e2b", FromID: "n3", ToID: "n4", Label: "结果 B"},
				},
			},
		},
		{
			ID: "design-review", Name: "设计→审查",
			Desc: "Pro 设计 → Flash 审查设计",
			Pipeline: Pipeline{
				Nodes: []AgentNode{
					{ID: "n1", Type: NodeArchitect, Label: "设计师", Model: "deepseek-pro", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "输出详细设计方案", X: 100, Y: 200},
					{ID: "n2", Type: NodeReviewer, Label: "审查者", Model: "deepseek-flash", Skill: "review", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "审查方案可行性", X: 400, Y: 200},
				},
				Edges: []Edge{
					{ID: "e1", FromID: "n1", ToID: "n2", Label: "方案审查"},
				},
			},
		},
		{
			ID: "loop-iterate", Name: "循环迭代（固定 3 轮）",
			Desc: "架构 → 实现 → 审查，固定循环 3 轮（Loop）",
			Pipeline: Pipeline{
				Nodes: []AgentNode{
					{ID: "n1", Type: NodeArchitect, Label: "架构师", Model: "deepseek-pro", Skill: "brainstorming", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "设计系统架构和方案", X: 100, Y: 200},
					{ID: "n2", Type: NodeExecutor, Label: "执行者", Model: "mimo-v2.5", Agent: "", Executor: ExecutorMimo, Mode: "serve", RoleDesc: "根据架构实现代码", X: 400, Y: 200},
					{ID: "n3", Type: NodeReviewer, Label: "审查者", Model: "deepseek-flash", Skill: "review", Executor: ExecutorReasonix, Mode: "serve", RoleDesc: "审查代码质量和安全性", X: 700, Y: 200},
				},
				Edges: []Edge{
					{ID: "e1", FromID: "n1", ToID: "n2", Label: "架构设计"},
					{ID: "e2", FromID: "n2", ToID: "n3", Label: "代码审查"},
				},
			},
			LoopConfig: &LoopConfig{
				Enabled:         true,
				Mode:            "fixed",
				FixedIterations: 3,
				ReviewNodeID:    "n3",
				Protocol:        "loop-review-v1",
			},
		},
	}
}

// --- helpers ---

func topologicalSort(pipe *Pipeline) []string {
	inDeg := make(map[string]int)
	adj := make(map[string][]string)
	for _, n := range pipe.Nodes {
		inDeg[n.ID] = 0
	}
	for _, e := range pipe.Edges {
		adj[e.FromID] = append(adj[e.FromID], e.ToID)
		inDeg[e.ToID]++
	}
	var queue []string
	for id, d := range inDeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	var order []string
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		order = append(order, u)
		for _, v := range adj[u] {
			inDeg[v]--
			if inDeg[v] == 0 {
				queue = append(queue, v)
			}
		}
	}
	if len(order) != len(pipe.Nodes) {
		// Debug: log the issue
		missing := make(map[string]bool)
		for id := range inDeg {
			missing[id] = true
		}
		for _, id := range order {
			delete(missing, id)
		}
		for id := range missing {
			_ = id // would log here
		}
		return nil // cycle
	}
	return order
}

// topologicalLevels returns nodes grouped into parallel levels.
// Each level contains nodes whose dependencies are all in earlier levels.
func topologicalLevels(pipe *Pipeline) [][]string {
	inDeg := make(map[string]int)
	adj := make(map[string][]string)
	for _, n := range pipe.Nodes {
		inDeg[n.ID] = 0
	}
	for _, e := range pipe.Edges {
		adj[e.FromID] = append(adj[e.FromID], e.ToID)
		inDeg[e.ToID]++
	}
	var levels [][]string
	var queue []string
	for id, d := range inDeg {
		if d == 0 {
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		level := make([]string, len(queue))
		copy(level, queue)
		levels = append(levels, level)
		queue = nil
		for _, u := range level {
			for _, v := range adj[u] {
				inDeg[v]--
				if inDeg[v] == 0 {
					queue = append(queue, v)
				}
			}
		}
	}
	if len(levels) == 0 {
		return nil
	}
	total := 0
	for _, l := range levels {
		total += len(l)
	}
	if total != len(pipe.Nodes) {
		return nil // cycle
	}
	return levels
}

func findNode(pipe *Pipeline, id string) *AgentNode {
	for i := range pipe.Nodes {
		if pipe.Nodes[i].ID == id {
			return &pipe.Nodes[i]
		}
	}
	return nil
}

func upstreamEdges(pipe *Pipeline, nodeID string) []Edge {
	var out []Edge
	for _, e := range pipe.Edges {
		if e.ToID == nodeID {
			out = append(out, e)
		}
	}
	return out
}

// clonePipeline creates a deep copy for safe async execution.
func clonePipeline(p *Pipeline) *Pipeline {
	if p == nil {
		return nil
	}
	cp := &Pipeline{
		ID:        p.ID,
		Name:      p.Name,
		CreatedAt: p.CreatedAt,
		UpdatedAt: p.UpdatedAt,
	}
	cp.Nodes = make([]AgentNode, len(p.Nodes))
	copy(cp.Nodes, p.Nodes)
	cp.Edges = make([]Edge, len(p.Edges))
	copy(cp.Edges, p.Edges)
	return cp
}

// loadRolePrompt reads the role prompt MD file for a given node type.
func loadRolePrompt(nodeType NodeType) string {
	name := string(nodeType) + ".md"
	data, err := promptFS.ReadFile("prompts/" + name)
	if err != nil {
		return ""
	}
	return string(data)
}

const architectAssignmentRules = `# TASK ASSIGNMENT RULES (MANDATORY)
你是架构师，负责为下游节点分配子任务。你无法感知下游节点的实际模型能力，必须遵守：

- 下游执行者/审查者的模型可能不支持图像输入（视觉能力未知），不要假设它们"直接看图"。
- 凡涉及识图的任务（比对效果图、截图、设计稿、报错图等），分配文本必须明确要求执行者通过辅助手委派识图（运行执行环境提供的辅助手委派命令），并把图片文件路径写清楚。
- 禁止在分配文本中要求下游节点直接调用 read_image 等图像输入工具；禁止替下游节点编造任何图像内容或图像差异。
- 分配文本应写明最终交付格式（JSON 结构、字段、文件路径），让下游节点只负责执行，不负责猜测你的意图。
`

const opencodeExecutionDisciplinePrompt = `# EXECUTION DISCIPLINE (MANDATORY)

免费/流式模型容易把思考过程当作输出。本节点必须遵守：

- 禁止复述任务、禁止输出"我将要做/让我先…/我来探索…"之类的计划清单或思考常规；直接执行。
- 探索最小化：最多读取 3 个文件/目录（read/glob/grep 各最多 2 次），读完立即产出最终结果。
- 禁止连续自我循环：不得反复说"开始探索""继续探索""再确认一下"；同一意图只做一次。
- 最终响应必须是完整交付物正文（方案、文档、实现、结论），不能是计划、洋葱式思考或角色能力自述（例如"我不能写文件"）。
- 如果你只负责分析并产出 Markdown 文档，直接把完整 Markdown 写在最终响应里，不要创建文件，也不要让人再帮你复制。
- 输出完毕后立即结束；不要自我审查、不要再次总结、不要生成第二个版本。
`

const loopNodeBoundaryPrompt = `# LOOP NODE EXECUTION BOUNDARY (MANDATORY)
你只处理当前这一轮、当前节点的一次调用。

- Loop 的轮数、下一轮是否开始、何时结束，全部由 Orchestrator 状态机决定，不由你决定。
- 不得自行设置新的 Goal，不得在节点内部循环、迭代或模拟三轮，不得自行调用后续节点。
- 不得把“循环三轮、直到通过、下一轮继续”等编排逻辑写进自己的执行步骤。
- 当前节点完成自己的单次职责后，立即输出结果并结束本次调用。
- 架构师只做方案设计，不写代码、不执行实现；执行者只实现当前轮任务；审查者只审查当前轮并给出结论。
- 如果上游没有提供足够信息，只报告缺失信息，不要自行扩展成另一个工作流。
`

const loopReviewerProtocolPrompt = `# LOOP REVIEW PROTOCOL (MANDATORY)
你现在是本轮 Loop 的唯一审查者，不是执行者。

你的职责：
- 决策必须对照「原始任务 + 架构师设计文档（若存在，见输入中的文件路径）」中的总体计划，而不是仅凭当轮代码的语法/逻辑；若本轮执行未覆盖总体计划中的任务项，必须 revise 并在 requiredChanges 中列出未覆盖项；
- 只审查上游节点在本轮产生的输出以及工作区中的已有结果；
- 可以读取文件和运行只读检查；禁止修改、创建、删除文件，禁止执行写入型命令；
- 本次调用最多进行一个审查回合：同一命令最多执行一次，最多执行 3 个必要的只读检查；
- 禁止 autoresearch、后台任务、持续研究、自动设置 Goal、重复验证、轮询或在已经得到结论后再次执行命令；输出 pass/revise/blocked 后立即结束；
- 不要把工具调用参数、命令 JSON、工具返回内容当成最终审查结果；工具调用结束后必须继续生成最终文本；
- 最终响应必须是“纯 JSON”，不能有 Markdown 代码围栏、解释、前缀、后缀或第二个 JSON；
- JSON 必须严格符合 loop-review-v1，至少包含 schemaVersion、decision、confidence、summary、blockingIssues、requiredChanges、nextTask、evidence；
- schemaVersion 必须是 "loop-review-v1"；decision 只能是 "pass"、"revise" 或 "blocked"；
- pass 时 blockingIssues、requiredChanges 必须是空数组，nextTask 必须是空字符串；
- revise 时 requiredChanges 必须是非空数组，nextTask 必须是非空字符串；
- blocked 时 blockingIssues 必须是非空数组；
- 如果无法完成审查或无法给出合法 JSON，必须输出 decision=blocked，并在 blockingIssues 中说明原因，不能输出工具调用 JSON。

最终 JSON 示例（只输出对象本身）：
{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"审查结论","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["证据"]}`

// executeNode runs a single pipeline node via the configured executor runtime.
// Returns output, stderr, token usage, runtimeID, endpoint, externalSessionID, and error.
// contextPolicy and externalSessionID are passed through for Codex session management.
func (s *Store) executeNode(ctx context.Context, node *AgentNode, input string, contextPolicy, externalSessionID string) (output, stderr string, realUsage *TokenUsage, runtimeID, endpoint, retExternalSessionID string, err error) {
	return s.executeNodeAtWorkspace(ctx, node, input, contextPolicy, externalSessionID, detectWorkspace())
}

func (s *Store) executeNodeAtWorkspace(ctx context.Context, node *AgentNode, input string, contextPolicy, externalSessionID, workspace string) (output, stderr string, realUsage *TokenUsage, runtimeID, endpoint, retExternalSessionID string, err error) {
	return s.executeNodeWithLoopProtocolAtWorkspace(ctx, node, input, contextPolicy, externalSessionID, false, false, workspace)
}

// executeNodeWithLoopProtocol is the internal execution path used by Loop's
// configured reviewer. Keeping the flag out of AgentNode prevents transient
// execution policy from leaking into persisted pipeline revisions.
func (s *Store) executeNodeWithLoopProtocol(ctx context.Context, node *AgentNode, input string, contextPolicy, externalSessionID string, loopActive, loopReview bool) (output, stderr string, realUsage *TokenUsage, runtimeID, endpoint, retExternalSessionID string, err error) {
	return s.executeNodeWithLoopProtocolAtWorkspace(ctx, node, input, contextPolicy, externalSessionID, loopActive, loopReview, detectWorkspace())
}

func (s *Store) executeNodeWithLoopProtocolAtWorkspace(ctx context.Context, node *AgentNode, input string, contextPolicy, externalSessionID string, loopActive, loopReview bool, workspace string, startNotify ...func(string, int)) (output, stderr string, realUsage *TokenUsage, runtimeID, endpoint, retExternalSessionID string, err error) {
	// Emit start event.
	s.emit(event.Event{Kind: event.PipelineNodeStart, Text: node.ID})

	// Load role prompt and prepend to input.
	rolePrompt := loadRolePrompt(node.Type)
	if loopActive {
		rolePrompt += "\n\n---\n\n" + loopNodeBoundaryPrompt
	}
	if loopReview {
		rolePrompt += "\n\n---\n\n" + loopReviewerProtocolPrompt
	}
	executorName := node.Executor
	if executorName == "" {
		executorName = ExecutorReasonix
	}
	task := input
	if rolePrompt != "" {
		task = rolePrompt + "\n\n---\n\n## 当前节点\n\n- 名称: " + node.Label + "\n- 模型: " + node.Model + "\n\n## 执行上下文\n\n" + input
	}
	// Architect nodes assign work to downstream nodes without knowing the
	// downstream model's capabilities. Constrain the assignment so vision work
	// always goes through the helper-agent delegation path instead of raw image
	// tools the downstream model may not support.
	if node.Type == NodeArchitect {
		task = architectAssignmentRules + "\n\n" + task
	}
	// Free/streaming models (e.g. deepseek-v4-flash-free) tend to burn their
	// output budget restating plans and "let me explore" loops, then fail with
	// an empty assistant turn. Hardening the prompt keeps those models on a
	// single bounded pass for opencode nodes.
	if executorName == ExecutorOpencode {
		task += "\n\n---\n\n" + opencodeExecutionDisciplinePrompt
	}

	workspace = strings.TrimSpace(workspace)
	if workspace == "" {
		workspace = detectWorkspace()
	}
	modelRef := strings.TrimSpace(node.Model)
	providerRoute := strings.ToLower(strings.TrimSpace(node.ProviderRoute))
	// "ccs" is a frontend-friendly alias. Treat it as a route selection, not
	// as a real Codex/Claude model name.
	if (executorName == ExecutorCodex || executorName == ExecutorClaude) && (strings.EqualFold(modelRef, "ccs") || strings.EqualFold(modelRef, "ccswitch")) {
		providerRoute = "ccswitch"
		modelRef = ""
	}
	// A Loop is controlled by the orchestrator, never by an agent's goal
	// runner. Persisted/generated nodes may still carry executionMode=goal;
	// allowing that value here makes the node start its own autonomous loop and
	// can continue issuing tools after the outer Loop iteration has finished.
	// Force every Loop node into one bounded task turn. This is especially
	// important for the reviewer, whose only valid result is one JSON decision.
	executionMode := node.ExecutionMode
	if loopActive {
		executionMode = "task"
	}
	// Codex/Claude are local CLIs whose active provider/model may be supplied
	// by CCSwitch. An empty model therefore defaults to the CCSwitch route.
	if (executorName == ExecutorCodex || executorName == ExecutorClaude) && modelRef == "" && providerRoute == "" {
		providerRoute = "ccswitch"
	}
	if cfgErr := validateNodeExecutionConfigAtWorkspaceWithRoute(executorName, node.Mode, modelRef, node.Agent, providerRoute, workspace); cfgErr != nil {
		s.emit(event.Event{Kind: event.PipelineNodeFailed, Text: node.ID, Detail: cfgErr.Error()})
		return "", cfgErr.Error(), nil, "", "", "", cfgErr
	}
	resolvedModelRef := resolveExecutorModelRef(workspace, executorName, node.Mode, modelRef)

	// Load Skill content for prompt injection
	var skillContent string
	if node.Skill != "" {
		if content, err := loadSkillContent(node.Skill); err == nil {
			skillContent = content
		}
		// If skill loading fails, log but don't fail the node
	}

	// Build execution spec.
	spec := ExecSpec{
		Workspace:         workspace,
		Prompt:            task,
		ModelRef:          resolvedModelRef,
		DisplayModel:      node.Model,
		ProviderRoute:     providerRoute,
		Agent:             node.Agent,
		Skill:             node.Skill,
		SkillContent:      skillContent,
		SkillPolicy:       node.SkillPolicy,
		ReasoningEffort:   node.ReasoningEffort,
		Mode:              node.Mode,
		Executor:          string(executorName),
		ApprovalMode:      node.ApprovalMode,
		ExecutionMode:     executionMode,
		Trust:             true,
		NeverAsk:          true,
		NodeID:            node.ID,
		NodeLabel:         node.Label,
		ContextPolicy:     contextPolicy,
		ExternalSessionID: externalSessionID,
		DshPreset:         strings.TrimSpace(node.DshPreset),
		StallMaintenance:  stallMaintenanceEnabled(node.StallMaintenance),
	}
	if loopReview {
		// Keep reviewer cost bounded even if the provider ignores the prompt
		// boundary and keeps trying tools. Reasonix enforces this via --max-steps.
		spec.MaxSteps = 3
		if strings.Contains(input, "[系统纠正]") {
			// The format-correction call must not start another review/tool
			// cycle; give it only one finalization round.
			spec.MaxSteps = 1
		}
	} else if executorName == ExecutorOpencode && spec.MaxSteps == 0 {
		// Bound free/streaming models (deepseek-v4-flash-free) to a single
		// bounded pass so a "let me explore" loop cannot run forever. opencode
		// run honors it via --max-steps; serve mode relies on the prompt
		// discipline block plus the turn timeout.
		spec.MaxSteps = 25
	}
	if executorName == ExecutorOpencode || executorName == ExecutorDsh {
		// Per-role serve-mode tuning: the architect gets read-only tools so
		// it can inspect the codebase without a write/execute exploration
		// loop; the executor reads the codebase and writes the real
		// deliverable, so it historically got the longest budget. These
		// TurnTimeout values are total-duration ceilings on top of the
		// activity watchdog (watchdog.go), which already renews on every
		// provider event and only fires when the stream goes silent — so a
		// legitimately streaming turn is never cut mid-work. 0 = no per-role
		// ceiling; the global turnMaxDurationDefault governs instead.
		switch node.Type {
		case NodeArchitect:
			spec.ToolsReadOnly = true
			spec.TurnTimeout = 30 * time.Minute
		case NodeReviewer:
			spec.TurnTimeout = 30 * time.Minute
		case NodeExecutor:
			spec.TurnTimeout = 0
		}
	}
	// Select executor.
	executor := getExecutor(node.Executor)
	if executor == nil {
		executor = getExecutor(ExecutorReasonix)
	}

	// Execute — pass onStart callback so the frontend shows the port badge
	// the moment the serve process starts (before waiting for readiness).
	onStart := func(endpoint string, port int) {
		if len(startNotify) > 0 && startNotify[0] != nil {
			startNotify[0](endpoint, port)
		}
		s.emitRuntimeEvent(node.ID, endpoint, port, "starting", string(node.Executor), runtimeAccessMode(node.Executor, node.Mode), "")
	}
	result, execErr := executor.Execute(ctx, spec, onStart)
	if result == nil {
		result = &ExecResult{}
	}
	output = result.FinalText
	stderr = result.RawStderr
	retExternalSessionID = result.ExternalSessionID
	if result.RuntimeID != "" {
		endpoint := result.Endpoint
		port := portFromEndpoint(endpoint)
		status := RuntimeReady
		model := node.Model
		executorType := node.Executor
		if live, ok := managedRuntimeState(result.RuntimeID); ok {
			if live.Endpoint != "" {
				endpoint = live.Endpoint
				port = live.Port
			}
			if live.Status != "" {
				status = live.Status
			}
			if live.Model != "" {
				model = live.Model
			}
			if live.Executor != "" {
				executorType = ExecutorType(live.Executor)
			}
		}
		s.RegisterAgent(AgentInstance{
			ID:       result.RuntimeID,
			Type:     node.Type,
			Port:     port,
			Model:    model,
			Status:   string(status),
			Label:    node.Label,
			Executor: executorType,
			Endpoint: endpoint,
		})
		// Emit the live provider state instead of always displaying a completed
		// Codex app-server turn as "ready".
		accessMode := runtimeAccessMode(executorType, node.Mode)
		if live, ok := managedRuntimeState(result.RuntimeID); ok && live.AccessMode != "" {
			accessMode = live.AccessMode
		}
		s.emitRuntimeEvent(node.ID, endpoint, port, string(status), string(executorType), accessMode, output)
	}

	if execErr != nil {
		if ctx.Err() == context.Canceled || ctx.Err() == context.DeadlineExceeded {
			s.emit(event.Event{Kind: event.PipelineNodeFailed, Text: node.ID, Detail: "cancelled"})
			// Keep any partial output so the stall-repair scene and the
			// interrupted attempt state can still be inspected.
			return result.FinalText, stderr, result.TokenUsage, result.RuntimeID, result.Endpoint, result.ExternalSessionID, ctx.Err()
		}
		errMsg := stderr
		if errMsg == "" {
			errMsg = execErr.Error()
		}
		s.emit(event.Event{Kind: event.PipelineNodeFailed, Text: node.ID, Detail: errMsg})
		return "", stderr, nil, result.RuntimeID, result.Endpoint, result.ExternalSessionID, fmt.Errorf("%s", errMsg)
	}

	if strings.TrimSpace(output) == "" {
		errMsg := strings.TrimSpace(stderr)
		if errMsg == "" {
			errMsg = "executor completed without any output"
		}
		s.emit(event.Event{Kind: event.PipelineNodeFailed, Text: node.ID, Detail: errMsg})
		return "", stderr, result.TokenUsage, result.RuntimeID, result.Endpoint, result.ExternalSessionID, fmt.Errorf("%s", errMsg)
	}

	// Persist the architect's output as a design document so later iterations
	// (and the reviewer) can reference the overall plan by path instead of
	// having the full text copied into every downstream prompt. The reviewer
	// stays read-only and opens the file itself.
	if node.Type == NodeArchitect {
		persistArchitectPlan(workspace, node.ID, output)
	}

	// Send full output in the SSE event.
	s.emit(event.Event{Kind: event.PipelineNodeDone, Text: node.ID, Detail: output})
	return output, stderr, result.TokenUsage, result.RuntimeID, result.Endpoint, result.ExternalSessionID, nil
}

// persistArchitectPlan writes the architect's plan to
// <workspace>/.reasonix/plans/<nodeID>.md so reviewers can read it by path.
func persistArchitectPlan(workspace, nodeID, output string) {
	if strings.TrimSpace(workspace) == "" || strings.TrimSpace(nodeID) == "" || strings.TrimSpace(output) == "" {
		return
	}
	planDir := filepath.Join(workspace, ".reasonix", "plans")
	if err := os.MkdirAll(planDir, 0o755); err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(planDir, nodeID+".md"), []byte(output), 0o644)
}

// architectPlanPath returns the on-disk location of the persisted architect
// design document for a node, and whether the file currently exists. Reviewer
// prompts reference this path instead of receiving the full architect output.
func architectPlanPath(workspace, architectNodeID string) (string, bool) {
	if strings.TrimSpace(workspace) == "" || strings.TrimSpace(architectNodeID) == "" {
		return "", false
	}
	p := filepath.Join(workspace, ".reasonix", "plans", architectNodeID+".md")
	_, err := os.Stat(p)
	return p, err == nil
}

func normalizeExecutionModelRef(workspace, model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	if strings.Contains(model, "/") {
		if strings.HasPrefix(model, "mimo-") {
			if _, bare, ok := strings.Cut(model, "/"); ok {
				return bare
			}
		}
		return model
	}

	cfg, err := config.LoadForRoot(workspace)
	if err != nil {
		return model
	}
	config.NormalizeLegacyMimoCustomProvidersForRefs(cfg, model)
	if resolved, _, ok := cfg.ResolveModelWithFallback(model); ok {
		return resolved
	}
	return model
}

func resolveExecutorModelRef(workspace string, executor ExecutorType, mode string, model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}

	switch executor {
	case ExecutorMimo:
		return normalizeMimoExecutionModelRef(workspace, model)
	case ExecutorCodex, ExecutorClaude, ExecutorOpencode, ExecutorDsh:
		// Codex/Claude/opencode/dsh pass model refs through verbatim: the CLI
		// (or, for dsh, the harness settings) owns provider resolution
		// (deepseek-v4-flash, gpt-5.6-luna, o3, ...). They must NOT be run
		// through the Reasonix config resolver, which rewrites bare model
		// names into "provider/model" pairs (e.g. deepseek-v4-flash ->
		// deepseek-flash/deepseek-v4-flash) and breaks the CLI model argument.
		return model
	case ExecutorReasonix:
		if strings.EqualFold(strings.TrimSpace(mode), "run") {
			return normalizeReasonixRunModelRef(workspace, model)
		}
		fallthrough
	default:
		return normalizeExecutionModelRef(workspace, model)
	}
}

func validateNodeExecutionConfig(executor ExecutorType, mode string, model string, agent string) error {
	return validateNodeExecutionConfigAtWorkspace(executor, mode, model, agent, detectWorkspace())
}

func validateNodeExecutionConfigAtWorkspace(executor ExecutorType, mode string, model string, agent string, workspace string) error {
	return validateNodeExecutionConfigAtWorkspaceWithRoute(executor, mode, model, agent, "", workspace)
}

func validateNodeExecutionConfigAtWorkspaceWithRoute(executor ExecutorType, mode string, model string, agent string, providerRoute string, workspace string) error {
	model = strings.TrimSpace(model)
	agent = strings.TrimSpace(agent)
	providerRoute = strings.ToLower(strings.TrimSpace(providerRoute))
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		if executor == ExecutorDsh {
			// DSH headless is one-shot: an omitted mode means run.
			mode = "run"
		} else {
			mode = "serve"
		}
	}
	ccswitchRoute := (executor == ExecutorCodex || executor == ExecutorClaude) && (providerRoute == "ccs" || providerRoute == "ccswitch")
	// DSH reads its model from its own harness settings; the node model is an
	// advisory --patch overlay, so an empty model is valid for dsh.
	if model == "" && !ccswitchRoute && executor != ExecutorDsh {
		return fmt.Errorf("model is required")
	}

	isMimoModel := strings.HasPrefix(model, "mimo") || strings.HasPrefix(model, "mimo-") || strings.Contains(model, "/mimo-")
	isDeepseekModel := strings.HasPrefix(model, "deepseek") || strings.Contains(model, "deepseek-") || strings.HasPrefix(model, "deepseek/")

	switch executor {
	case ExecutorMimo:
		if mode != "run" && mode != "serve" {
			return fmt.Errorf("unsupported mimo mode %q", mode)
		}
		if !strings.Contains(resolveExecutorModelRef(workspace, executor, mode, model), "/") {
			return fmt.Errorf("mimo executor requires provider/model ref; current model %q could not be normalized", model)
		}
		if agent != "" {
			// Let explicit user-provided agent pass through, but reject the known bad placeholder.
			if strings.EqualFold(agent, "coder") {
				return fmt.Errorf("mimo agent %q is not a guaranteed built-in agent in current mimocode; clear the agent field or choose an installed agent", agent)
			}
		}
	case ExecutorReasonix:
		if mode != "run" && mode != "serve" {
			return fmt.Errorf("unsupported reasonix mode %q", mode)
		}
		if isDeepseekModel {
			return nil
		}
		if isMimoModel && mode == "run" {
			// reasonix run can route to provider aliases like mimo-flash/mimo-pro.
			return nil
		}
		if isMimoModel && mode == "serve" {
			// Allow, but only after normalization, since reasonix serve binds the model at server start.
			return nil
		}
	case ExecutorCodex:
		// `run` is the one-shot `codex exec` path. `serve` is the retained
		// `codex app-server` WebSocket path owned by CodexRuntimeManager.
		if mode != "run" && mode != "serve" {
			return fmt.Errorf("unsupported codex mode %q; supported modes are run and serve", mode)
		}
		if providerRoute != "" && !ccswitchRoute {
			return fmt.Errorf("unsupported codex provider route %q; use ccswitch", providerRoute)
		}
		// Deepseek and other self-configured models are passed through to the
		// Codex CLI; the CLI's own provider configuration decides availability.
		// The old hard rejection was lifted so per-node custom models work.
	case ExecutorClaude:
		// `run` is the one-shot `claude -p --output-format json` path. `serve`
		// is the retained stream-json runtime owned by ClaudeRuntimeManager.
		if mode != "run" && mode != "serve" {
			return fmt.Errorf("unsupported claude mode %q; supported modes are run and serve", mode)
		}
		if providerRoute != "" && !ccswitchRoute {
			return fmt.Errorf("unsupported claude provider route %q; use ccswitch", providerRoute)
		}
	case ExecutorOpencode:
		// `run` is the one-shot `opencode run --format json` path. `serve` is
		// the retained `opencode serve` HTTP runtime owned by
		// OpenCodeRuntimeManager. Models are provider/model refs passed
		// through verbatim (opencode/deepseek-v4-flash-free, deepseek/deepseek-v4-flash).
		if mode != "run" && mode != "serve" {
			return fmt.Errorf("unsupported opencode mode %q; supported modes are run and serve", mode)
		}
		if strings.TrimSpace(model) == "" {
			return fmt.Errorf("model is required for opencode executor")
		}
		if !strings.Contains(model, "/") {
			return fmt.Errorf("opencode executor requires provider/model ref (e.g. opencode/deepseek-v4-flash-free)")
		}
	case ExecutorDsh:
		// `run` is the one-shot `dsh --profile headless` path. `serve` is not
		// supported: DSH headless has no retained session protocol. Provider
		// routing is owned by DSH itself ($DSH_HOME/settings.yaml → llm-*),
		// so a providerRoute has no meaning here.
		if mode != "run" {
			return fmt.Errorf("unsupported dsh mode %q; DSH headless is one-shot, use mode=run", mode)
		}
		if providerRoute != "" {
			return fmt.Errorf("unsupported dsh provider route %q; DSH routes providers itself via its harness settings", providerRoute)
		}
	}
	return nil
}

func normalizeReasonixRunModelRef(workspace, model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	switch model {
	case "mimo-v2.5":
		return "mimo-flash"
	case "mimo-v2.5-pro":
		return "mimo-pro"
	}
	if !strings.Contains(model, "/") {
		return model
	}

	cfg, err := config.LoadForRoot(workspace)
	if err != nil {
		if provider, _, ok := strings.Cut(model, "/"); ok {
			return strings.TrimSpace(provider)
		}
		return model
	}
	config.NormalizeLegacyMimoCustomProvidersForRefs(cfg, model)
	entry, ok := cfg.ResolveModel(model)
	if !ok || entry == nil {
		if provider, _, ok := strings.Cut(model, "/"); ok {
			return strings.TrimSpace(provider)
		}
		return model
	}
	if strings.TrimSpace(entry.Name) != "" {
		return strings.TrimSpace(entry.Name)
	}
	if provider, _, ok := strings.Cut(model, "/"); ok {
		return strings.TrimSpace(provider)
	}
	return model
}

func normalizeMimoExecutionModelRef(workspace, model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	if strings.Contains(model, "/") {
		return model
	}
	if resolved := resolveInstalledMimoModelRef(model); resolved != "" {
		return resolved
	}

	cfg, err := config.LoadForRoot(workspace)
	if err != nil {
		return model
	}
	config.NormalizeLegacyMimoCustomProvidersForRefs(cfg, model)
	entry, ok := cfg.ResolveModel(model)
	if !ok || entry == nil {
		return model
	}
	if strings.HasPrefix(entry.Name, "mimo") {
		return entry.Name + "/" + entry.Model
	}
	return model
}

func resolveInstalledMimoModelRef(model string) string {
	model = strings.TrimSpace(model)
	if model == "" {
		return ""
	}
	models := listInstalledMimoModels()
	if len(models) == 0 {
		return ""
	}
	for _, ref := range models {
		if _, bare, ok := strings.Cut(ref, "/"); ok && strings.EqualFold(strings.TrimSpace(bare), model) {
			return ref
		}
	}
	for _, ref := range models {
		if strings.EqualFold(strings.TrimSpace(ref), model) {
			return ref
		}
	}
	return ""
}

func listInstalledMimoModels() []string {
	mimoModelsCache.mu.Lock()
	defer mimoModelsCache.mu.Unlock()
	if mimoModelsCache.loaded {
		return append([]string(nil), mimoModelsCache.models...)
	}
	cmd := exec.Command("mimo", "models")
	proc.HideWindow(cmd)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = io.Discard
	if err := cmd.Run(); err != nil {
		mimoModelsCache.loaded = true
		mimoModelsCache.models = nil
		return nil
	}
	var models []string
	for _, line := range strings.Split(strings.ReplaceAll(stdout.String(), "\r\n", "\n"), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !strings.Contains(line, "/") {
			continue
		}
		models = append(models, line)
	}
	mimoModelsCache.loaded = true
	mimoModelsCache.models = models
	return append([]string(nil), models...)
}

func reasonixRuntimeDir(workspace, key string) string {
	root := workspace
	if strings.TrimSpace(root) == "" {
		root = detectWorkspace()
	}
	safe := sanitizeFilename(key)
	if safe == "" {
		safe = fmt.Sprintf("rt-%d", time.Now().UnixNano())
	}
	return filepath.Join(root, ".reasonix-orchestrator", safe)
}

var absoluteWorkspacePathPattern = regexp.MustCompile(`(?i)(?:[a-z]:[\\/]|/)[^\r\n\t"'<>|]+`)

func resolveRunWorkspace(requested, sessionWorkspace string) string {
	return resolveRunWorkspaceForTask(requested, sessionWorkspace, "")
}

func resolveRunWorkspaceForTask(requested, sessionWorkspace, task string) string {
	if workspace := strings.TrimSpace(requested); workspace != "" {
		return filepath.Clean(workspace)
	}
	if workspace := strings.TrimSpace(sessionWorkspace); workspace != "" {
		return filepath.Clean(workspace)
	}
	if workspace := workspacePathFromTask(task); workspace != "" {
		return workspace
	}
	return detectWorkspace()
}

// workspacePathFromTask extracts only existing absolute directories. It is
// intentionally conservative: a path mentioned in prose is not a workspace
// unless it resolves to a directory on this machine. The longest matching
// directory wins, which handles prompts containing both a repository root and
// a deeper target project path.
func workspacePathFromTask(task string) string {
	best := ""
	for _, candidate := range absoluteWorkspacePathPattern.FindAllString(task, -1) {
		candidate = strings.TrimSpace(candidate)
		candidate = strings.Trim(candidate, "`")
		candidate = strings.TrimRight(candidate, ".,;:!?)]}，。；：！？）】")
		if candidate == "" {
			continue
		}
		candidate = filepath.Clean(candidate)
		// The prose after a Windows path is often separated by a space
		// (for example, "G:\project 中执行测试"). Trim trailing prose one
		// token at a time, but keep paths containing spaces when the complete
		// candidate already resolves successfully.
		for {
			info, err := os.Stat(candidate)
			if err == nil && info.IsDir() {
				if len(candidate) > len(best) {
					best = candidate
				}
				break
			}
			cut := strings.LastIndexAny(candidate, " \t")
			if cut < 0 {
				break
			}
			candidate = strings.TrimSpace(candidate[:cut])
		}
	}
	return best
}

func runWorkspace(run *PipelineRun) string {
	if run != nil {
		if workspace := strings.TrimSpace(run.ExecOptions.Workspace); workspace != "" {
			return workspace
		}
	}
	return detectWorkspace()
}

func detectWorkspace() string {
	if detectWorkspaceForTest != nil {
		return detectWorkspaceForTest()
	}
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	return filepath.Dir(os.Args[0])
}

func portFromEndpoint(endpoint string) int {
	parts := strings.Split(endpoint, ":")
	if len(parts) == 0 {
		return 0
	}
	var p int
	fmt.Sscanf(parts[len(parts)-1], "%d", &p)
	return p
}

func loadRunMetrics(path string) (*TokenUsage, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var metrics struct {
		PromptTokens     int     `json:"prompt_tokens"`
		CompletionTokens int     `json:"completion_tokens"`
		Cost             float64 `json:"cost"`
	}
	if err := json.Unmarshal(b, &metrics); err != nil {
		return nil, err
	}
	return &TokenUsage{
		InputTokens:  metrics.PromptTokens,
		OutputTokens: metrics.CompletionTokens,
		TotalTokens:  metrics.PromptTokens + metrics.CompletionTokens,
		CostYuan:     metrics.Cost,
		Source:       "reasonix-run-metrics",
	}, nil
}

// findReasonixBin locates the reasonix binary.
func findReasonixBin() string {
	bin := "reasonix"
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Dir(exe)
		bin = filepath.Join(bin, "reasonix.exe")
		if _, err := os.Stat(bin); err != nil {
			bin = "reasonix"
		}
	}
	return bin
}

// findFreePort finds an available TCP port.
func findFreePort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 8800 + rand.Intn(100) // fallback
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// waitForServer polls the /status endpoint until the server is ready.
func waitForServer(port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	url := fmt.Sprintf("http://127.0.0.1:%d/status", port)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("server on port %d not ready after %v", port, timeout)
}

// configureReasonixRuntime applies approvalMode and executionMode to a running
// reasonix serve instance via its HTTP API.
func configureReasonixRuntime(endpoint string, approvalMode, executionMode, taskPrompt string) {
	endpoint = strings.TrimRight(endpoint, "/")
	// 1. Set approval mode
	if approvalMode == "auto" || approvalMode == "yolo" {
		body, _ := json.Marshal(map[string]string{"mode": approvalMode})
		resp, err := http.Post(endpoint+"/tool-approval-mode", "application/json", bytes.NewReader(body))
		if err != nil {
			slog.Warn("orchestrator: failed to set approval mode", "endpoint", endpoint, "mode", approvalMode, "err", err)
		} else {
			resp.Body.Close()
			if resp.StatusCode >= 400 {
				slog.Warn("orchestrator: approval mode API returned error", "endpoint", endpoint, "mode", approvalMode, "status", resp.StatusCode)
			}
		}
	}
	// 2. Set or clear goal based on executionMode
	if executionMode == "goal" {
		goalText := extractGoalFromPrompt(taskPrompt)
		if goalText != "" {
			body, _ := json.Marshal(map[string]string{"goal": goalText})
			resp, err := http.Post(endpoint+"/goal", "application/json", bytes.NewReader(body))
			if err != nil {
				slog.Warn("orchestrator: failed to set goal", "endpoint", endpoint, "err", err)
			} else {
				resp.Body.Close()
				if resp.StatusCode >= 400 {
					slog.Warn("orchestrator: goal API returned error", "endpoint", endpoint, "status", resp.StatusCode)
				}
			}
		}
	} else {
		// Clear any previous goal when in task mode
		body, _ := json.Marshal(map[string]string{"goal": ""})
		resp, err := http.Post(endpoint+"/goal", "application/json", bytes.NewReader(body))
		if err != nil {
			slog.Warn("orchestrator: failed to clear goal", "endpoint", endpoint, "err", err)
		} else {
			resp.Body.Close()
		}
	}
}

// extractGoalFromPrompt extracts a short goal from a longer task prompt.
// Takes the first non-empty, non-boilerplate line as the goal text.
func extractGoalFromPrompt(prompt string) string {
	for _, line := range strings.Split(prompt, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "---") {
			continue
		}
		if strings.HasPrefix(line, "角色") || strings.HasPrefix(line, "##") {
			continue
		}
		if len([]rune(line)) > 200 {
			return string([]rune(line)[:200])
		}
		return line
	}
	return ""
}

// submitTask sends a task to a reasonix serve instance and waits for the result.
// Returns the output text and real usage data from the API.
func submitTask(ctx context.Context, port int, task string) (string, *TokenUsage, error) {
	// Subscribe to events before submitting. The serve API returns 202 and a
	// very fast turn can emit turn_done before a post-submit subscription is
	// established; that race used to make status polling wait until the node
	// timeout and then cancel the retained runtime.
	turnCtx, cancelTurn := context.WithCancel(ctx)
	defer cancelTurn()
	turnCh := make(chan error, 1)
	turnReady := make(chan struct{})
	go func() {
		turnCh <- waitForTurnDoneReady(turnCtx, port, 5*time.Minute, turnReady)
	}()
	select {
	case <-turnReady:
	case <-ctx.Done():
		return "", nil, ctx.Err()
	case <-time.After(2 * time.Second):
		// SSE is a best-effort signal path. Continue with /submit and /status
		// even if the event endpoint is unavailable or slow to connect.
	}

	// Submit the task
	body, _ := json.Marshal(map[string]string{"input": task})
	url := fmt.Sprintf("http://127.0.0.1:%d/submit", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return "", nil, fmt.Errorf("submit request failed: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", nil, fmt.Errorf("submit failed: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		msg, _ := io.ReadAll(resp.Body)
		return "", nil, fmt.Errorf("submit failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	resp.Body.Close()

	// Poll /status concurrently with the already-connected SSE watcher. The
	// old implementation waited up to five minutes for SSE first and then
	// another five minutes for /status. If the SSE event was missed or the
	// model took a long turn, the node context expired exactly when the final
	// agent had finished, causing the runtime to be stopped and the UI to show
	// "reconnecting". /status is the completion authority; the SSE watcher is
	// kept alive only to auto-answer ask requests and capture turn errors.
	var turnErr error
	turnDone := false
	statusURL := fmt.Sprintf("http://127.0.0.1:%d/status", port)
	deadline := time.Now().Add(9 * time.Minute)
	if ctxDeadline, ok := ctx.Deadline(); ok {
		// Leave a small window for the history request before the node context
		// expires; do not spend the whole node budget in status polling.
		if d := ctxDeadline.Add(-2 * time.Second); d.Before(deadline) {
			deadline = d
		}
	}
	var usage *TokenUsage
	statusCtxErr := error(nil)
	statusDone := false
	statusStarted := false
	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
			statusCtxErr = ctx.Err()
			break
		}

		// Handle any event result without making it a second serial wait. A
		// normal turn_done does not itself prove that history is fully flushed,
		// so we still confirm idle through /status below.
		select {
		case err := <-turnCh:
			turnErr = err
			turnCh = nil
			if err == nil {
				turnDone = true
				statusStarted = true
			}
		default:
		}

		timer := time.NewTimer(1 * time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			statusCtxErr = ctx.Err()
		case err := <-turnCh:
			timer.Stop()
			turnErr = err
			turnCh = nil
			if err == nil {
				turnDone = true
				statusStarted = true
			}
		case <-timer.C:
		}
		if statusCtxErr != nil {
			break
		}

		statusReq, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, statusURL, nil)
		if reqErr != nil {
			continue
		}
		resp, err := http.DefaultClient.Do(statusReq)
		if err != nil {
			if ctx.Err() != nil {
				statusCtxErr = ctx.Err()
				break
			}
			continue
		}
		var status struct {
			// The serve /status API exposes `running` (not `status`). Keep the
			// legacy string for older runtimes/tests, but never interpret a
			// missing field as idle: an omitted field is not proof that the
			// asynchronously submitted turn has finished.
			Running   *bool  `json:"running"`
			Status    string `json:"status"`
			LastUsage *struct {
				PromptTokens     int `json:"promptTokens"`
				CompletionTokens int `json:"completionTokens"`
				TotalTokens      int `json:"totalTokens"`
				CacheHitTokens   int `json:"cacheHitTokens"`
				CacheMissTokens  int `json:"cacheMissTokens"`
			} `json:"lastUsage"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
			resp.Body.Close()
			continue
		}
		resp.Body.Close()

		statusKnown := status.Running != nil || strings.TrimSpace(status.Status) != ""
		running := false
		if status.Running != nil {
			running = *status.Running
		} else {
			running = strings.EqualFold(strings.TrimSpace(status.Status), "running")
		}
		if running {
			// /submit is asynchronous. Seeing running=true is the
			// acknowledgement that this request, rather than the previous
			// idle state, has actually been admitted by the controller.
			statusStarted = true
			continue
		}
		if statusKnown && (statusStarted || turnDone) {
			statusDone = true
			if status.LastUsage != nil {
				usage = &TokenUsage{
					InputTokens:  status.LastUsage.PromptTokens,
					OutputTokens: status.LastUsage.CompletionTokens,
					TotalTokens:  status.LastUsage.TotalTokens,
				}
			}
			break
		}
	}
	if statusCtxErr == nil && !statusDone {
		statusCtxErr = fmt.Errorf("agent status polling timed out after 9m")
	}

	// Get the session history to extract the last assistant message
	historyURL := fmt.Sprintf("http://127.0.0.1:%d/history", port)
	historyCtx, cancelHistory := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelHistory()
	historyReq, reqErr := http.NewRequestWithContext(historyCtx, http.MethodGet, historyURL, nil)
	if reqErr != nil {
		return "", usage, fmt.Errorf("history request failed: %w", reqErr)
	}
	hresp, err := http.DefaultClient.Do(historyReq)
	if err != nil {
		return "", usage, fmt.Errorf("history fetch failed: %w", err)
	}
	defer hresp.Body.Close()

	var messages []struct {
		Role      string `json:"role"`
		Content   string `json:"content"`
		Reasoning string `json:"reasoning,omitempty"`
	}
	if err := json.NewDecoder(hresp.Body).Decode(&messages); err != nil {
		if turnErr != nil {
			return "", usage, fmt.Errorf("turn failed: %w; history decode failed: %v", turnErr, err)
		}
		if statusCtxErr != nil {
			return "", usage, fmt.Errorf("status wait failed: %w; history decode failed: %v", statusCtxErr, err)
		}
		return "", usage, fmt.Errorf("history decode failed: %w", err)
	}

	// Find the last assistant message
	var output string
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role != "assistant" {
			continue
		}
		if strings.TrimSpace(messages[i].Content) != "" {
			output = messages[i].Content
			break
		}
		if strings.TrimSpace(messages[i].Reasoning) != "" {
			output = messages[i].Reasoning
			break
		}
	}
	if turnErr != nil {
		// Some reasonix serve runs can produce a valid final assistant message in
		// history even when the events stream ends without an explicit turn_done.
		// In that case, prefer the real output instead of surfacing a misleading
		// timeout error for an otherwise successful node.
		if strings.TrimSpace(output) != "" {
			return output, usage, nil
		}
		return output, usage, turnErr
	}
	if statusCtxErr != nil {
		if strings.TrimSpace(output) != "" {
			return output, usage, nil
		}
		return output, usage, statusCtxErr
	}
	return output, usage, nil
}

func waitForTurnDone(ctx context.Context, port int, timeout time.Duration) error {
	return waitForTurnDoneReady(ctx, port, timeout, nil)
}

// waitForTurnDoneReady is the SSE watcher used by submitTask. ready is closed
// after the HTTP stream is successfully subscribed, allowing callers to submit
// an asynchronous turn without racing a fast turn_done event.
func waitForTurnDoneReady(ctx context.Context, port int, timeout time.Duration, ready chan<- struct{}) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	url := fmt.Sprintf("http://127.0.0.1:%d/events", port)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("events stream failed: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	if ready != nil {
		close(ready)
	}

	s := bufio.NewScanner(resp.Body)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, ":") || !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
		if payload == "" {
			continue
		}
		var evt struct {
			Kind string `json:"kind"`
			Err  string `json:"err,omitempty"`
			Ask  *struct {
				ID        string `json:"id"`
				Questions []struct {
					ID      string `json:"id"`
					Options []struct {
						Label string `json:"label"`
					} `json:"options"`
				} `json:"questions"`
			} `json:"ask,omitempty"`
		}
		if err := json.Unmarshal([]byte(payload), &evt); err != nil {
			continue
		}
		switch evt.Kind {
		case "turn_done":
			if strings.TrimSpace(evt.Err) != "" {
				return fmt.Errorf("%s", strings.TrimSpace(evt.Err))
			}
			return nil
		case "ask_request":
			// Auto-answer: select the first option for each question.
			if evt.Ask != nil {
				autoAnswerAsk(ctx, port, evt.Ask.ID, evt.Ask.Questions)
			}
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("events stream ended before turn_done")
}

// autoAnswerAsk sends an automatic answer to an ask_request in headless mode.
// It selects the first option for each question.
func autoAnswerAsk(ctx context.Context, port int, askID string, questions []struct {
	ID      string `json:"id"`
	Options []struct {
		Label string `json:"label"`
	} `json:"options"`
}) {
	type askAnswer struct {
		QuestionID string   `json:"questionId"`
		Selected   []string `json:"selected"`
	}
	type answerBody struct {
		ID      string      `json:"id"`
		Answers []askAnswer `json:"answers"`
	}

	var answers []askAnswer
	for _, q := range questions {
		if len(q.Options) > 0 {
			answers = append(answers, askAnswer{
				QuestionID: q.ID,
				Selected:   []string{q.Options[0].Label},
			})
		}
	}

	body, _ := json.Marshal(answerBody{ID: askID, Answers: answers})
	url := fmt.Sprintf("http://127.0.0.1:%d/answer", port)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		return
	}
	resp.Body.Close()
}

func simulateAgentCall(node *AgentNode, input string) (string, error) {
	time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)
	return fmt.Sprintf("[%s] Processed by %s:\n%s", node.Type, node.Label, input), nil
}

// getModelPrices returns per-token prices (yuan) for input and output based on model name.
// understandTask uses Flash to analyze the requirement and optimize node roles.
// Returns a structured requirement document.
func (s *Store) understandTask(ctx context.Context, task string, pipe *Pipeline) string {
	nodesDesc := ""
	for _, n := range pipe.Nodes {
		role := n.RoleDesc
		if role == "" {
			role = "(未设置)"
		}
		nodesDesc += fmt.Sprintf("- %s (%s): %s\n", n.Label, n.Type, role)
	}

	prompt := `你是一个需求理解助手。请分析以下需求，输出结构化的规范文档。

需求：
` + task + `

协作节点：
` + nodesDesc + `

请输出以下格式（纯文本，不要JSON）：

## 需求规范

### 背景
[为什么要做这个]

### 目标
[要实现什么]

### 约束
[限制条件]

### 验收标准
[怎么算完成]

### 各节点职责

[为每个节点写出具体、可执行的职责描述，告诉它该做什么、输出什么格式]
`

	bin := findReasonixBin()
	cmd := exec.CommandContext(ctx, bin, "run", "--model", "deepseek-flash", prompt)
	proc.HideWindow(cmd)
	var stdout bytes.Buffer
	cmd.Stdout = &stdout

	if err := cmd.Run(); err != nil {
		slog.Warn("understandTask: Flash failed, using original task", "err", err)
		return task
	}

	result := strings.TrimSpace(stdout.String())
	if result == "" {
		return task
	}
	return result
}

func mustParseTime(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, s)
	return t
}

// loadSkillContent resolves the installed/versioned Skill catalog and returns
// bounded content for prompt injection. Keeping this wrapper preserves the
// existing executor call sites while fixing the old non-versioned path lookup.
func loadSkillContent(skillName string) (string, error) {
	_, content, err := LoadSkill(skillName)
	return content, err
}
