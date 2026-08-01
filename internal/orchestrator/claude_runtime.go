package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	claudeclient "reasonix/internal/executor/claude"
)

// claudeRuntime is the retained `claude -p --input-format stream-json` runtime
// state. The Claude Agent SDK protocol runs over the child's stdin/stdout
// pipes (newline-delimited JSON). One runtime hosts one retained Claude
// conversation; the session id reported by the init event is persisted as the
// provider session identity for cross-process resume via `--resume`.
type claudeRuntime struct {
	ID            string
	Endpoint      string
	NodeID        string
	ModelRef      string
	DisplayModel  string
	ProviderRoute string
	Workspace     string
	Agent         string
	ApprovalMode  string
	Cmd           *exec.Cmd
	stdinW        *os.File
	stdoutR       *os.File
	Stderr        *bytes.Buffer
	done          chan struct{}
	StartedAt     time.Time
	LastUsedAt    time.Time

	mu        sync.Mutex
	client    *claudeclient.SdkClient
	sessionID string
	turnID    string
	status    RuntimeStatus
	output    string
	lastErr   string
	events    []RuntimeConsoleEvent
	stream    *consoleStreamCoalescer
}

// ClaudeRuntimeManager starts `claude -p` as a retained, loopback-only runtime
// and speaks the Agent SDK stream-json protocol over its stdio pipes.
type ClaudeRuntimeManager struct {
	mu       sync.Mutex
	runtimes map[string]*claudeRuntime
	onUpdate func(RuntimeState)
}

func newClaudeRuntimeManager() *ClaudeRuntimeManager {
	return &ClaudeRuntimeManager{runtimes: make(map[string]*claudeRuntime)}
}

// SetUpdateSink registers the process-local bridge used to wake the browser
// Runtime Console through Orchestrator SSE.
func (m *ClaudeRuntimeManager) SetUpdateSink(sink func(RuntimeState)) {
	m.mu.Lock()
	m.onUpdate = sink
	m.mu.Unlock()
}

func (m *ClaudeRuntimeManager) notify(rt *claudeRuntime) {
	m.mu.Lock()
	sink := m.onUpdate
	m.mu.Unlock()
	if sink == nil {
		return
	}
	state := m.stateFor(rt, ExecSpec{}, CleanupRetained)
	sink(*state)
}

func (m *ClaudeRuntimeManager) runtimeKey(spec ExecSpec) string {
	workspace := spec.Workspace
	if workspace == "" {
		workspace = "."
	}
	return spec.NodeID + "|" + spec.ModelRef + "|" + workspace + "|" + spec.ProviderRoute
}

func (m *ClaudeRuntimeManager) Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error) {
	rt, err := m.ensure(ctx, spec, nil)
	if err != nil {
		return nil, err
	}
	return m.stateFor(rt, spec, policy), nil
}

func (m *ClaudeRuntimeManager) Release(runtimeID string) error {
	m.mu.Lock()
	var target *claudeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("claude runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	if target.status != RuntimeBusy {
		target.status = RuntimeIdle
	}
	target.LastUsedAt = time.Now()
	target.mu.Unlock()
	m.notify(target)
	return nil
}

func (m *ClaudeRuntimeManager) Stop(runtimeID string) error {
	m.mu.Lock()
	var target *claudeRuntime
	var key string
	for k, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target, key = rt, k
			break
		}
	}
	if target != nil {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("claude runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	target.status = RuntimeStopped
	client := target.client
	target.client = nil
	stream := target.stream
	target.stream = nil
	target.mu.Unlock()
	if stream != nil {
		stream.stop()
	}
	if client != nil {
		client.Close()
	}
	if target.Cmd != nil {
		stopRuntimeProcess(target.Cmd, target.done)
	}
	if target.stdoutR != nil {
		_ = target.stdoutR.Close()
	}
	m.notify(target)
	return nil
}

func (m *ClaudeRuntimeManager) Get(runtimeID string) (*RuntimeState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			return m.stateFor(rt, ExecSpec{NodeID: ""}, CleanupRetained), true
		}
	}
	return nil, false
}

func (m *ClaudeRuntimeManager) List() []*RuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*RuntimeState, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		out = append(out, m.stateFor(rt, ExecSpec{}, CleanupRetained))
	}
	return out
}

// cleanupAll kills all managed claude processes and clears the registry.
func (m *ClaudeRuntimeManager) cleanupAll() {
	m.mu.Lock()
	runtimes := make([]*claudeRuntime, 0, len(m.runtimes))
	for key, rt := range m.runtimes {
		delete(m.runtimes, key)
		runtimes = append(runtimes, rt)
	}
	m.mu.Unlock()
	for _, rt := range runtimes {
		rt.mu.Lock()
		client := rt.client
		rt.client = nil
		stream := rt.stream
		rt.stream = nil
		rt.mu.Unlock()
		if stream != nil {
			stream.stop()
		}
		if client != nil {
			client.Close()
		}
		stopRuntimeProcess(rt.Cmd, rt.done)
		if rt.stdoutR != nil {
			_ = rt.stdoutR.Close()
		}
	}
}

func (m *ClaudeRuntimeManager) stateFor(rt *claudeRuntime, spec ExecSpec, policy CleanupPolicy) *RuntimeState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	nodeID := rt.NodeID
	if nodeID == "" {
		nodeID = spec.NodeID
	}
	model := rt.DisplayModel
	if model == "" {
		model = spec.DisplayModel
	}
	if model == "" {
		model = rt.ModelRef
	}
	if model == "" {
		model = spec.ModelRef
	}
	pid := 0
	if rt.Cmd != nil && rt.Cmd.Process != nil {
		pid = rt.Cmd.Process.Pid
	}
	return &RuntimeState{
		RuntimeID: rt.ID, NodeID: nodeID, Executor: string(ExecutorClaude), Model: model,
		Endpoint: rt.Endpoint, Port: 0, PID: pid, Status: rt.status, Error: rt.lastErr,
		CreatedAt: rt.StartedAt, LastActiveAt: rt.LastUsedAt, CleanupPolicy: policy,
		AccessMode: "runtime_console", ApprovalMode: rt.ApprovalMode,
		ThreadID: rt.sessionID, TurnID: rt.turnID,
	}
}

func (m *ClaudeRuntimeManager) ensure(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*claudeRuntime, error) {
	key := m.runtimeKey(spec)
	m.mu.Lock()
	if rt := m.runtimes[key]; rt != nil {
		rt.mu.Lock()
		live := rt.client != nil && rt.status != RuntimeStopped && rt.status != RuntimeError
		rt.LastUsedAt = time.Now()
		rt.mu.Unlock()
		if live {
			if spec.NodeID != "" {
				rt.NodeID = spec.NodeID
			}
			if spec.DisplayModel != "" {
				rt.DisplayModel = spec.DisplayModel
			}
			if spec.ApprovalMode != "" {
				rt.ApprovalMode = spec.ApprovalMode
			}
			m.mu.Unlock()
			return rt, nil
		}
		delete(m.runtimes, key)
	}
	rt := &claudeRuntime{
		ID:            fmt.Sprintf("claude_rt_%d", time.Now().UnixNano()),
		Endpoint:      "stdio://claude",
		NodeID:        spec.NodeID,
		ModelRef:      spec.ModelRef,
		DisplayModel:  spec.DisplayModel,
		ProviderRoute: spec.ProviderRoute,
		Workspace:     spec.Workspace,
		Agent:         spec.Agent,
		ApprovalMode:  spec.ApprovalMode,
		StartedAt:     time.Now(),
		LastUsedAt:    time.Now(),
		done:          make(chan struct{}),
		status:        RuntimeStarting,
	}
	m.runtimes[key] = rt
	m.mu.Unlock()

	rt.stream = newConsoleStreamCoalescer(0, func(evt RuntimeConsoleEvent) {
		rt.mu.Lock()
		rt.events = append(rt.events, evt)
		if len(rt.events) > 300 {
			rt.events = append([]RuntimeConsoleEvent(nil), rt.events[len(rt.events)-300:]...)
		}
		rt.mu.Unlock()
		m.notify(rt)
	})

	args := []string{"-p", "--input-format", "stream-json", "--output-format", "stream-json", "--verbose", "--include-partial-messages"}
	if model := claudeRuntimeModel(spec); model != "" {
		args = append(args, "--model", model)
	}
	if resume := strings.TrimSpace(spec.ExternalSessionID); resume != "" {
		args = append(args, "--resume", resume)
	}
	// --permission-mode is intentionally not passed at spawn: in some CLI
	// builds it delays the init event, and permission decisions belong to the
	// SDK control protocol (EnablePermissionProtocol + the node policy), the
	// same way mimo answers ACP requestPermission and codex its WS approvals.
	bin := claudeBinary()
	cmd := newRetainedRuntimeCommand(ctx, bin, args...)
	if strings.TrimSpace(spec.Workspace) != "" {
		cmd.Dir = spec.Workspace
	}
	if configDir := claudeConfigDir(spec); configDir != "" {
		// Pin the node's own settings.json (e.g. ~/.claude-deepseek for the
		// DeepSeek official endpoint) so ccs/proxy nodes keep the default
		// ~/.claude profile. The key never lives in the repository.
		cmd.Env = append(os.Environ(), "CLAUDE_CONFIG_DIR="+configDir)
	}
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("claude stdin pipe: %w", err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("claude stdout pipe: %w", err)
	}
	cmd.Stdin = stdinR
	cmd.Stdout = stdoutW
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		_ = stdoutR.Close()
		_ = stdoutW.Close()
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("start claude: %w", err)
	}
	_ = stdinR.Close()
	_ = stdoutW.Close()
	rt.Cmd = cmd
	rt.stdinW = stdinW
	rt.stdoutR = stdoutR
	rt.Stderr = stderr
	go m.watchRuntime(key, rt)
	if onStart != nil {
		onStart(rt.Endpoint, 0)
	}

	client := claudeclient.NewSdkClient(stdoutR, stdinW, func(e claudeclient.Event) { m.recordEvent(rt, e) })
	client.SetPermissionPolicy(claudePermissionPolicy(rt))
	rt.mu.Lock()
	rt.client = client
	rt.mu.Unlock()

	// The CLI needs time to boot (config, plugins) and the init event reports
	// the retained session id. Provider API unavailability shows up as
	// system/api_retry events on the console; the runtime itself stays usable.
	deadline := time.Now().Add(120 * time.Second)
	for {
		if client.SessionID() != "" {
			// Enable tool-approval routing once the CLI is up so the retained
			// runtime can actually edit files / run commands in auto mode.
			_ = client.EnablePermissionProtocol()
			rt.mu.Lock()
			rt.sessionID = client.SessionID()
			rt.status = RuntimeIdle
			rt.LastUsedAt = time.Now()
			rt.mu.Unlock()
			m.notify(rt)
			return rt, nil
		}
		if time.Now().After(deadline) {
			diagnostic := strings.TrimSpace(stderr.String())
			if diagnostic == "" {
				diagnostic = "no init event within 120s (provider API may be unavailable; see ~/.claude/debug for CLI logs)"
			}
			m.Stop(rt.ID)
			return nil, fmt.Errorf("claude did not emit init within 120s: %s", diagnostic)
		}
		select {
		case <-ctx.Done():
			m.Stop(rt.ID)
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
}

func (m *ClaudeRuntimeManager) dropRuntime(key string, rt *claudeRuntime) {
	m.mu.Lock()
	if current := m.runtimes[key]; current == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
}

func (m *ClaudeRuntimeManager) watchRuntime(key string, rt *claudeRuntime) {
	err := rt.Cmd.Wait()
	rt.mu.Lock()
	if rt.status != RuntimeStopped {
		rt.status = RuntimeError
		if err != nil {
			rt.lastErr = err.Error()
		} else {
			rt.lastErr = "claude exited"
		}
	}
	client := rt.client
	stream := rt.stream
	rt.stream = nil
	rt.client = nil
	rt.mu.Unlock()
	if stream != nil {
		stream.stop()
	}
	if client != nil {
		client.Close()
	}
	if rt.stdoutR != nil {
		_ = rt.stdoutR.Close()
	}
	m.notify(rt)
	m.mu.Lock()
	if m.runtimes[key] == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	close(rt.done)
}

func (m *ClaudeRuntimeManager) recordEvent(rt *claudeRuntime, event claudeclient.Event) {
	// Agent SDK stream deltas (text/thinking) are coalesced into one readable
	// console block per session; everything else is a flush boundary.
	if rt.stream != nil {
		if method, key, category, ok := claudeStreamPart(event); ok {
			rt.stream.append(method, key, category, event.Text+event.Reasoning)
			return
		}
		rt.stream.flushNow()
	}
	rt.mu.Lock()
	level := "info"
	if strings.Contains(event.Type, "error") || strings.Contains(event.Subtype, "error") {
		level = "error"
	}
	text := event.Text
	if text == "" {
		text = event.Reasoning
	}
	rt.events = append(rt.events, RuntimeConsoleEvent{At: event.At, Level: level, Method: event.Type + "/" + event.Subtype, Text: text, Payload: event.Payload})
	if len(rt.events) > 300 {
		rt.events = append([]RuntimeConsoleEvent(nil), rt.events[len(rt.events)-300:]...)
	}
	rt.mu.Unlock()
	m.notify(rt)
}

// claudeStreamPart classifies an Agent SDK event as a streaming delta.
// text_delta is the assistant answer; thinking_delta is reasoning.
func claudeStreamPart(event claudeclient.Event) (method, key, category string, isDelta bool) {
	if !event.IsDelta {
		return "", "", "", false
	}
	if event.Text != "" {
		return "claude_message", event.SessionID, "assistant", true
	}
	if event.Reasoning != "" {
		return "claude_thought", event.SessionID, "reasoning", true
	}
	return "", "", "", false
}

// Execute runs one orchestration node turn against the retained claude
// session.
func (m *ClaudeRuntimeManager) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if spec.ContextPolicy == "fresh" || spec.ContextPolicy == "fresh_per_run" {
		// A fresh context needs a brand-new conversation, which the Claude CLI
		// only supports by respawning without --resume.
		spec.ExternalSessionID = ""
		key := m.runtimeKey(spec)
		m.mu.Lock()
		old := m.runtimes[key]
		m.mu.Unlock()
		if old != nil {
			_ = m.Stop(old.ID)
		}
	}
	rt, err := m.ensure(ctx, spec, onStart)
	if err != nil {
		return &ExecResult{ExitCode: -1}, err
	}
	client, err := m.reserveTurn(rt)
	if err != nil {
		return &ExecResult{RuntimeID: rt.ID, Endpoint: rt.Endpoint, ExitCode: -1}, err
	}

	sessionID, err := m.prepareSession(ctx, rt, client, spec)
	if err != nil {
		m.finishTurn(rt, sessionID, err)
		return m.execResult(rt, "", sessionID), err
	}
	result, promptErr := client.Prompt(ctx, spec.Prompt)
	rt.mu.Lock()
	if result != nil {
		rt.output = result.Text
	}
	rt.mu.Unlock()
	if promptErr == nil && result != nil && result.IsError {
		promptErr = fmt.Errorf("claude turn failed: %s", result.Error)
	}
	if promptErr != nil {
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	if strings.TrimSpace(result.Text) == "" {
		promptErr = fmt.Errorf("claude turn completed without assistant output")
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	m.finishTurn(rt, sessionID, nil)
	execResult := m.execResult(rt, result.Text, sessionID)
	if result.Usage != nil {
		execResult.TokenUsage = &TokenUsage{
			InputTokens:  int(result.Usage.InputTokens),
			OutputTokens: int(result.Usage.OutputTokens),
			TotalTokens:  int(result.Usage.InputTokens + result.Usage.OutputTokens),
		}
	}
	return execResult, nil
}

// prepareSession resolves the session used by one turn. The retained process
// owns exactly one conversation, so the init-reported session id (or the
// persisted ExternalSessionID the process was resumed from) is reused; a
// mismatch is impossible because the runtime key pins one node/workspace/model.
func (m *ClaudeRuntimeManager) prepareSession(_ context.Context, rt *claudeRuntime, _ *claudeclient.SdkClient, spec ExecSpec) (string, error) {
	rt.mu.Lock()
	sessionID := strings.TrimSpace(rt.sessionID)
	rt.mu.Unlock()
	if sessionID == "" {
		sessionID = strings.TrimSpace(spec.ExternalSessionID)
	}
	if sessionID == "" {
		return "", fmt.Errorf("claude runtime has no session yet")
	}
	return sessionID, nil
}

// reserveTurn atomically reserves the retained claude runtime for exactly one
// active prompt turn.
func (m *ClaudeRuntimeManager) reserveTurn(rt *claudeRuntime) (*claudeclient.SdkClient, error) {
	rt.mu.Lock()
	if rt.client == nil {
		rt.mu.Unlock()
		return nil, fmt.Errorf("claude runtime lost its sdk connection")
	}
	if rt.status == RuntimeBusy {
		rt.mu.Unlock()
		return nil, fmt.Errorf("claude runtime already has an active turn")
	}
	if rt.status == RuntimeStopped {
		rt.mu.Unlock()
		return nil, fmt.Errorf("claude runtime is stopped")
	}
	if rt.status == RuntimeError {
		errText := rt.lastErr
		rt.mu.Unlock()
		if errText == "" {
			errText = "runtime is in an error state"
		}
		return nil, fmt.Errorf("claude runtime is unavailable: %s", errText)
	}
	client := rt.client
	rt.status = RuntimeBusy
	rt.turnID = ""
	rt.LastUsedAt = time.Now()
	rt.output = ""
	rt.lastErr = ""
	rt.mu.Unlock()
	m.notify(rt)
	return client, nil
}

func (m *ClaudeRuntimeManager) finishTurn(rt *claudeRuntime, sessionID string, err error) {
	rt.mu.Lock()
	rt.sessionID = sessionID
	rt.turnID = ""
	rt.LastUsedAt = time.Now()
	if errors.Is(err, claudeclient.ErrTurnInterrupted) {
		rt.status = RuntimeIdle
		rt.lastErr = ""
	} else if err != nil {
		rt.status = RuntimeError
		rt.lastErr = err.Error()
	} else {
		rt.status = RuntimeIdle
		rt.lastErr = ""
	}
	rt.mu.Unlock()
	m.notify(rt)
}

func (m *ClaudeRuntimeManager) execResult(rt *claudeRuntime, text, sessionID string) *ExecResult {
	rt.mu.Lock()
	rt.output = text
	stderr := rt.lastErr
	rt.mu.Unlock()
	return &ExecResult{FinalText: text, RawStdout: text, RawStderr: stderr, RuntimeID: rt.ID, Endpoint: rt.Endpoint, ExternalSessionID: sessionID}
}

// Snapshot returns the server-proxied console state for one runtime.
func (m *ClaudeRuntimeManager) Snapshot(runtimeID string) (*RuntimeConsoleSnapshot, bool) {
	m.mu.Lock()
	var target *claudeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return nil, false
	}
	state := m.stateFor(target, ExecSpec{}, CleanupRetained)
	target.mu.Lock()
	defer target.mu.Unlock()
	events := append([]RuntimeConsoleEvent(nil), target.events...)
	return &RuntimeConsoleSnapshot{
		Runtime:      *state,
		ThreadID:     target.sessionID,
		TurnID:       target.turnID,
		Output:       target.output,
		Error:        target.lastErr,
		Events:       events,
		CanSend:      target.status == RuntimeIdle && target.sessionID != "",
		CanInterrupt: target.status == RuntimeBusy,
	}, true
}

// Send runs one manual Runtime Console turn against the retained session. The
// turn is orchestrator-only: it never creates a PipelineRun, Attempt or
// Iteration and is recorded as orchestrator/manual_turn.
func (m *ClaudeRuntimeManager) Send(ctx context.Context, runtimeID, text string) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("message must not be empty")
	}
	m.mu.Lock()
	var rt *claudeRuntime
	for _, candidate := range m.runtimes {
		if candidate.ID == runtimeID {
			rt = candidate
			break
		}
	}
	m.mu.Unlock()
	if rt == nil {
		return "", fmt.Errorf("claude runtime %q not found", runtimeID)
	}
	client, err := m.reserveTurn(rt)
	if err != nil {
		return "", err
	}
	rt.mu.Lock()
	sessionID := rt.sessionID
	rt.mu.Unlock()
	if sessionID == "" {
		err := fmt.Errorf("claude runtime has no session yet; run the node first")
		m.finishTurn(rt, "", err)
		return "", err
	}
	turnID := fmt.Sprintf("turn_%d", time.Now().UnixNano())
	rt.mu.Lock()
	rt.turnID = turnID
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.recordEvent(rt, claudeclient.Event{At: time.Now(), Type: "orchestrator/manual_turn", Text: text})
	m.notify(rt)
	go func() {
		turnCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		result, promptErr := client.Prompt(turnCtx, text)
		rt.mu.Lock()
		if result != nil {
			rt.output = result.Text
		}
		rt.mu.Unlock()
		m.finishTurn(rt, sessionID, promptErr)
	}()
	return turnID, nil
}

// Interrupt cancels the active turn via the Agent SDK control protocol. The
// retained session survives the interruption.
func (m *ClaudeRuntimeManager) Interrupt(ctx context.Context, runtimeID string) error {
	m.mu.Lock()
	var rt *claudeRuntime
	for _, candidate := range m.runtimes {
		if candidate.ID == runtimeID {
			rt = candidate
			break
		}
	}
	m.mu.Unlock()
	if rt == nil {
		return fmt.Errorf("claude runtime %q not found", runtimeID)
	}
	rt.mu.Lock()
	client, active := rt.client, rt.status == RuntimeBusy
	rt.mu.Unlock()
	if client == nil || !active {
		return fmt.Errorf("claude runtime has no active turn")
	}
	return client.Interrupt()
}

// claudePermissionPolicy answers Agent SDK permission requests the way the
// node's approval mode expects: ask denies in the non-interactive
// orchestrator, auto allows the tool call.
func claudePermissionPolicy(rt *claudeRuntime) claudeclient.PermissionPolicy {
	return func(_ string, _ string, _ json.RawMessage) (bool, error) {
		rt.mu.Lock()
		approval := rt.ApprovalMode
		rt.mu.Unlock()
		if strings.EqualFold(approval, "ask") {
			return false, nil
		}
		return true, nil
	}
}

// claudeConfigDir picks the CLAUDE_CONFIG_DIR overlay for a node.
//
//   - model starts with deepseek: the dedicated DeepSeek official config
//     directory (~/.claude-deepseek by default, override with the
//     CLAUDE_DEEPSEEK_CONFIG_DIR environment variable). That directory's
//     settings.json carries ANTHROPIC_BASE_URL=https://api.deepseek.com/anthropic
//     and the machine-local API key; it is never stored in the repository.
//   - ccs/ccswitch routes and other models: empty -> the default ~/.claude
//     settings (right.codes / cc-switch proxy) is used.
func claudeConfigDir(spec ExecSpec) string {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(spec.ModelRef)), "deepseek") {
		return ""
	}
	if dir := strings.TrimSpace(os.Getenv("CLAUDE_DEEPSEEK_CONFIG_DIR")); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".claude-deepseek")
}

// claudeRuntimeModel omits --model when the node routes through CCSwitch (the
// CLI then uses its own configured provider/model); otherwise the node model
// is passed through verbatim so self-configured models (aliases or full names)
// reach the CLI.
func claudeRuntimeModel(spec ExecSpec) string {
	if strings.EqualFold(spec.ProviderRoute, "ccswitch") || strings.EqualFold(spec.ModelRef, "ccs") || strings.EqualFold(spec.ModelRef, "ccswitch") {
		return ""
	}
	return strings.TrimSpace(spec.ModelRef)
}

// claudeBinaryOverride is a test seam for the retained-runtime integration
// tests; production code always resolves the installed CLI.
var claudeBinaryOverride string

// claudeBinary resolves the native Claude Code CLI binary.
func claudeBinary() string {
	if claudeBinaryOverride != "" {
		return claudeBinaryOverride
	}
	if bin := claudeclient.DiscoverBin(); bin != "" {
		return bin
	}
	return "claude"
}

var claudeRuntimeMgr = newClaudeRuntimeManager()

func ListClaudeRuntimes() []*RuntimeState              { return claudeRuntimeMgr.List() }
func GetClaudeRuntime(id string) (*RuntimeState, bool) { return claudeRuntimeMgr.Get(id) }
func StopClaudeRuntime(id string) error                { return claudeRuntimeMgr.Stop(id) }
func GetClaudeRuntimeConsole(id string) (*RuntimeConsoleSnapshot, bool) {
	return claudeRuntimeMgr.Snapshot(id)
}
func SendClaudeRuntimeMessage(ctx context.Context, id, text string) (string, error) {
	return claudeRuntimeMgr.Send(ctx, id, text)
}
func InterruptClaudeRuntime(ctx context.Context, id string) error {
	return claudeRuntimeMgr.Interrupt(ctx, id)
}
