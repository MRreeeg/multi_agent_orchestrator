package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	mimoclient "reasonix/internal/executor/mimo"
)

// mimoRuntime is the retained `mimo acp` runtime state. The ACP protocol runs
// over the child's stdin/stdout pipes (newline-delimited JSON-RPC 2.0); the
// port hosts mimo's internal HTTP/SSE server and doubles as a readiness signal.
type mimoRuntime struct {
	ID           string
	Port         int
	Endpoint     string
	NodeID       string
	ModelRef     string
	Workspace    string
	Agent        string
	ApprovalMode string
	Cmd          *exec.Cmd
	stdinW       *os.File
	stdoutR      *os.File
	Stderr       *bytes.Buffer
	done         chan struct{}
	StartedAt    time.Time
	LastUsedAt   time.Time

	mu        sync.Mutex
	client    *mimoclient.AcpClient
	clock     *activityClock
	sessionID string
	turnID    string
	status    RuntimeStatus
	output    string
	lastErr   string
	events    []RuntimeConsoleEvent
	stream    *consoleStreamCoalescer

	// pendingPerms holds tool-approval prompts parked in "ask" mode, keyed by
	// the JSON-RPC env id (as string). The Runtime Console answers them
	// through AnswerMimoRuntimePermission.
	pendingPerms map[string]PermissionRequestInfo
}

// MimoRuntimeManager starts `mimo acp` as a retained, loopback-only runtime
// and speaks ACP over its stdio pipes. One runtime can host one retained mimo
// session; the ProviderSession's ExternalSessionID remains the source of
// session identity across processes.
type MimoRuntimeManager struct {
	mu       sync.Mutex
	runtimes map[string]*mimoRuntime
	onUpdate func(RuntimeState)
}

func newMimoRuntimeManager() *MimoRuntimeManager {
	return &MimoRuntimeManager{runtimes: make(map[string]*mimoRuntime)}
}

// SetUpdateSink registers the process-local bridge used to wake the browser
// Runtime Console through Orchestrator SSE. It receives metadata only; raw
// provider messages stay readable through the authenticated console API.
func (m *MimoRuntimeManager) SetUpdateSink(sink func(RuntimeState)) {
	m.mu.Lock()
	m.onUpdate = sink
	m.mu.Unlock()
}

func (m *MimoRuntimeManager) notify(rt *mimoRuntime) {
	m.mu.Lock()
	sink := m.onUpdate
	m.mu.Unlock()
	if sink == nil {
		return
	}
	state := m.stateFor(rt, ExecSpec{}, CleanupRetained)
	sink(*state)
}

func (m *MimoRuntimeManager) runtimeKey(spec ExecSpec) string {
	workspace := spec.Workspace
	if workspace == "" {
		workspace = "."
	}
	return spec.NodeID + "|" + spec.ModelRef + "|" + workspace + "|" + spec.Agent
}

func (m *MimoRuntimeManager) Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error) {
	rt, err := m.ensure(ctx, spec, nil)
	if err != nil {
		return nil, err
	}
	return m.stateFor(rt, spec, policy), nil
}

func (m *MimoRuntimeManager) Release(runtimeID string) error {
	m.mu.Lock()
	var target *mimoRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("mimo runtime %q not found", runtimeID)
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

func (m *MimoRuntimeManager) Stop(runtimeID string) error {
	m.mu.Lock()
	var target *mimoRuntime
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
		return fmt.Errorf("mimo runtime %q not found", runtimeID)
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

func (m *MimoRuntimeManager) Get(runtimeID string) (*RuntimeState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			return m.stateFor(rt, ExecSpec{NodeID: ""}, CleanupRetained), true
		}
	}
	return nil, false
}

func (m *MimoRuntimeManager) List() []*RuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*RuntimeState, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		out = append(out, m.stateFor(rt, ExecSpec{}, CleanupRetained))
	}
	return out
}

// cleanupAll kills all managed acp processes and clears the registry.
func (m *MimoRuntimeManager) cleanupAll() {
	m.mu.Lock()
	runtimes := make([]*mimoRuntime, 0, len(m.runtimes))
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

func (m *MimoRuntimeManager) stateFor(rt *mimoRuntime, spec ExecSpec, policy CleanupPolicy) *RuntimeState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	nodeID := rt.NodeID
	if nodeID == "" {
		nodeID = spec.NodeID
	}
	model := rt.ModelRef
	if model == "" {
		model = spec.ModelRef
	}
	pid := 0
	if rt.Cmd != nil && rt.Cmd.Process != nil {
		pid = rt.Cmd.Process.Pid
	}
	return &RuntimeState{
		RuntimeID:     rt.ID,
		NodeID:        nodeID,
		Executor:      "mimo",
		Model:         model,
		Endpoint:      rt.Endpoint,
		Port:          rt.Port,
		PID:           pid,
		Status:        rt.status,
		Error:         rt.lastErr,
		CreatedAt:     rt.StartedAt,
		LastActiveAt:  rt.LastUsedAt,
		CleanupPolicy: policy,
		AccessMode:    "runtime_console",
		ApprovalMode:  rt.ApprovalMode,
		ThreadID:      rt.sessionID,
		TurnID:        rt.turnID,
		PermissionRequests: pendingPermissionList(rt.pendingPerms),
	}
}

// pendingPermissionList flattens the runtime's parked permission map into a
// stable, newest-first slice for the public RuntimeState. Callers hold rt.mu.
func pendingPermissionList(m map[string]PermissionRequestInfo) []PermissionRequestInfo {
	if len(m) == 0 {
		return nil
	}
	out := make([]PermissionRequestInfo, 0, len(m))
	for _, info := range m {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].AskedAt.After(out[j].AskedAt) })
	return out
}

func (m *MimoRuntimeManager) ensure(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*mimoRuntime, error) {
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
			if spec.ApprovalMode != "" {
				rt.ApprovalMode = spec.ApprovalMode
			}
			m.mu.Unlock()
			return rt, nil
		}
		delete(m.runtimes, key)
	}
	port := findFreePort()
	rt := &mimoRuntime{
		ID:           fmt.Sprintf("mimo_rt_%d", time.Now().UnixNano()),
		Port:         port,
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d", port),
		NodeID:       spec.NodeID,
		ModelRef:     spec.ModelRef,
		Workspace:    spec.Workspace,
		Agent:        spec.Agent,
		ApprovalMode: spec.ApprovalMode,
		StartedAt:    time.Now(),
		LastUsedAt:   time.Now(),
		done:         make(chan struct{}),
		status:       RuntimeStarting,
		clock:        newActivityClock(),
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

	workspace := strings.TrimSpace(spec.Workspace)
	args := []string{"acp", "--port", fmt.Sprint(port), "--hostname", "127.0.0.1"}
	if workspace != "" {
		args = append(args, "--cwd", workspace)
	}
	cmd := newRetainedRuntimeCommand(ctx, "mimo", args...)
	if workspace != "" {
		cmd.Dir = workspace
	}
	stdinR, stdinW, err := os.Pipe()
	if err != nil {
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("mimo acp stdin pipe: %w", err)
	}
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		_ = stdinR.Close()
		_ = stdinW.Close()
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("mimo acp stdout pipe: %w", err)
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
		return nil, fmt.Errorf("start mimo acp: %w", err)
	}
	// The parent never reads the child stdin nor writes its stdout.
	_ = stdinR.Close()
	_ = stdoutW.Close()
	rt.Cmd = cmd
	rt.stdinW = stdinW
	rt.stdoutR = stdoutR
	rt.Stderr = stderr
	go m.watchRuntime(key, rt)
	if onStart != nil {
		onStart(rt.Endpoint, rt.Port)
	}

	client := mimoclient.NewAcpClient(stdoutR, stdinW, func(e mimoclient.AcpEvent) { m.recordEvent(rt, e) })
	client.SetPermissionPolicy(mimoPermissionPolicy(rt))
	client.SetPermissionHook(func(req mimoclient.PermissionRequest) { m.parkPermission(rt, req) })
	rt.mu.Lock()
	rt.client = client
	if rt.pendingPerms == nil {
		rt.pendingPerms = make(map[string]PermissionRequestInfo)
	}
	rt.mu.Unlock()

	// The ACP server needs a few seconds to warm up (plugins, memory reconcile).
	deadline := time.Now().Add(60 * time.Second)
	for {
		dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		err := client.Initialize(dialCtx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			diagnostic := strings.TrimSpace(stderr.String())
			m.Stop(rt.ID)
			return nil, fmt.Errorf("mimo acp did not accept initialize within 60s: %s", diagnostic)
		}
		select {
		case <-ctx.Done():
			m.Stop(rt.ID)
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}

	rt.mu.Lock()
	rt.status = RuntimeIdle
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.notify(rt)
	return rt, nil
}

func (m *MimoRuntimeManager) dropRuntime(key string, rt *mimoRuntime) {
	m.mu.Lock()
	if current := m.runtimes[key]; current == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
}

func (m *MimoRuntimeManager) watchRuntime(key string, rt *mimoRuntime) {
	err := rt.Cmd.Wait()
	rt.mu.Lock()
	if rt.status != RuntimeStopped {
		rt.status = RuntimeError
		if err != nil {
			rt.lastErr = err.Error()
		} else {
			rt.lastErr = "mimo acp exited"
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

func (m *MimoRuntimeManager) recordEvent(rt *mimoRuntime, event mimoclient.AcpEvent) {
	// Any ACP event is a liveness signal for the turn watchdog.
	rt.clock.touch()
	// ACP streams reasoning/assistant text as tiny agent_*_chunk updates.
	// Coalesce them into one readable console block per message; everything
	// else (tool calls, part completion, usage) is a flush boundary.
	if rt.stream != nil {
		if method, key, category, ok := mimoStreamPart(event); ok {
			rt.stream.append(method, key, category, event.Text)
			return
		}
		rt.stream.flushNow()
	}
	rt.mu.Lock()
	level := "info"
	if strings.Contains(event.Method, "error") || strings.Contains(event.Method, "permission") {
		level = "error"
	}
	rt.events = append(rt.events, RuntimeConsoleEvent{At: event.At, Level: level, Method: event.Method, Text: event.Text, Payload: event.Payload})
	if len(rt.events) > 300 {
		rt.events = append([]RuntimeConsoleEvent(nil), rt.events[len(rt.events)-300:]...)
	}
	rt.mu.Unlock()
	m.notify(rt)
}

// mimoStreamPart classifies an ACP event as a streaming delta. agent_thought_chunk
// is reasoning; agent_message_chunk is the assistant answer; both are grouped by
// message id so a turn produces at most one console block per kind.
func mimoStreamPart(event mimoclient.AcpEvent) (method, key, category string, isDelta bool) {
	switch event.Update {
	case "agent_message_chunk":
		return "agent_message", event.MessageID, "assistant", true
	case "agent_thought_chunk":
		return "agent_thought", event.MessageID, "reasoning", true
	default:
		return "", "", "", false
	}
}

// Execute runs one orchestration node turn against the retained mimo session.
func (m *MimoRuntimeManager) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
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
	// Turn governance is activity-based; a watchdog cut surfaces as
	// ErrTurnIdleTimeout/ErrTurnMaxDuration (not a context error) so the
	// retained ACP session stays registered for resume/manual inspection.
	total := turnMaxDurationDefault
	if spec.TurnTimeout > 0 {
		total = spec.TurnTimeout
	}
	turnCtx, cancelTurn, wd := watchTurnActivity(ctx, rt.clock, turnIdleTimeoutDefault, total)
	defer cancelTurn()
	result, promptErr := client.Prompt(turnCtx, sessionID, spec.Prompt)
	if promptErr != nil && ctx.Err() == nil && wd.fired.Load() {
		// Cancel the server-side turn so the provider stops working into a
		// cancelled wait; session/cancel keeps the session itself. Cancel is
		// a plain stdin write (no ctx seam) and must not block the pipeline.
		_ = client.Cancel(sessionID)
		promptErr = wd.Err()
	}
	rt.mu.Lock()
	if result != nil {
		rt.output = result.Text
	}
	rt.mu.Unlock()
	if promptErr == nil && result != nil && result.StopReason == "cancelled" {
		promptErr = mimoclient.ErrTurnInterrupted
	}
	if promptErr != nil {
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	if strings.TrimSpace(result.Text) == "" {
		promptErr = fmt.Errorf("mimo acp turn completed without assistant output")
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	m.finishTurn(rt, sessionID, nil)
	execResult := m.execResult(rt, result.Text, sessionID)
	if result.Usage != nil {
		execResult.TokenUsage = &TokenUsage{
			InputTokens:  int(result.Usage.Input),
			OutputTokens: int(result.Usage.Output),
			TotalTokens:  int(result.Usage.Total),
		}
	}
	return execResult, nil
}

// prepareSession resolves the session used by one turn: a persisted
// ExternalSessionID is resumed with session/load; otherwise a new session is
// created and configured with the node model and agent mode.
func (m *MimoRuntimeManager) prepareSession(ctx context.Context, rt *mimoRuntime, client *mimoclient.AcpClient, spec ExecSpec) (string, error) {
	if spec.ContextPolicy == "fresh" || spec.ContextPolicy == "fresh_per_run" {
		return m.createConfiguredSession(ctx, rt, client, spec)
	}
	sessionID := strings.TrimSpace(spec.ExternalSessionID)
	if sessionID == "" {
		rt.mu.Lock()
		sessionID = strings.TrimSpace(rt.sessionID)
		rt.mu.Unlock()
	}
	if sessionID == "" {
		return m.createConfiguredSession(ctx, rt, client, spec)
	}
	loaded, err := client.LoadSession(ctx, spec.Workspace, sessionID)
	if err != nil {
		// The retained session may no longer exist in this mimo install (data
		// directory moved or session cleaned). Fall back to a fresh session.
		m.recordEvent(rt, mimoclient.AcpEvent{At: time.Now(), Method: "session/load", Text: "falling back to a new session: " + err.Error()})
		return m.createConfiguredSession(ctx, rt, client, spec)
	}
	return loaded, nil
}

func (m *MimoRuntimeManager) createConfiguredSession(ctx context.Context, rt *mimoRuntime, client *mimoclient.AcpClient, spec ExecSpec) (string, error) {
	workspace := strings.TrimSpace(spec.Workspace)
	sessionID, err := client.NewSession(ctx, workspace)
	if err != nil {
		return "", err
	}
	if model := strings.TrimSpace(spec.ModelRef); model != "" && strings.Contains(model, "/") {
		if err := client.SetConfigOption(ctx, sessionID, "model", model); err != nil {
			m.recordEvent(rt, mimoclient.AcpEvent{At: time.Now(), Method: "session/set_config_option", Text: "model ignored: " + err.Error()})
		}
	}
	modeID := strings.TrimSpace(spec.Agent)
	if modeID == "" {
		modeID = "build"
	}
	if err := client.SetMode(ctx, sessionID, modeID); err != nil {
		m.recordEvent(rt, mimoclient.AcpEvent{At: time.Now(), Method: "session/set_mode", Text: "mode ignored: " + err.Error()})
	}
	return sessionID, nil
}

// reserveTurn atomically reserves the retained acp runtime for exactly one
// active prompt turn. Both orchestrator work and Runtime Console messages use
// this gate so an operator cannot race a Loop node with a second provider turn.
func (m *MimoRuntimeManager) reserveTurn(rt *mimoRuntime) (*mimoclient.AcpClient, error) {
	rt.mu.Lock()
	if rt.client == nil {
		rt.mu.Unlock()
		return nil, fmt.Errorf("mimo runtime lost its acp connection")
	}
	if rt.status == RuntimeBusy {
		rt.mu.Unlock()
		return nil, fmt.Errorf("mimo runtime already has an active turn")
	}
	if rt.status == RuntimeStopped {
		rt.mu.Unlock()
		return nil, fmt.Errorf("mimo runtime is stopped")
	}
	if rt.status == RuntimeError {
		errText := rt.lastErr
		rt.mu.Unlock()
		if errText == "" {
			errText = "runtime is in an error state"
		}
		return nil, fmt.Errorf("mimo runtime is unavailable: %s", errText)
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

func (m *MimoRuntimeManager) finishTurn(rt *mimoRuntime, sessionID string, err error) {
	rt.mu.Lock()
	rt.sessionID = sessionID
	rt.turnID = ""
	rt.LastUsedAt = time.Now()
	if errors.Is(err, mimoclient.ErrTurnInterrupted) {
		// Interrupt is an expected operator action. The completed Turn is no
		// longer active while the retained session remains ready for a later
		// manual Turn or orchestrator-owned Loop attempt.
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

func (m *MimoRuntimeManager) execResult(rt *mimoRuntime, text, sessionID string) *ExecResult {
	rt.mu.Lock()
	rt.output = text
	stderr := rt.lastErr
	rt.mu.Unlock()
	return &ExecResult{FinalText: text, RawStdout: text, RawStderr: stderr, RuntimeID: rt.ID, Endpoint: rt.Endpoint, ExternalSessionID: sessionID}
}

// Snapshot returns the server-proxied console state for one runtime. The
// browser never dials provider endpoints directly.
func (m *MimoRuntimeManager) Snapshot(runtimeID string) (*RuntimeConsoleSnapshot, bool) {
	m.mu.Lock()
	var target *mimoRuntime
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
		CanSend:      target.status == RuntimeIdle,
		CanInterrupt: target.status == RuntimeBusy,
	}, true
}

// Send runs one manual Runtime Console turn against the retained session. The
// turn is orchestrator-only: it never creates a PipelineRun, Attempt or
// Iteration and is recorded as orchestrator/manual_turn.
func (m *MimoRuntimeManager) Send(ctx context.Context, runtimeID, text string) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("message must not be empty")
	}
	m.mu.Lock()
	var rt *mimoRuntime
	for _, candidate := range m.runtimes {
		if candidate.ID == runtimeID {
			rt = candidate
			break
		}
	}
	m.mu.Unlock()
	if rt == nil {
		return "", fmt.Errorf("mimo runtime %q not found", runtimeID)
	}
	client, err := m.reserveTurn(rt)
	if err != nil {
		return "", err
	}
	rt.mu.Lock()
	sessionID := rt.sessionID
	rt.mu.Unlock()
	if sessionID == "" {
		// Bounded session bootstrap so a dead server cannot wedge the console
		// handler forever. The 30-minute bound below covers the prompt itself.
		setupCtx, setupCancel := context.WithTimeout(context.Background(), 2*time.Minute)
		sessionID, err = m.createConfiguredSession(setupCtx, rt, client, ExecSpec{
			Workspace: rt.Workspace,
			ModelRef:  rt.ModelRef,
			Agent:     rt.Agent,
		})
		setupCancel()
		if err != nil {
			m.finishTurn(rt, "", err)
			return "", err
		}
		rt.mu.Lock()
		rt.sessionID = sessionID
		rt.mu.Unlock()
	}
	turnID := fmt.Sprintf("turn_%d", time.Now().UnixNano())
	rt.mu.Lock()
	rt.turnID = turnID
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.recordEvent(rt, mimoclient.AcpEvent{At: time.Now(), Method: "orchestrator/manual_turn", Text: text})
	m.notify(rt)
	go func() {
		turnCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
		defer cancel()
		result, promptErr := client.Prompt(turnCtx, sessionID, text)
		if promptErr == nil && result != nil && result.StopReason == "cancelled" {
			promptErr = mimoclient.ErrTurnInterrupted
		}
		rt.mu.Lock()
		if result != nil {
			rt.output = result.Text
		}
		rt.mu.Unlock()
		m.finishTurn(rt, sessionID, promptErr)
	}()
	return turnID, nil
}

// Interrupt cancels the active prompt turn via the ACP session/cancel
// notification. The retained session survives the interruption.
func (m *MimoRuntimeManager) Interrupt(ctx context.Context, runtimeID string) error {
	m.mu.Lock()
	var rt *mimoRuntime
	for _, candidate := range m.runtimes {
		if candidate.ID == runtimeID {
			rt = candidate
			break
		}
	}
	m.mu.Unlock()
	if rt == nil {
		return fmt.Errorf("mimo runtime %q not found", runtimeID)
	}
	rt.mu.Lock()
	client, sessionID, active := rt.client, rt.sessionID, rt.status == RuntimeBusy
	rt.mu.Unlock()
	if client == nil || sessionID == "" || !active {
		return fmt.Errorf("mimo runtime has no active turn")
	}
	return client.Cancel(sessionID)
}

// mimoPermissionPolicy answers ACP permission requests the way the node's
// approval mode expects: ask parks the request for a human decision in the
// Runtime Console (ErrPermissionPending), auto/yolo allows the tool call for
// the whole session.
func mimoPermissionPolicy(rt *mimoRuntime) mimoclient.PermissionPolicy {
	return func(_ string, _ json.RawMessage) (string, error) {
		rt.mu.Lock()
		approval := rt.ApprovalMode
		rt.mu.Unlock()
		if strings.EqualFold(approval, "ask") {
			return "", mimoclient.ErrPermissionPending
		}
		return "allow_always", nil
	}
}

// parkPermission stores one parked ACP permission request and wakes the
// Runtime Console through the state sink.
func (m *MimoRuntimeManager) parkPermission(rt *mimoRuntime, req mimoclient.PermissionRequest) {
	key := string(req.EnvID)
	info := PermissionRequestInfo{
		RequestID: key,
		ToolName:  req.ToolName,
		ToolInput: trimPermissionInput(req.ToolInput),
		SessionID: req.SessionID,
		AskedAt:   req.AskedAt,
	}
	rt.mu.Lock()
	if rt.pendingPerms == nil {
		rt.pendingPerms = make(map[string]PermissionRequestInfo)
	}
	rt.pendingPerms[key] = info
	rt.mu.Unlock()
	m.notify(rt)
}

// AnswerMimoRuntimePermission resolves a parked ACP permission request with
// the chosen action: "allow_once", "allow_always" or "reject".
func AnswerMimoRuntimePermission(runtimeID, requestID, action string) error {
	mimoRuntimeMgr.mu.Lock()
	var target *mimoRuntime
	for _, rt := range mimoRuntimeMgr.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	mimoRuntimeMgr.mu.Unlock()
	if target == nil {
		return fmt.Errorf("mimo runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	_, ok := target.pendingPerms[requestID]
	if ok {
		delete(target.pendingPerms, requestID)
	}
	client := target.client
	target.mu.Unlock()
	if !ok {
		return fmt.Errorf("permission request %q not pending on runtime %q", requestID, runtimeID)
	}
	if client == nil {
		return fmt.Errorf("mimo runtime %q has no client", runtimeID)
	}
	if err := client.AnswerPermission(json.RawMessage(requestID), action); err != nil {
		return err
	}
	mimoRuntimeMgr.notify(target)
	return nil
}

// trimPermissionInput keeps the console card compact: the raw ACP toolCall
// can embed large tool arguments.
func trimPermissionInput(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}

var mimoRuntimeMgr = newMimoRuntimeManager()

func ListMimoRuntimes() []*RuntimeState              { return mimoRuntimeMgr.List() }
func GetMimoRuntime(id string) (*RuntimeState, bool) { return mimoRuntimeMgr.Get(id) }
func StopMimoRuntime(id string) error                { return mimoRuntimeMgr.Stop(id) }
func GetMimoRuntimeConsole(id string) (*RuntimeConsoleSnapshot, bool) {
	return mimoRuntimeMgr.Snapshot(id)
}
func SendMimoRuntimeMessage(ctx context.Context, id, text string) (string, error) {
	return mimoRuntimeMgr.Send(ctx, id, text)
}
func InterruptMimoRuntime(ctx context.Context, id string) error {
	return mimoRuntimeMgr.Interrupt(ctx, id)
}

var _ = strconv.Itoa
