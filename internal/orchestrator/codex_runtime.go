package orchestrator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"sync"
	"time"

	codexclient "reasonix/internal/executor/codex"
)

// RuntimeConsoleEvent is a provider-neutral record displayed in the browser
// Runtime Console. Payload is raw JSON-RPC data and is never executed by the
// browser; the UI renders it as escaped diagnostic text.
type RuntimeConsoleEvent struct {
	At       time.Time `json:"at"`
	Level    string    `json:"level"`
	Method   string    `json:"method"`
	Text     string    `json:"text,omitempty"`
	Payload  string    `json:"payload,omitempty"`
	Category string    `json:"category,omitempty"` // reasoning | assistant | prompt | tool | ""
}

// RuntimeConsoleSnapshot is the backend-owned view returned to the browser.
// The browser never dials provider WebSocket endpoints directly.
type RuntimeConsoleSnapshot struct {
	Runtime      RuntimeState          `json:"runtime"`
	ThreadID     string                `json:"threadID,omitempty"`
	TurnID       string                `json:"turnID,omitempty"`
	Output       string                `json:"output,omitempty"`
	Error        string                `json:"error,omitempty"`
	Events       []RuntimeConsoleEvent `json:"events"`
	CanSend      bool                  `json:"canSend"`
	CanInterrupt bool                  `json:"canInterrupt"`
}

type codexRuntime struct {
	ID            string
	Port          int
	Endpoint      string
	NodeID        string
	ModelRef      string
	DisplayModel  string
	ProviderRoute string
	ApprovalMode  string
	ExecutionMode string
	Workspace     string
	Cmd           *exec.Cmd
	Stderr        *bytes.Buffer
	done          chan struct{}
	StartedAt     time.Time
	LastUsedAt    time.Time
	failedAt      time.Time

	mu       sync.Mutex
	client   *codexclient.AppServerClient
	threadID string
	turnID   string
	status   RuntimeStatus
	output   string
	lastErr  string
	events   []RuntimeConsoleEvent
	stream   *consoleStreamCoalescer
}

// CodexRuntimeManager starts `codex app-server` as a retained, loopback-only
// WebSocket process. One runtime can host many persisted Codex threads; the
// ProviderSession's ExternalSessionID remains the source of thread identity.
type CodexRuntimeManager struct {
	mu       sync.Mutex
	runtimes map[string]*codexRuntime
	onUpdate func(RuntimeState)
}

func newCodexRuntimeManager() *CodexRuntimeManager {
	return &CodexRuntimeManager{runtimes: make(map[string]*codexRuntime)}
}

// SetUpdateSink registers the process-local bridge used to wake the browser
// Runtime Console through Orchestrator SSE. It receives metadata only; raw
// provider messages stay readable through the authenticated console API.
func (m *CodexRuntimeManager) SetUpdateSink(sink func(RuntimeState)) {
	m.mu.Lock()
	m.onUpdate = sink
	m.mu.Unlock()
}

func (m *CodexRuntimeManager) notify(rt *codexRuntime) {
	m.mu.Lock()
	sink := m.onUpdate
	m.mu.Unlock()
	if sink == nil {
		return
	}
	state := m.stateFor(rt, ExecSpec{}, CleanupRetained)
	sink(*state)
}

func (m *CodexRuntimeManager) runtimeKey(spec ExecSpec) string {
	workspace := spec.Workspace
	if workspace == "" {
		workspace = "."
	}
	return spec.NodeID + "|" + spec.ModelRef + "|" + workspace + "|" + spec.ProviderRoute
}

func (m *CodexRuntimeManager) Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error) {
	rt, err := m.ensure(ctx, spec, nil)
	if err != nil {
		return nil, err
	}
	return m.stateFor(rt, spec, policy), nil
}

func (m *CodexRuntimeManager) Release(runtimeID string) error {
	m.mu.Lock()
	var target *codexRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("codex runtime %q not found", runtimeID)
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

func (m *CodexRuntimeManager) Stop(runtimeID string) error {
	m.mu.Lock()
	var target *codexRuntime
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
		return fmt.Errorf("codex runtime %q not found", runtimeID)
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
	m.notify(target)
	return nil
}

func (m *CodexRuntimeManager) Get(runtimeID string) (*RuntimeState, bool) {
	m.pruneFailed()
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			return m.stateFor(rt, ExecSpec{NodeID: ""}, CleanupRetained), true
		}
	}
	return nil, false
}

func (m *CodexRuntimeManager) List() []*RuntimeState {
	m.pruneFailed()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*RuntimeState, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		out = append(out, m.stateFor(rt, ExecSpec{}, CleanupRetained))
	}
	return out
}

func (m *CodexRuntimeManager) stateFor(rt *codexRuntime, spec ExecSpec, policy CleanupPolicy) *RuntimeState {
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
	approvalMode := rt.ApprovalMode
	if approvalMode == "" {
		approvalMode = spec.ApprovalMode
	}
	executionMode := rt.ExecutionMode
	if executionMode == "" {
		executionMode = spec.ExecutionMode
	}
	return &RuntimeState{
		RuntimeID: rt.ID, NodeID: nodeID, Executor: string(ExecutorCodex), Model: model,
		Endpoint: rt.Endpoint, Port: rt.Port, Status: rt.status, Error: rt.lastErr,
		CreatedAt: rt.StartedAt, LastActiveAt: rt.LastUsedAt, CleanupPolicy: policy,
		AccessMode: "runtime_console", ApprovalMode: approvalMode, ExecutionMode: executionMode,
		ThreadID: rt.threadID, TurnID: rt.turnID,
	}
}

// failedRuntimeTTL keeps a dead runtime registered so the console can show its
// startup/connection error instead of a 404, then it is pruned lazily.
const failedRuntimeTTL = 60 * time.Second

// pruneFailed removes dead runtime records that have been error for longer
// than failedRuntimeTTL. Called lazily from List/Get/ensure.
func (m *CodexRuntimeManager) pruneFailed() {
	now := time.Now()
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, rt := range m.runtimes {
		rt.mu.Lock()
		dead := rt.status == RuntimeError && !rt.failedAt.IsZero() && now.Sub(rt.failedAt) > failedRuntimeTTL
		rt.mu.Unlock()
		if dead {
			delete(m.runtimes, key)
		}
	}
}

func (m *CodexRuntimeManager) ensure(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*codexRuntime, error) {
	key := m.runtimeKey(spec)
	m.mu.Lock()
	if rt := m.runtimes[key]; rt != nil {
		rt.mu.Lock()
		live := rt.client != nil && rt.status != RuntimeStopped && rt.status != RuntimeError
		rt.LastUsedAt = time.Now()
		rt.mu.Unlock()
		if live {
			// Preserve display metadata across Loop turns. Provider routing may use
			// an empty ModelRef (CCSwitch), but the UI must still show its configured
			// alias and owning pipeline node.
			if spec.NodeID != "" {
				rt.NodeID = spec.NodeID
			}
			if spec.DisplayModel != "" {
				rt.DisplayModel = spec.DisplayModel
			}
			if spec.ApprovalMode != "" {
				rt.ApprovalMode = spec.ApprovalMode
			}
			if spec.ExecutionMode != "" {
				rt.ExecutionMode = spec.ExecutionMode
			}
			m.mu.Unlock()
			return rt, nil
		}
		delete(m.runtimes, key)
	}
	port := findFreePort()
	rt := &codexRuntime{
		ID:            fmt.Sprintf("codex_rt_%d", time.Now().UnixNano()),
		Port:          port,
		Endpoint:      fmt.Sprintf("ws://127.0.0.1:%d", port),
		NodeID:        spec.NodeID,
		ModelRef:      spec.ModelRef,
		DisplayModel:  spec.DisplayModel,
		ProviderRoute: spec.ProviderRoute,
		ApprovalMode:  spec.ApprovalMode,
		ExecutionMode: spec.ExecutionMode,
		Workspace:     spec.Workspace,
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

	spawnArgs := []string{"app-server", "--listen", rt.Endpoint}
	// `codex app-server` does not accept --profile, but it does accept nested
	// -c key=value overrides. Load the node's profile overlay
	// ($CODEX_HOME/<profile>.config.toml, generated from cc-switch providers)
	// and pass every key as -c, so serve mode uses the exact same
	// machine-local configuration as run mode (`codex --profile`) and never
	// depends on the shared config.toml that cc-switch rewrites on switch.
	for _, ov := range codexProfileOverrides(codexProfile(spec)) {
		spawnArgs = append(spawnArgs, "-c", ov)
	}
	// Match run-mode trust semantics: trusted Loop nodes get full sandbox
	// access so they can write deliverable files. Without this override the
	// app-server inherits the read-only sandbox_mode default and every node
	// produces output that can never be persisted to the workspace.
	if spec.Trust {
		spawnArgs = append(spawnArgs, "-c", "sandbox_mode=danger-full-access")
	}
	cmd := newRetainedRuntimeCommand(ctx, "codex", spawnArgs...)
	cmd.Dir = spec.Workspace
	cmd.Stdout = io.Discard
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		m.mu.Lock()
		delete(m.runtimes, key)
		m.mu.Unlock()
		return nil, fmt.Errorf("start codex app-server: %w", err)
	}
	rt.Cmd = cmd
	rt.Stderr = stderr
	go m.watchRuntime(key, rt)
	if onStart != nil {
		onStart(rt.Endpoint, rt.Port)
	}

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if runtimeExited(rt.done) {
			break
		}
		dialCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		client, err := codexclient.DialAppServer(dialCtx, rt.Endpoint, func(e codexclient.AppServerEvent) { m.recordEvent(rt, e) })
		cancel()
		if err == nil {
			rt.mu.Lock()
			rt.client = client
			rt.status = RuntimeIdle
			rt.LastUsedAt = time.Now()
			rt.mu.Unlock()
			return rt, nil
		}
		select {
		case <-ctx.Done():
			m.Stop(rt.ID)
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}
	diagnostic := strings.TrimSpace(stderr.String())
	// Do NOT Stop the runtime here: watchRuntime already kept the dead record
	// (status=error) so the Runtime Console can show the exact failure.
	rt.mu.Lock()
	if rt.status != RuntimeError {
		rt.status = RuntimeError
		rt.lastErr = diagnostic
		if rt.lastErr == "" {
			rt.lastErr = "codex app-server exited before accepting WebSocket connections"
		}
		rt.failedAt = time.Now()
	}
	rt.mu.Unlock()
	m.notify(rt)
	return nil, fmt.Errorf("codex app-server %s did not accept WebSocket connections within 45s: %s", rt.Endpoint, diagnostic)
}

func (m *CodexRuntimeManager) watchRuntime(key string, rt *codexRuntime) {
	err := rt.Cmd.Wait()
	rt.mu.Lock()
	if rt.status != RuntimeStopped {
		rt.status = RuntimeError
		if err != nil {
			rt.lastErr = err.Error()
		} else {
			rt.lastErr = "codex app-server exited"
		}
	}
	client := rt.client
	stream := rt.stream
	rt.stream = nil
	rt.client = nil
	if rt.status != RuntimeStopped {
		// Keep a failed runtime registered for a short window so the Runtime
		// Console can surface the startup/connection error instead of a 404.
		// The manager prunes it lazily once it is older than failedRuntimeTTL.
		rt.failedAt = time.Now()
	}
	rt.mu.Unlock()
	if stream != nil {
		stream.stop()
	}
	if client != nil {
		client.Close()
	}
	m.notify(rt)
	m.mu.Lock()
	if rt.status == RuntimeStopped && m.runtimes[key] == rt {
		// User-initiated Stop already removed it from the registry in Stop().
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	close(rt.done)
}

func (m *CodexRuntimeManager) recordEvent(rt *codexRuntime, event codexclient.AppServerEvent) {
	// Stream the tiny token deltas into a consolidated block so the console
	// shows one readable entry per message instead of one row per fragment.
	if rt.stream != nil {
		if method, key, category, ok := codexStreamPart(event); ok {
			rt.stream.append(method, key, category, event.Text)
			return
		}
		rt.stream.flushNow()
	}
	rt.mu.Lock()
	payload := string(event.Params)
	level := "info"
	if strings.Contains(event.Method, "error") {
		level = "error"
	}
	rt.events = append(rt.events, RuntimeConsoleEvent{At: event.At, Level: level, Method: event.Method, Text: event.Text, Payload: payload})
	if len(rt.events) > 300 {
		rt.events = append([]RuntimeConsoleEvent(nil), rt.events[len(rt.events)-300:]...)
	}
	rt.mu.Unlock()
	m.notify(rt)
}

// codexStreamPart classifies a Codex app-server event as a streaming delta.
// Deltas are coalesced per turn so reasoning and assistant text each become a
// single console block; every other event is a boundary that flushes them.
func codexStreamPart(event codexclient.AppServerEvent) (method, key, category string, isDelta bool) {
	// Delta method names vary: item/agentMessage/delta, item/reasoning/textDelta,
	// item/input/textDelta, ...Chunk variants.
	if !strings.Contains(strings.ToLower(event.Method), "delta") {
		return "", "", "", false
	}
	key = codexEventTurnID(event.Params)
	category = "assistant"
	if strings.Contains(event.Method, "reasoning") || strings.Contains(event.Method, "thought") {
		category = "reasoning"
	} else if strings.Contains(event.Method, "input") {
		category = "prompt"
	}
	return event.Method, key, category, true
}

func codexEventTurnID(params json.RawMessage) string {
	var body struct {
		TurnID string `json:"turnId"`
	}
	if json.Unmarshal(params, &body) != nil || body.TurnID == "" {
		return ""
	}
	return body.TurnID
}

func (m *CodexRuntimeManager) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	rt, err := m.ensure(ctx, spec, onStart)
	if err != nil {
		return &ExecResult{ExitCode: -1}, err
	}
	opts := codexclient.ThreadOptions{Workspace: spec.Workspace, Model: codexRuntimeModel(spec), ApprovalPolicy: codexApprovalPolicy(spec)}
	client, err := m.reserveTurn(rt)
	if err != nil {
		return &ExecResult{RuntimeID: rt.ID, Endpoint: rt.Endpoint, ExitCode: -1}, err
	}

	threadID, err := client.ResumeThread(ctx, spec.ExternalSessionID, opts)
	if err != nil {
		// Preserve a known historical Thread ID even when thread/resume itself
		// fails. A subsequent retained runtime can retry the same resume rather
		// than silently losing the durable conversation identity.
		threadID = strings.TrimSpace(spec.ExternalSessionID)
		if threadID == "" {
			rt.mu.Lock()
			threadID = rt.threadID
			rt.mu.Unlock()
		}
		m.finishTurn(rt, threadID, err)
		return m.execResult(rt, "", threadID), err
	}
	turnID, err := client.StartTurn(ctx, threadID, spec.Prompt, codexRuntimeModel(spec), spec.ReasoningEffort)
	if err != nil {
		m.finishTurn(rt, threadID, err)
		return m.execResult(rt, "", threadID), err
	}
	rt.mu.Lock()
	rt.threadID = threadID
	rt.turnID = turnID
	rt.mu.Unlock()
	text, err := client.WaitTurn(ctx, turnID)
	if err == nil && strings.TrimSpace(text) == "" {
		err = fmt.Errorf("codex app-server turn completed without assistant output")
	}
	rt.mu.Lock()
	rt.output = text
	rt.mu.Unlock()
	m.finishTurn(rt, threadID, err)
	result := m.execResult(rt, text, threadID)
	if err != nil {
		return result, err
	}
	return result, nil
}

// reserveTurn atomically reserves the retained App Server for exactly one
// active Turn. Both orchestrator work and Runtime Console messages use this
// gate, so an operator cannot accidentally race a Loop node with a second
// provider Turn on the same Codex Thread.
func (m *CodexRuntimeManager) reserveTurn(rt *codexRuntime) (*codexclient.AppServerClient, error) {
	rt.mu.Lock()
	if rt.client == nil {
		rt.mu.Unlock()
		return nil, fmt.Errorf("codex runtime lost its app-server connection")
	}
	if rt.status == RuntimeBusy {
		rt.mu.Unlock()
		return nil, fmt.Errorf("codex runtime already has an active turn")
	}
	if rt.status == RuntimeStopped {
		rt.mu.Unlock()
		return nil, fmt.Errorf("codex runtime is stopped")
	}
	if rt.status == RuntimeError {
		errText := rt.lastErr
		rt.mu.Unlock()
		if errText == "" {
			errText = "runtime is in an error state"
		}
		return nil, fmt.Errorf("codex runtime is unavailable: %s", errText)
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

func (m *CodexRuntimeManager) finishTurn(rt *codexRuntime, threadID string, err error) {
	rt.mu.Lock()
	rt.threadID = threadID
	rt.turnID = ""
	rt.LastUsedAt = time.Now()
	if errors.Is(err, codexclient.ErrAppServerTurnInterrupted) {
		// Interrupt is an expected operator action. The completed Turn is no
		// longer active, while the retained Thread remains ready for a later
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

func (m *CodexRuntimeManager) execResult(rt *codexRuntime, text, threadID string) *ExecResult {
	rt.mu.Lock()
	rt.output = text
	stderr := rt.lastErr
	rt.mu.Unlock()
	return &ExecResult{FinalText: text, RawStdout: text, RawStderr: stderr, RuntimeID: rt.ID, Endpoint: rt.Endpoint, ExternalSessionID: threadID}
}

// codexProfile picks the $CODEX_HOME/<name>.config.toml overlay for a node.
//
//   - providerRoute=ccswitch (or model=ccs/ccswitch): "ccs" profile -> the
//     cc-switch local proxy owns the upstream model (e.g. gpt5.6).
//   - model starts with deepseek: "deepseek" profile -> DeepSeek official
//     direct connection, bypassing any local proxy.
//   - otherwise: no profile; the base config default provider is used.
//
// Profiles are the supported way to switch model_provider per invocation in
// codex >= 0.145 (a single config cannot route by model name).
func codexProfile(spec ExecSpec) string {
	if strings.EqualFold(spec.ProviderRoute, "ccswitch") || strings.EqualFold(spec.ModelRef, "ccs") || strings.EqualFold(spec.ModelRef, "ccswitch") {
		return "ccs"
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(spec.ModelRef)), "deepseek") {
		return "deepseek"
	}
	return ""
}

// codexServeProvider maps a node onto the model_provider override passed to
// the retained `codex app-server` process. The deepseek provider is the
// official direct connection; ccs routes through the cc-switch local proxy
// (named "custom" in the shared base config).
func codexServeProvider(spec ExecSpec) string {
	switch codexProfile(spec) {
	case "deepseek":
		return "deepseek"
	case "ccs":
		return "custom"
	}
	return ""
}

func codexRuntimeModel(spec ExecSpec) string {
	if strings.EqualFold(spec.ProviderRoute, "ccswitch") || strings.EqualFold(spec.ModelRef, "ccs") || strings.EqualFold(spec.ModelRef, "ccswitch") {
		return ""
	}
	return strings.TrimSpace(spec.ModelRef)
}
func codexApprovalPolicy(spec ExecSpec) string {
	if strings.EqualFold(spec.ApprovalMode, "ask") {
		return "on-request"
	}
	return "never"
}

func (m *CodexRuntimeManager) Snapshot(runtimeID string) (*RuntimeConsoleSnapshot, bool) {
	m.mu.Lock()
	var target *codexRuntime
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
	return &RuntimeConsoleSnapshot{Runtime: *state, ThreadID: target.threadID, TurnID: target.turnID, Output: target.output, Error: target.lastErr, Events: events, CanSend: target.status == RuntimeIdle, CanInterrupt: target.status == RuntimeBusy && target.threadID != "" && target.turnID != ""}, true
}

func (m *CodexRuntimeManager) Send(ctx context.Context, runtimeID, text string) (string, error) {
	text = strings.TrimSpace(text)
	if text == "" {
		return "", fmt.Errorf("message must not be empty")
	}
	m.mu.Lock()
	var rt *codexRuntime
	for _, candidate := range m.runtimes {
		if candidate.ID == runtimeID {
			rt = candidate
			break
		}
	}
	m.mu.Unlock()
	if rt == nil {
		return "", fmt.Errorf("codex runtime %q not found", runtimeID)
	}
	client, err := m.reserveTurn(rt)
	if err != nil {
		return "", err
	}
	rt.mu.Lock()
	threadID := rt.threadID
	rt.mu.Unlock()
	if threadID == "" {
		err := fmt.Errorf("codex runtime has no thread yet; run the node first")
		m.finishTurn(rt, "", err)
		return "", err
	}
	// This marker is intentionally runtime-only: it makes the console's human
	// interaction explicit without creating a PipelineRun, Attempt or Iteration.
	m.recordEvent(rt, codexclient.AppServerEvent{At: time.Now(), Method: "orchestrator/manual_turn", Text: text})
	turnID, err := client.StartTurn(ctx, threadID, text, "", "")
	if err != nil {
		m.finishTurn(rt, threadID, err)
		return "", err
	}
	rt.mu.Lock()
	rt.turnID = turnID
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.notify(rt)
	go func() {
		answer, waitErr := client.WaitTurn(context.Background(), turnID)
		rt.mu.Lock()
		rt.output = answer
		rt.mu.Unlock()
		m.finishTurn(rt, threadID, waitErr)
	}()
	return turnID, nil
}

func (m *CodexRuntimeManager) Interrupt(ctx context.Context, runtimeID string) error {
	m.mu.Lock()
	var rt *codexRuntime
	for _, candidate := range m.runtimes {
		if candidate.ID == runtimeID {
			rt = candidate
			break
		}
	}
	m.mu.Unlock()
	if rt == nil {
		return fmt.Errorf("codex runtime %q not found", runtimeID)
	}
	rt.mu.Lock()
	client, threadID, turnID := rt.client, rt.threadID, rt.turnID
	rt.mu.Unlock()
	if client == nil || threadID == "" || turnID == "" {
		return fmt.Errorf("codex runtime has no active turn")
	}
	if err := client.InterruptTurn(ctx, threadID, turnID); err != nil {
		return err
	}
	return nil
}

var codexRuntimeMgr = newCodexRuntimeManager()

func ListCodexRuntimes() []*RuntimeState              { return codexRuntimeMgr.List() }
func GetCodexRuntime(id string) (*RuntimeState, bool) { return codexRuntimeMgr.Get(id) }
func StopCodexRuntime(id string) error                { return codexRuntimeMgr.Stop(id) }
func GetCodexRuntimeConsole(id string) (*RuntimeConsoleSnapshot, bool) {
	return codexRuntimeMgr.Snapshot(id)
}
func SendCodexRuntimeMessage(ctx context.Context, id, text string) (string, error) {
	return codexRuntimeMgr.Send(ctx, id, text)
}
func InterruptCodexRuntime(ctx context.Context, id string) error {
	return codexRuntimeMgr.Interrupt(ctx, id)
}
