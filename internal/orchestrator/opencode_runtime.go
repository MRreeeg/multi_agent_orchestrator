package orchestrator

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	opencodeclient "reasonix/internal/executor/opencode"
)

// opencodeRuntime is a retained `opencode serve` runtime driven over its
// loopback HTTP API. One runtime hosts one retained opencode session.
type opencodeRuntime struct {
	ID           string
	Port         int
	Endpoint     string
	NodeID       string
	ModelRef     string
	Workspace    string
	ApprovalMode string
	Cmd          *exec.Cmd
	Stderr       *bytes.Buffer
	done         chan struct{}
	StartedAt    time.Time
	LastUsedAt   time.Time

	mu        sync.Mutex
	client    *opencodeclient.Client
	sessionID string
	turnID    string
	status    RuntimeStatus
	output    string
	lastErr   string
	events    []RuntimeConsoleEvent
	stream    *consoleStreamCoalescer
}

// OpenCodeRuntimeManager starts `opencode serve` as a retained, loopback-only
// runtime and drives it over HTTP.
type OpenCodeRuntimeManager struct {
	mu       sync.Mutex
	runtimes map[string]*opencodeRuntime
	onUpdate func(RuntimeState)
}

func newOpenCodeRuntimeManager() *OpenCodeRuntimeManager {
	return &OpenCodeRuntimeManager{runtimes: make(map[string]*opencodeRuntime)}
}

func (m *OpenCodeRuntimeManager) SetUpdateSink(sink func(RuntimeState)) {
	m.mu.Lock()
	m.onUpdate = sink
	m.mu.Unlock()
}

func (m *OpenCodeRuntimeManager) notify(rt *opencodeRuntime) {
	m.mu.Lock()
	sink := m.onUpdate
	m.mu.Unlock()
	if sink == nil {
		return
	}
	sink(*m.stateFor(rt, ExecSpec{}, CleanupRetained))
}

func (m *OpenCodeRuntimeManager) runtimeKey(spec ExecSpec) string {
	workspace := spec.Workspace
	if workspace == "" {
		workspace = "."
	}
	return spec.NodeID + "|" + spec.ModelRef + "|" + workspace
}

func (m *OpenCodeRuntimeManager) Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error) {
	rt, err := m.ensure(ctx, spec, nil)
	if err != nil {
		return nil, err
	}
	return m.stateFor(rt, spec, policy), nil
}

func (m *OpenCodeRuntimeManager) Release(runtimeID string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
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

func (m *OpenCodeRuntimeManager) Stop(runtimeID string) error {
	m.mu.Lock()
	var target *opencodeRuntime
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
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
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
		// Best-effort abort of an active turn.
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = client.Abort(ctx, target.sessionID)
		cancel()
	}
	if target.Cmd != nil {
		stopRuntimeProcess(target.Cmd, target.done)
	}
	m.notify(target)
	return nil
}

func (m *OpenCodeRuntimeManager) Get(runtimeID string) (*RuntimeState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			return m.stateFor(rt, ExecSpec{NodeID: ""}, CleanupRetained), true
		}
	}
	return nil, false
}

func (m *OpenCodeRuntimeManager) List() []*RuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*RuntimeState, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		out = append(out, m.stateFor(rt, ExecSpec{}, CleanupRetained))
	}
	return out
}

func (m *OpenCodeRuntimeManager) cleanupAll() {
	m.mu.Lock()
	runtimes := make([]*opencodeRuntime, 0, len(m.runtimes))
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
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = client.Abort(ctx, rt.sessionID)
			cancel()
		}
		stopRuntimeProcess(rt.Cmd, rt.done)
	}
}

func (m *OpenCodeRuntimeManager) stateFor(rt *opencodeRuntime, spec ExecSpec, policy CleanupPolicy) *RuntimeState {
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
		Executor:      "opencode",
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
	}
}

func (m *OpenCodeRuntimeManager) ensure(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*opencodeRuntime, error) {
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
			m.mu.Unlock()
			return rt, nil
		}
		delete(m.runtimes, key)
	}
	port := findFreePort()
	rt := &opencodeRuntime{
		ID:           fmt.Sprintf("opencode_rt_%d", time.Now().UnixNano()),
		Port:         port,
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d", port),
		NodeID:       spec.NodeID,
		ModelRef:     spec.ModelRef,
		Workspace:    spec.Workspace,
		ApprovalMode: spec.ApprovalMode,
		StartedAt:    time.Now(),
		LastUsedAt:   time.Now(),
		done:         make(chan struct{}),
		status:       RuntimeStarting,
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

	args := []string{"serve", "--port", fmt.Sprint(port), "--hostname", "127.0.0.1"}
	cmd := newRetainedRuntimeCommand(ctx, "opencode", args...)
	if spec.Workspace != "" {
		cmd.Dir = spec.Workspace
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("start opencode serve: %w", err)
	}
	rt.Cmd = cmd
	rt.Stderr = stderr
	go m.watchRuntime(key, rt)
	if onStart != nil {
		onStart(rt.Endpoint, rt.Port)
	}

	client := opencodeclient.NewClient(rt.Endpoint)
	rt.mu.Lock()
	rt.client = client
	rt.mu.Unlock()

	// Wait for the server health endpoint.
	deadline := time.Now().Add(60 * time.Second)
	for {
		healthy := false
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		func() {
			defer cancel()
			req, _ := http.NewRequestWithContext(checkCtx, http.MethodGet, rt.Endpoint+"/global/health", nil)
			resp, err := client.HTTP.Do(req)
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					healthy = true
				}
			}
		}()
		if healthy {
			break
		}
		if time.Now().After(deadline) {
			diagnostic := strings.TrimSpace(stderr.String())
			m.Stop(rt.ID)
			return nil, fmt.Errorf("opencode serve did not become healthy within 60s: %s", diagnostic)
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

func (m *OpenCodeRuntimeManager) dropRuntime(key string, rt *opencodeRuntime) {
	m.mu.Lock()
	if current := m.runtimes[key]; current == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
}

func (m *OpenCodeRuntimeManager) watchRuntime(key string, rt *opencodeRuntime) {
	err := rt.Cmd.Wait()
	rt.mu.Lock()
	if rt.status != RuntimeStopped {
		rt.status = RuntimeError
		if err != nil {
			rt.lastErr = err.Error()
		} else {
			rt.lastErr = "opencode serve exited"
		}
	}
	rt.mu.Unlock()
	m.notify(rt)
	m.mu.Lock()
	if m.runtimes[key] == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	close(rt.done)
}

// Execute runs one orchestration node turn against the retained opencode
// session over HTTP.
func (m *OpenCodeRuntimeManager) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
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
	text, promptErr := client.Prompt(ctx, sessionID, spec.ModelRef, spec.Prompt)
	rt.mu.Lock()
	if text != "" {
		rt.output = text
	}
	rt.mu.Unlock()
	if promptErr != nil {
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	if strings.TrimSpace(text) == "" {
		promptErr = fmt.Errorf("opencode turn completed without assistant output")
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	m.finishTurn(rt, sessionID, nil)
	return m.execResult(rt, text, sessionID), nil
}

func (m *OpenCodeRuntimeManager) prepareSession(ctx context.Context, rt *opencodeRuntime, client *opencodeclient.Client, spec ExecSpec) (string, error) {
	if spec.ContextPolicy == "fresh" || spec.ContextPolicy == "fresh_per_run" {
		return m.createSession(ctx, rt, client, spec)
	}
	sessionID := strings.TrimSpace(spec.ExternalSessionID)
	if sessionID == "" {
		rt.mu.Lock()
		sessionID = strings.TrimSpace(rt.sessionID)
		rt.mu.Unlock()
	}
	if sessionID == "" {
		return m.createSession(ctx, rt, client, spec)
	}
	return sessionID, nil
}

func (m *OpenCodeRuntimeManager) createSession(ctx context.Context, rt *opencodeRuntime, client *opencodeclient.Client, spec ExecSpec) (string, error) {
	title := strings.TrimSpace(spec.NodeLabel)
	if title == "" {
		title = "opencode-node"
	}
	sessionID, err := client.NewSession(ctx, title)
	if err != nil {
		return "", err
	}
	rt.mu.Lock()
	rt.sessionID = sessionID
	rt.mu.Unlock()
	rt.stream.append("session/new", sessionID, "system", "opencode session "+sessionID)
	return sessionID, nil
}

func (m *OpenCodeRuntimeManager) reserveTurn(rt *opencodeRuntime) (*opencodeclient.Client, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.client == nil {
		return nil, fmt.Errorf("opencode runtime lost its connection")
	}
	if rt.status == RuntimeBusy {
		return nil, fmt.Errorf("opencode runtime already has an active turn")
	}
	rt.status = RuntimeBusy
	rt.turnID = fmt.Sprintf("turn_%d", time.Now().UnixNano())
	return rt.client, nil
}

func (m *OpenCodeRuntimeManager) finishTurn(rt *opencodeRuntime, sessionID string, turnErr error) {
	rt.mu.Lock()
	rt.sessionID = sessionID
	rt.turnID = ""
	rt.status = RuntimeIdle
	if turnErr == nil {
		rt.lastErr = ""
	} else {
		rt.lastErr = turnErr.Error()
	}
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.notify(rt)
}

func (m *OpenCodeRuntimeManager) execResult(rt *opencodeRuntime, text, sessionID string) *ExecResult {
	return &ExecResult{
		RuntimeID:         rt.ID,
		Endpoint:          rt.Endpoint,
		FinalText:         text,
		ExternalSessionID: sessionID,
	}
}

// Interrupt cancels the active turn but keeps the runtime and session alive.
func (m *OpenCodeRuntimeManager) Interrupt(runtimeID string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	client := target.client
	sessionID := target.sessionID
	target.mu.Unlock()
	if client == nil || sessionID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Abort(ctx, sessionID)
}

// Snapshot returns the Runtime Console view for one runtime.
func (m *OpenCodeRuntimeManager) Snapshot(runtimeID string) (*RuntimeConsoleSnapshot, bool) {
	m.mu.Lock()
	var target *opencodeRuntime
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

// SendMessage sends a manual turn from the Runtime Console (debug only; does
// not advance the Loop).
func (m *OpenCodeRuntimeManager) SendMessage(runtimeID, text string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	client := target.client
	sessionID := target.sessionID
	model := target.ModelRef
	target.mu.Unlock()
	if client == nil || sessionID == "" {
		return fmt.Errorf("opencode runtime has no session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := client.Prompt(ctx, sessionID, model, text)
	if err != nil {
		return err
	}
	target.mu.Lock()
	target.output = out
	target.mu.Unlock()
	m.notify(target)
	return nil
}

var opencodeRuntimeMgr = newOpenCodeRuntimeManager()

// Package-level exports for the serve layer.
func ListOpencodeRuntimes() []*RuntimeState              { return opencodeRuntimeMgr.List() }
func GetOpencodeRuntime(id string) (*RuntimeState, bool) { return opencodeRuntimeMgr.Get(id) }
func StopOpencodeRuntime(id string) error                { return opencodeRuntimeMgr.Stop(id) }
func InterruptOpencodeRuntime(id string) error           { return opencodeRuntimeMgr.Interrupt(id) }
func SendOpencodeRuntimeMessage(id, text string) error   { return opencodeRuntimeMgr.SendMessage(id, text) }
func SnapshotOpencodeRuntime(id string) (*RuntimeConsoleSnapshot, bool) {
	return opencodeRuntimeMgr.Snapshot(id)
}

// OpenCodePipelineExecutor executes a node via the opencode CLI.
type OpenCodePipelineExecutor struct{}

func (e *OpenCodePipelineExecutor) Name() string { return "opencode" }

func (e *OpenCodePipelineExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if strings.EqualFold(strings.TrimSpace(spec.Mode), "run") {
		return executeOpencodeRun(ctx, spec)
	}
	return opencodeRuntimeMgr.Execute(ctx, spec, onStart)
}

func executeOpencodeRun(ctx context.Context, spec ExecSpec) (*ExecResult, error) {
	executor := &opencodeclient.Executor{}
	opts := opencodeclient.ExecOptions{
		Model:     spec.ModelRef,
		Workspace: spec.Workspace,
	}
	if spec.ContextPolicy != "fresh" && spec.ContextPolicy != "fresh_per_run" {
		opts.ResumeSessionID = strings.TrimSpace(spec.ExternalSessionID)
	}
	start := time.Now()
	res, err := executor.Execute(ctx, opts, buildExecutorPrompt(spec))
	duration := time.Since(start).Milliseconds()
	if err != nil {
		if res == nil {
			return &ExecResult{ExitCode: -1}, err
		}
		return &ExecResult{ExitCode: res.ExitCode, RawStderr: res.Stderr}, err
	}
	result := &ExecResult{
		ExitCode:          res.ExitCode,
		FinalText:         res.Output,
		RawStdout:         res.RawStdout,
		RawStderr:         res.Stderr,
		ExternalSessionID: res.SessionID,
		DurationMs:        duration,
	}
	if res.TotalTokens > 0 {
		result.TokenUsage = &TokenUsage{TotalTokens: int(res.TotalTokens)}
	}
	return result, nil
}
