package orchestrator

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
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
	sseCancel context.CancelFunc
	partLast  map[string]int
	partType  map[string]string

	// pendingPerms holds "permission.updated" prompts parked in "ask" mode,
	// keyed by the opencode permission id. The Runtime Console answers them
	// through AnswerOpencodeRuntimePermission.
	pendingPerms map[string]PermissionRequestInfo
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
	m.stopSSE(target)
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
		m.stopSSE(rt)
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
		PermissionRequests: pendingPermissionList(rt.pendingPerms),
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
		partLast:     make(map[string]int),
		partType:     make(map[string]string),
		pendingPerms: make(map[string]PermissionRequestInfo),
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

	bin := opencodeclient.DiscoverBin()
	if bin == "" {
		bin = "opencode"
	}
	args := []string{"serve", "--port", fmt.Sprint(port), "--hostname", "127.0.0.1"}
	cmd := newRetainedRuntimeCommand(ctx, bin, args...)
	if spec.Workspace != "" {
		cmd.Dir = spec.Workspace
	}
	// opencode serve has no --auto flag (only `opencode run` and the TUI
	// accept it), so automation is expressed through an injected inline
	// config. The "question" tool is always denied: it exists to wait for a
	// human answer the orchestrator can never provide, so the model must
	// decide on its own instead of blocking the pipeline. In auto/yolo mode
	// every other permission is allowed up front; in ask mode the user's
	// permission rules stay active and requests surface in the Runtime
	// Console as approval cards.
	if content := opencodePermissionConfig(spec); content != "" {
		cmd.Env = append(os.Environ(), "OPENCODE_CONFIG_CONTENT="+content)
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	slog.Info("opencode serve: spawning", "bin", bin, "port", port, "workspace", spec.Workspace)
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

	// Wait for the server health endpoint. The polling loop also watches
	// rt.done: the watcher closes it when the process exits, so a crash on
	// startup (bad binary, port conflict, missing login) fails fast with the
	// captured stderr instead of spinning for the full 60s against a corpse.
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
		select {
		case <-rt.done:
			rt.mu.Lock()
			exitErr := rt.lastErr
			rt.mu.Unlock()
			if exitErr == "" {
				exitErr = "process exited"
			}
			diagnostic := truncateStderr(stderr.String())
			m.Stop(rt.ID)
			return nil, fmt.Errorf("opencode serve exited before becoming healthy: %s; stderr: %s", exitErr, diagnostic)
		default:
		}
		if time.Now().After(deadline) {
			diagnostic := truncateStderr(stderr.String())
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
	slog.Info("opencode serve: healthy", "port", port, "runtime", rt.ID)
	m.notify(rt)
	m.subscribeSSE(ctx, rt)
	return rt, nil
}

// truncateStderr keeps startup diagnostics bounded; opencode banners can
// exceed what is useful in an error string.
func truncateStderr(s string) string {
	if len(s) > 2000 {
		return s[:2000] + "..."
	}
	return s
}

func (m *OpenCodeRuntimeManager) dropRuntime(key string, rt *opencodeRuntime) {
	m.mu.Lock()
	if current := m.runtimes[key]; current == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
}

// opencodePermissionConfig builds the OPENCODE_CONFIG_CONTENT inline config
// for a node's approval mode (see the spawn comment in ensure).
func opencodePermissionConfig(spec ExecSpec) string {
	const questionDeny = `{"permission":{"question":"deny"}}`
	if spec.ApprovalMode == "ask" {
		return questionDeny
	}
	return `{"permission":{"*":"allow","question":"deny"}}`
}

// parkPermission stores one parked opencode permission prompt and wakes the
// Runtime Console through the state sink.
func (m *OpenCodeRuntimeManager) parkPermission(rt *opencodeRuntime, id, title, pattern string, askedAt time.Time) {
	info := PermissionRequestInfo{
		RequestID: id,
		ToolName:  title,
		ToolInput: pattern,
		AskedAt:   askedAt,
	}
	rt.mu.Lock()
	if rt.pendingPerms == nil {
		rt.pendingPerms = make(map[string]PermissionRequestInfo)
	}
	rt.pendingPerms[id] = info
	rt.mu.Unlock()
	m.notify(rt)
}

// AnswerOpencodeRuntimePermission resolves a parked opencode permission
// request with "once", "always" or "reject" (the opencode API's response
// values; "always" remembers the decision for the session).
func AnswerOpencodeRuntimePermission(runtimeID, permissionID, response string) error {
	opencodeRuntimeMgr.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range opencodeRuntimeMgr.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	opencodeRuntimeMgr.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	_, ok := target.pendingPerms[permissionID]
	if ok {
		delete(target.pendingPerms, permissionID)
	}
	client := target.client
	sessionID := target.sessionID
	target.mu.Unlock()
	if !ok {
		return fmt.Errorf("permission request %q not pending on runtime %q", permissionID, runtimeID)
	}
	if client == nil {
		return fmt.Errorf("opencode runtime %q has no client", runtimeID)
	}
	if sessionID == "" {
		return fmt.Errorf("opencode runtime %q has no session", runtimeID)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// The Runtime Console sends allow_once/allow_always/reject; opencode's
	// API speaks once/always/reject.
	if err := client.RespondPermission(ctx, sessionID, permissionID, opencodeResponseForAction(response)); err != nil {
		return err
	}
	opencodeRuntimeMgr.notify(target)
	return nil
}

// opencodeResponseForAction maps the Runtime Console's action vocabulary
// (allow_once / allow_always / reject) to opencode's API responses
// (once / always / reject).
func opencodeResponseForAction(action string) string {
	switch action {
	case "allow_once":
		return "once"
	case "allow_always":
		return "always"
	}
	return action
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
	m.stopSSE(rt)
	m.notify(rt)
	m.mu.Lock()
	if m.runtimes[key] == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	close(rt.done)
}

func (m *OpenCodeRuntimeManager) stopSSE(rt *opencodeRuntime) {
	rt.mu.Lock()
	cancel := rt.sseCancel
	rt.sseCancel = nil
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// opencodeSSEEvent mirrors the opencode /event SSE payload (discriminated by
// "type"; relevant payload fields are kept).
type opencodeSSEEvent struct {
	Type string `json:"type"`
	Properties struct {
		SessionID string `json:"sessionID"`
		MessageID string `json:"messageID"`
		PartID    string `json:"partID"`
		Field     string `json:"field"`
		Delta     string `json:"delta"`
		Part      struct {
			ID    string `json:"id"`
			Type  string `json:"type"`
			Text  string `json:"text"`
			State string `json:"state"`
		} `json:"part"`
		// Permission carries the fields of a "permission.updated" event
		// (opencode Permission type): the approval prompt payload.
		Permission struct {
			ID        string `json:"id"`
			Type      string `json:"type"`
			Title     string `json:"title"`
			Pattern   string `json:"pattern"`
			SessionID string `json:"sessionID"`
			Time      struct {
				Created int64 `json:"created"`
			} `json:"time"`
		} `json:"permission"`
		// PermissionID is the flat field of "permission.replied" events.
		PermissionID string `json:"permissionID"`
	} `json:"properties"`
}

// subscribeSSE opens the opencode instance event stream and feeds console
// events live. The stream is best-effort: it reconnects on errors and is
// cancelled when the runtime stops.
func (m *OpenCodeRuntimeManager) subscribeSSE(ctx context.Context, rt *opencodeRuntime) {
	sseCtx, cancel := context.WithCancel(ctx)
	rt.mu.Lock()
	rt.sseCancel = cancel
	rt.mu.Unlock()
	go func() {
		defer cancel()
		client := &http.Client{}
		for {
			if opencodeRuntimeStopped(rt) {
				return
			}
			req, err := http.NewRequestWithContext(sseCtx, http.MethodGet, rt.Endpoint+"/event", nil)
			if err != nil {
				return
			}
			resp, err := client.Do(req)
			if err != nil {
				if !opencodeSleepAbort(sseCtx, 2*time.Second) {
					return
				}
				continue
			}
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 0, 1<<20), 4<<20)
			for sc.Scan() {
				line := strings.TrimSpace(sc.Text())
				if !strings.HasPrefix(line, "data:") {
					continue
				}
				var ev opencodeSSEEvent
				if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data:"))), &ev); err != nil {
					continue
				}
				m.handleSSEEvent(rt, ev)
			}
			_ = resp.Body.Close()
			if !opencodeSleepAbort(sseCtx, 2*time.Second) {
				return
			}
		}
	}()
}

func opencodeRuntimeStopped(rt *opencodeRuntime) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.status == RuntimeStopped || rt.status == RuntimeError
}

func opencodeSleepAbort(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

// handleSSEEvent routes opencode stream events into the Runtime Console.
// Streaming text arrives as message.part.delta chunks; message.part.updated
// carries the accumulated text (fed as the suffix not yet streamed), which
// also reveals the part type (text vs reasoning).
func (m *OpenCodeRuntimeManager) handleSSEEvent(rt *opencodeRuntime, ev opencodeSSEEvent) {
	if rt.stream == nil {
		return
	}
	switch ev.Type {
	case "message.part.delta":
		if ev.Properties.Field != "text" || ev.Properties.Delta == "" {
			return
		}
		partID := ev.Properties.PartID
		rt.mu.Lock()
		category := "assistant"
		if rt.partType[partID] == "reasoning" {
			category = "reasoning"
		}
		rt.mu.Unlock()
		rt.stream.append("message", partID, category, ev.Properties.Delta)
	case "message.part.updated":
		p := ev.Properties.Part
		if p.ID == "" {
			return
		}
		if p.Type != "text" && p.Type != "reasoning" {
			rt.stream.flushNow()
			return
		}
		category := "assistant"
		if p.Type == "reasoning" {
			category = "reasoning"
		}
		rt.mu.Lock()
		rt.partType[p.ID] = p.Type
		last := rt.partLast[p.ID]
		rt.mu.Unlock()
		if len(p.Text) > last {
			rt.stream.append("message", p.ID, category, p.Text[last:])
			rt.mu.Lock()
			rt.partLast[p.ID] = len(p.Text)
			rt.mu.Unlock()
		} else {
			rt.stream.flushNow()
		}
	case "message.part.removed":
		rt.stream.flushNow()
	case "session.idle", "message.updated", "session.status":
		rt.stream.flushNow()
	case "permission.updated":
		m.handlePermissionUpdated(rt, ev)
	case "permission.replied":
		// The request was answered elsewhere (e.g. auto mode); keep the
		// console informed and drop any stale parked card.
		permID := ev.Properties.PermissionID
		rt.mu.Lock()
		if permID != "" {
			delete(rt.pendingPerms, permID)
		}
		rt.mu.Unlock()
		m.notify(rt)
	}
}

// handlePermissionUpdated routes one opencode "permission.updated" event.
// ask mode parks the request for a Runtime Console card; auto/yolo mode
// answers it immediately (the injected config normally prevents requests
// from reaching this point at all, this is the safety net).
func (m *OpenCodeRuntimeManager) handlePermissionUpdated(rt *opencodeRuntime, ev opencodeSSEEvent) {
	p := ev.Properties.Permission
	if p.ID == "" {
		return
	}
	askedAt := time.UnixMilli(p.Time.Created)
	rt.mu.Lock()
	approval := rt.ApprovalMode
	rt.mu.Unlock()
	if strings.EqualFold(approval, "ask") {
		m.parkPermission(rt, p.ID, p.Title, p.Pattern, askedAt)
		return
	}
	client := rt.client
	sessionID := p.SessionID
	if sessionID == "" {
		rt.mu.Lock()
		sessionID = rt.sessionID
		rt.mu.Unlock()
	}
	if client == nil || sessionID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// "always" mirrors mimo's allow_always: remember the decision for the
	// session so the same tool does not re-prompt every call.
	if err := client.RespondPermission(ctx, sessionID, p.ID, "always"); err != nil {
		rt.mu.Lock()
		rt.lastErr = "auto-approve failed: " + err.Error()
		rt.mu.Unlock()
	}
	m.notify(rt)
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
	budget := opencodeDiscipline{}.turnBudget()
	if spec.TurnTimeout > 0 {
		budget = spec.TurnTimeout
	}
	turnCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	turnStart := time.Now()
	discipline := opencodeDiscipline{}
	denyTools := discipline.denyTools(spec)
	text, promptErr := client.Prompt(turnCtx, sessionID, spec.ModelRef, discipline.system(spec.ToolsReadOnly), spec.Prompt, denyTools)
	// Only a budget expiry (not a transport/API error) is recoverable: the
	// model may have streamed a usable partial document before the deadline.
	// Any other failure aborts nothing and surfaces as-is.
	if promptErr != nil && ctx.Err() == nil && errors.Is(turnCtx.Err(), context.DeadlineExceeded) {
		// Abort so the runtime is left usable and the pipeline fails fast
		// instead of stalling on the wedged turn.
		abortCtx, abortCancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer abortCancel()
		_ = client.Abort(abortCtx, sessionID)
		// Recover partial output produced by THIS turn only: the message must
		// be an assistant text created after the turn started. Without the
		// anchor, a reused session could silently substitute a previous
		// turn's document for the aborted one.
		if history, histErr := client.History(abortCtx, sessionID); histErr == nil {
			anchorMs := turnStart.Add(-time.Second).UnixMilli()
			for i := len(history) - 1; i >= 0; i-- {
				if history[i].Role == "assistant" && history[i].CreatedMs >= anchorMs && strings.TrimSpace(history[i].Text) != "" {
					text = strings.TrimSpace(history[i].Text)
					promptErr = nil
					break
				}
			}
		}
	}
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
	out, err := client.Prompt(ctx, sessionID, model, "", text, nil)
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
	return e.ExecuteWithProgress(ctx, spec, onStart, nil)
}

// ExecuteWithProgress runs an opencode node, forwarding each non-empty stdout
// line to onLine when set (run mode only; serve mode delegates without
// streaming).
func (e *OpenCodePipelineExecutor) ExecuteWithProgress(ctx context.Context, spec ExecSpec, onStart func(string, int), onLine func(line string)) (*ExecResult, error) {
	if strings.EqualFold(strings.TrimSpace(spec.Mode), "run") {
		return executeOpencodeRun(ctx, spec, onLine)
	}
	return opencodeRuntimeMgr.Execute(ctx, spec, onStart)
}

func executeOpencodeRun(ctx context.Context, spec ExecSpec, onLine func(line string)) (*ExecResult, error) {
	executor := &opencodeclient.Executor{}
	opts := opencodeclient.ExecOptions{
		Model:     spec.ModelRef,
		Workspace: spec.Workspace,
		MaxSteps:  spec.MaxSteps,
		Variant:   spec.ReasoningEffort,
		OnLine:    onLine,
		// opencode run has no programmatic permission-reply channel (unlike
		// mimo ACP / claude SDK), so an ask would hang the one-shot process
		// forever. Always auto-approve non-denied requests and hard-deny the
		// "question" tool: a model that wants to ask the user gets a tool
		// rejection and must decide on its own instead of blocking a pipeline.
		AutoApprove: true,
		PermissionConfig: `{"permission":{"question":"deny"}}`,
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

// opencodeDiscipline bounds each serve-mode turn and hardens weak models
// against the runaway "explore forever" loop that previously stalled
// orchestrator pipelines until the HTTP client timed out (10 minutes).
type opencodeDiscipline struct{}

// turnBudget is the hard per-turn deadline before the turn is aborted.
func (opencodeDiscipline) turnBudget() time.Duration { return 300 * time.Second }

// denyTools returns the serve-API tools map for the given spec: nil keeps the
// server default; a named deny list keeps read-only exploration while blocking
// everything that mutates state or runs commands (bash, edits, writes, moves,
// sub-agents, web fetch/search, MCP).
func (opencodeDiscipline) denyTools(spec ExecSpec) map[string]bool {
	if spec.ToolsReadOnly {
		return map[string]bool{
			"bash":      false,
			"edit":      false,
			"write":     false,
			"move":      false,
			"patch":     false,
			"create":    false,
			"delete":    false,
			"task":      false,
			"webfetch":  false,
			"websearch": false,
			"mcp__*":    false,
		}
	}
	return nil
}

// system returns the system-slot discipline prompt applied to every turn.
// The serve API's "system" field is higher authority than the user prompt,
// so this lands before anything node-specific the pipeline injected. With
// readOnly the model may inspect the codebase, just never change anything.
func (opencodeDiscipline) system(readOnly bool) string {
	if readOnly {
		return `You are a node in an automated multi-agent pipeline. Your final response text IS the deliverable: it will be saved verbatim as a document and passed to the next node. You cannot write files, run commands, or use any tool except read-only exploration (read, grep, glob). Writing and executing are other nodes' jobs.

Rules:
1. Read the codebase as needed to ground your answer, but be economical: at most 5 reads per turn, and never repeat the same exploration twice.
2. Answer directly and completely. Never narrate your plan, never restate the task, never ask rhetorical questions, never reply "I'll explore the codebase first" — if you need to look, just do it once and then produce the deliverable.
3. If information is insufficient, produce the best possible answer based on what you have and clearly mark assumptions — do not stall.
4. Output only the deliverable content (the document text). No preamble, no summary of steps taken, no trailing commentary.
5. Vision: the current model may not support image input, so image tools (read_image etc.) will fail and are forbidden. If the task requires reading images and the prompt provides a "辅助手（Helper Agent）" delegation section, delegate there; otherwise state plainly that you cannot read the image and never fabricate image content.`
	}
	return `You are a node in an automated multi-agent pipeline. Your final response text IS the deliverable: it will be saved verbatim as a document and passed to the next node. You cannot write files — writing is the executor node's job. 

Rules:
1. Answer directly and completely in one pass. Never narrate your plan, never restate the task, never ask rhetorical questions, never reply "I'll explore the codebase first".
2. Limit exploration: at most 3 file reads. If the answer can be produced from the task text alone, produce it immediately.
3. If information is insufficient, produce the best possible answer based on what you have and clearly mark assumptions — do not stall.
4. Output only the deliverable content (the document text). No preamble, no summary of steps taken, no trailing commentary.
5. Vision: the current model may not support image input, so image tools (read_image etc.) will fail and are forbidden. If the task requires reading images and the prompt provides a "辅助手（Helper Agent）" delegation section, delegate there; otherwise state plainly that you cannot read the image and never fabricate image content.`
}
