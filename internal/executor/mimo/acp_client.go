// Package mimo implements the Agent Client Protocol (ACP) client used by the
// orchestrator to drive a retained `mimo acp` runtime over its stdio pipes.
//
// Transport note: the current mimo build exposes ACP as JSON-RPC 2.0 over
// stdin/stdout (newline-delimited). The ACP SDK also defines WebSocket and
// Streamable-HTTP transports for remote agents; this client isolates the wire
// behind a pair of io.Reader/io.Writer so a WebSocket transport can be plugged
// in later without changing the orchestrator-facing API.
package mimo

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTurnInterrupted is returned when a prompt turn was cancelled by the
// operator (session/cancel) rather than completing normally. The retained
// session stays valid for a later prompt.
var ErrTurnInterrupted = errors.New("mimo acp turn interrupted")

// AcpEvent is a provider-neutral ACP notification record. Update is the
// sessionUpdate discriminator (agent_message_chunk, message_part_completed,
// tool_call_update, usage_update, ...) and Payload carries the raw JSON-RPC
// params verbatim so callers can render future update kinds without coupling
// to every schema revision.
type AcpEvent struct {
	At        time.Time `json:"at"`
	Method    string    `json:"method"`
	SessionID string    `json:"sessionId,omitempty"`
	Update    string    `json:"update,omitempty"`
	MessageID string    `json:"messageId,omitempty"`
	Text      string    `json:"text,omitempty"`
	Payload   string    `json:"payload,omitempty"`
}

// TokenUsage mirrors the ACP usage object returned by session/prompt.
type TokenUsage struct {
	Total       int64 `json:"totalTokens,omitempty"`
	Input       int64 `json:"inputTokens,omitempty"`
	Output      int64 `json:"outputTokens,omitempty"`
	Thought     int64 `json:"thoughtTokens,omitempty"`
	CachedRead  int64 `json:"cachedReadTokens,omitempty"`
	CachedWrite int64 `json:"cachedWriteTokens,omitempty"`
}

// PromptResult is the outcome of one ACP session/prompt turn. Text is the
// accumulated assistant text extracted from agent_message_chunk updates.
type PromptResult struct {
	SessionID  string
	StopReason string
	Text       string
	Usage      *TokenUsage
	Raw        json.RawMessage
}

// PermissionPolicy decides how the client answers an agent requestPermission
// call. It returns the ACP option id to select: "allow_always", "allow_once"
// or "reject". A nil policy rejects every permission request. Returning
// ErrPermissionPending parks the request until a human answers it through
// AnswerPermission; the reply is delayed, the agent keeps waiting.
type PermissionPolicy func(sessionID string, toolCall json.RawMessage) (string, error)

// ErrPermissionPending marks a permission request that the policy cannot
// answer automatically. The client parks the request (no JSON-RPC reply yet)
// and hands it to the onPermission hook so a human can decide later.
var ErrPermissionPending = errors.New("permission pending human decision")

// PermissionRequest is one agent-initiated permission prompt that has been
// parked for a human decision. EnvID is the JSON-RPC request id the reply
// must carry.
type PermissionRequest struct {
	EnvID     json.RawMessage
	SessionID string
	ToolName  string
	ToolInput json.RawMessage
	AskedAt   time.Time
}

type rpcError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *rpcError) Error() string {
	if e == nil || e.Message == "" {
		return "mimo acp rpc error"
	}
	return e.Message
}

type envelope struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type activePrompt struct {
	id          int64
	sessionID   string
	messageText map[string]string
	messageSeq  []string
}

// AcpClient owns one ACP JSON-RPC connection to a `mimo acp` process.
// Requests are correlated by JSON-RPC id while agent notifications are
// delivered to the optional event hook and, during a prompt turn, accumulated
// into the prompt result text.
type AcpClient struct {
	r       io.Reader
	w       io.Writer
	onEvent func(AcpEvent)
	permit  PermissionPolicy
	// onPermission receives parked permission requests (ErrPermissionPending).
	// The hook must eventually call AnswerPermission with the request's
	// EnvID; until then the agent waits and no other reply is written.
	onPermission func(PermissionRequest)

	writeMu     sync.Mutex
	mu          sync.Mutex
	nextID      atomic.Int64
	pending     map[int64]chan envelope
	active      *activePrompt
	closed      chan struct{}
	closeOnce   sync.Once
	terminalErr error
}

// NewAcpClient creates a client over the given pipes and starts reading.
// Callers must pass the write end of the stdin pipe and the read end of the
// stdout pipe of the spawned `mimo acp` process.
func NewAcpClient(r io.Reader, w io.Writer, onEvent func(AcpEvent)) *AcpClient {
	c := &AcpClient{
		r:       r,
		w:       w,
		onEvent: onEvent,
		pending: make(map[int64]chan envelope),
		closed:  make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// SetPermissionPolicy overrides the default reject-all permission handling.
func (c *AcpClient) SetPermissionPolicy(policy PermissionPolicy) {
	c.mu.Lock()
	c.permit = policy
	c.mu.Unlock()
}

// SetPermissionHook registers the receiver for parked permission requests
// (policy returned ErrPermissionPending). At most one hook is kept.
func (c *AcpClient) SetPermissionHook(hook func(PermissionRequest)) {
	c.mu.Lock()
	c.onPermission = hook
	c.mu.Unlock()
}

// AnswerPermission replies to a parked permission request with the chosen ACP
// option id ("allow_always", "allow_once" or "reject"). Safe to call from any
// goroutine; the request must still be pending (no reply written yet).
func (c *AcpClient) AnswerPermission(envID json.RawMessage, optionID string) error {
	switch optionID {
	case "allow_always", "allow_once", "reject":
	default:
		return fmt.Errorf("mimo acp: invalid permission option %q", optionID)
	}
	return c.write(map[string]any{"id": envID, "result": map[string]any{
		"outcome": map[string]any{"outcome": "selected", "optionId": optionID},
	}})
}

func (c *AcpClient) write(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return errors.New("mimo acp connection is closed")
	default:
	}
	_, err = c.w.Write(append(data, '\n'))
	return err
}

func (c *AcpClient) emit(event AcpEvent) {
	if c.onEvent != nil {
		c.onEvent(event)
	}
}

// Call sends one JSON-RPC request and waits for its correlated response.
func (c *AcpClient) Call(ctx context.Context, method string, params any, out any) error {
	id := c.nextID.Add(1)
	response := make(chan envelope, 1)
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return fmt.Errorf("mimo acp connection is closed: %w", c.terminalErr)
	default:
	}
	c.pending[id] = response
	c.mu.Unlock()

	if err := c.write(map[string]any{"id": id, "method": method, "params": params}); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return err
	}

	var env envelope
	select {
	case env = <-response:
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	case <-c.closed:
		c.mu.Lock()
		delete(c.pending, id)
		err := c.terminalErr
		c.mu.Unlock()
		if err == nil {
			err = errors.New("mimo acp connection closed")
		}
		return err
	}
	if env.Error != nil {
		return fmt.Errorf("mimo acp %s: %w", method, env.Error)
	}
	if out != nil && len(env.Result) > 0 {
		if err := json.Unmarshal(env.Result, out); err != nil {
			return fmt.Errorf("mimo acp %s: decode result: %w", method, err)
		}
	}
	return nil
}

// Initialize performs the ACP protocol handshake.
func (c *AcpClient) Initialize(ctx context.Context) error {
	var ignored json.RawMessage
	return c.Call(ctx, "initialize", map[string]any{
		"protocolVersion":    1,
		"clientCapabilities": map[string]any{},
		"clientInfo":         map[string]any{"name": "reasonix-orchestrator", "version": "1"},
	}, &ignored)
}

// NewSession creates a session in the given working directory and returns its
// session id.
func (c *AcpClient) NewSession(ctx context.Context, cwd string) (string, error) {
	var res struct {
		SessionID string `json:"sessionId"`
	}
	if err := c.Call(ctx, "session/new", map[string]any{"cwd": cwd, "mcpServers": []any{}}, &res); err != nil {
		return "", err
	}
	if strings.TrimSpace(res.SessionID) == "" {
		return "", errors.New("mimo acp session/new returned no sessionId")
	}
	return res.SessionID, nil
}

// LoadSession resumes an existing session previously persisted by mimo. This
// is the retained-runtime equivalent of Codex thread/resume.
func (c *AcpClient) LoadSession(ctx context.Context, cwd, sessionID string) (string, error) {
	var res struct {
		SessionID string `json:"sessionId"`
	}
	if err := c.Call(ctx, "session/load", map[string]any{"cwd": cwd, "sessionId": sessionID, "mcpServers": []any{}}, &res); err != nil {
		return "", err
	}
	if strings.TrimSpace(res.SessionID) == "" {
		return sessionID, nil
	}
	return res.SessionID, nil
}

// SetConfigOption sets one session configuration option, for example
// configId="model" value="xiaomi/mimo-v2.5". Configuration changes are
// best-effort; the caller may ignore errors and fall back to the session
// default.
func (c *AcpClient) SetConfigOption(ctx context.Context, sessionID, configID, value string) error {
	var ignored json.RawMessage
	return c.Call(ctx, "session/set_config_option", map[string]any{
		"sessionId": sessionID,
		"configId":  configID,
		"value":     value,
	}, &ignored)
}

// SetMode switches the session agent mode (for example "build" or "plan").
func (c *AcpClient) SetMode(ctx context.Context, sessionID, modeID string) error {
	var ignored json.RawMessage
	return c.Call(ctx, "session/set_mode", map[string]any{
		"sessionId": sessionID,
		"modeId":    modeID,
	}, &ignored)
}

// Prompt sends one user message and streams the turn until the agent responds.
// Only one prompt may be active per client at a time; the orchestrator enforces
// this with its per-runtime reserveTurn gate.
func (c *AcpClient) Prompt(ctx context.Context, sessionID, text string) (*PromptResult, error) {
	id := c.nextID.Add(1)
	response := make(chan envelope, 1)
	c.mu.Lock()
	if c.active != nil {
		c.mu.Unlock()
		return nil, errors.New("mimo acp: a prompt turn is already active")
	}
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, fmt.Errorf("mimo acp connection is closed: %w", c.terminalErr)
	default:
	}
	c.pending[id] = response
	c.active = &activePrompt{id: id, sessionID: sessionID, messageText: make(map[string]string)}
	c.mu.Unlock()

	if err := c.write(map[string]any{"id": id, "method": "session/prompt", "params": map[string]any{
		"sessionId": sessionID,
		"prompt":    []any{map[string]any{"type": "text", "text": text}},
	}}); err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.active = nil
		c.mu.Unlock()
		return nil, err
	}

	var env envelope
	select {
	case env = <-response:
	case <-ctx.Done():
		// Best-effort abort so the agent does not keep burning tokens after
		// the orchestrator node times out.
		_ = c.Cancel(sessionID)
		c.mu.Lock()
		delete(c.pending, id)
		c.active = nil
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.closed:
		c.mu.Lock()
		c.active = nil
		err := c.terminalErr
		c.mu.Unlock()
		if err == nil {
			err = errors.New("mimo acp connection closed")
		}
		return nil, err
	}

	c.mu.Lock()
	act := c.active
	c.active = nil
	delete(c.pending, id)
	c.mu.Unlock()

	if env.Error != nil {
		return &PromptResult{SessionID: sessionID, Text: c.finalText(act)}, fmt.Errorf("mimo acp session/prompt: %w", env.Error)
	}
	result, err := parsePromptResult(env.Result)
	if err != nil {
		return &PromptResult{SessionID: sessionID, Text: c.finalText(act)}, err
	}
	result.SessionID = sessionID
	result.Text = c.finalText(act)
	return result, nil
}

func (c *AcpClient) finalText(act *activePrompt) string {
	if act == nil {
		return ""
	}
	var parts []string
	for _, messageID := range act.messageSeq {
		if text := strings.TrimSpace(act.messageText[messageID]); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "")
}

// Cancel aborts the current prompt turn (ACP notification). The retained
// session remains valid for a later prompt.
func (c *AcpClient) Cancel(sessionID string) error {
	return c.write(map[string]any{"jsonrpc": "2.0", "method": "session/cancel", "params": map[string]any{"sessionId": sessionID}})
}

// Close terminates the connection by closing the write side (EOF on the ACP
// process stdin) and failing all pending calls.
func (c *AcpClient) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.mu.Lock()
		pending := c.pending
		c.pending = make(map[int64]chan envelope)
		if closer, ok := c.w.(io.Closer); ok {
			_ = closer.Close()
		}
		c.mu.Unlock()
		for _, ch := range pending {
			ch <- envelope{Error: &rpcError{Code: -32000, Message: "mimo acp connection closed"}}
			close(ch)
		}
	})
}

func parsePromptResult(raw json.RawMessage) (*PromptResult, error) {
	var body struct {
		StopReason string `json:"stopReason"`
		Usage      struct {
			Total       int64 `json:"totalTokens"`
			Input       int64 `json:"inputTokens"`
			Output      int64 `json:"outputTokens"`
			Thought     int64 `json:"thoughtTokens"`
			CachedRead  int64 `json:"cachedReadTokens"`
			CachedWrite int64 `json:"cachedWriteTokens"`
		} `json:"usage"`
	}
	if len(raw) == 0 {
		return &PromptResult{StopReason: "end_turn"}, nil
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("mimo acp session/prompt result: %w", err)
	}
	result := &PromptResult{StopReason: body.StopReason, Raw: append(json.RawMessage(nil), raw...)}
	if body.Usage.Total > 0 || body.Usage.Input > 0 || body.Usage.Output > 0 || body.Usage.Thought > 0 {
		result.Usage = &TokenUsage{
			Total: body.Usage.Total, Input: body.Usage.Input, Output: body.Usage.Output,
			Thought: body.Usage.Thought, CachedRead: body.Usage.CachedRead, CachedWrite: body.Usage.CachedWrite,
		}
	}
	return result, nil
}

func (c *AcpClient) readLoop() {
	defer c.Close()
	scanner := bufio.NewScanner(c.r)
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var env envelope
		if err := json.Unmarshal([]byte(line), &env); err != nil {
			c.emit(AcpEvent{At: time.Now(), Method: "protocol/error", Text: err.Error(), Payload: line})
			continue
		}
		hasID := len(env.ID) > 0 && string(env.ID) != "null"
		switch {
		case hasID && env.Method != "":
			// Server-initiated request (for example session/request_permission).
			c.handleServerRequest(env)
		case hasID:
			c.mu.Lock()
			ch := c.pending[env.IDint()]
			delete(c.pending, env.IDint())
			c.mu.Unlock()
			if ch != nil {
				ch <- env
				close(ch)
			}
		case env.Method != "":
			c.handleNotification(env)
		}
	}
	if err := scanner.Err(); err != nil {
		c.mu.Lock()
		if c.terminalErr == nil {
			c.terminalErr = err
		}
		c.mu.Unlock()
	}
}

func (e envelope) IDint() int64 {
	var id int64
	_ = json.Unmarshal(e.ID, &id)
	return id
}

func (c *AcpClient) handleNotification(env envelope) {
	event := AcpEvent{At: time.Now(), Method: env.Method, Payload: string(env.Params)}
	var params struct {
		SessionID string `json:"sessionId"`
		Update    struct {
			SessionUpdate string `json:"sessionUpdate"`
			MessageID     string `json:"messageId"`
			Content       struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
			Part struct {
				Type string `json:"type"`
				Text string `json:"text"`
				ID   string `json:"id"`
			} `json:"part"`
		} `json:"update"`
	}
	_ = json.Unmarshal(env.Params, &params)
	event.SessionID = params.SessionID
	event.Update = params.Update.SessionUpdate
	event.MessageID = params.Update.MessageID
	event.Text = params.Update.Content.Text
	if event.Text == "" && params.Update.Part.Type == "text" {
		event.Text = params.Update.Part.Text
	}

	switch params.Update.SessionUpdate {
	case "agent_message_chunk":
		c.mu.Lock()
		if act := c.active; act != nil && act.sessionID == params.SessionID && strings.TrimSpace(params.Update.Content.Text) != "" {
			if _, seen := act.messageText[params.Update.MessageID]; !seen {
				act.messageSeq = append(act.messageSeq, params.Update.MessageID)
			}
			act.messageText[params.Update.MessageID] += params.Update.Content.Text
		}
		c.mu.Unlock()
	case "message_part_completed":
		c.mu.Lock()
		if act := c.active; act != nil && act.sessionID == params.SessionID && params.Update.Part.Type == "text" && strings.TrimSpace(params.Update.Part.Text) != "" {
			messageID := params.Update.MessageID
			if _, seen := act.messageText[messageID]; !seen {
				act.messageSeq = append(act.messageSeq, messageID)
			}
			if strings.TrimSpace(act.messageText[messageID]) == "" {
				act.messageText[messageID] = params.Update.Part.Text
			}
		}
		c.mu.Unlock()
	}
	c.emit(event)
}

// handleServerRequest answers agent-initiated JSON-RPC requests. The only
// request the orchestrator client must answer is session/request_permission;
// without a reply the agent waits forever on the permission queue.
func (c *AcpClient) handleServerRequest(env envelope) {
	switch env.Method {
	case "session/request_permission":
		var params struct {
			SessionID string          `json:"sessionId"`
			ToolCall  json.RawMessage `json:"toolCall"`
		}
		_ = json.Unmarshal(env.Params, &params)
		optionID := "reject"
		var replyErr *rpcError
		c.mu.Lock()
		policy := c.permit
		hook := c.onPermission
		c.mu.Unlock()
		if policy != nil {
			chosen, err := policy(params.SessionID, params.ToolCall)
			if errors.Is(err, ErrPermissionPending) {
				// Park the request: no reply yet. The hook (usually the
				// orchestrator runtime) stores it and answers via
				// AnswerPermission once a human decides.
				if hook != nil {
					hook(PermissionRequest{
						EnvID:     env.ID,
						SessionID: params.SessionID,
						ToolName:  permissionToolName(params.ToolCall),
						ToolInput: params.ToolCall,
						AskedAt:   time.Now(),
					})
					return
				}
				// No hook to answer later: stay conservative and reject.
				_ = c.write(map[string]any{"id": env.ID, "result": map[string]any{
					"outcome": map[string]any{"outcome": "selected", "optionId": "reject"},
				}})
				return
			}
			if err != nil {
				replyErr = &rpcError{Code: -32603, Message: err.Error()}
			} else if chosen != "" {
				optionID = chosen
			}
		}
		if replyErr != nil {
			_ = c.write(map[string]any{"id": env.ID, "error": replyErr})
			return
		}
		_ = c.write(map[string]any{"id": env.ID, "result": map[string]any{
			"outcome": map[string]any{"outcome": "selected", "optionId": optionID},
		}})
	default:
		// Unknown agent request: respond with method-not-found so the agent
		// does not wait indefinitely for a reply.
		_ = c.write(map[string]any{"id": env.ID, "error": &rpcError{Code: -32601, Message: "method not found: " + env.Method}})
	}
}

// permissionToolName extracts a display name from an ACP toolCall payload.
// The ACP toolCall object carries a "title" (e.g. "bash"); fall back to a
// "name"/"tool" field or a generic label when it is missing.
func permissionToolName(toolCall json.RawMessage) string {
	var obj struct {
		Title string `json:"title"`
		Name  string `json:"name"`
		Tool  string `json:"tool"`
	}
	_ = json.Unmarshal(toolCall, &obj)
	if obj.Title != "" {
		return obj.Title
	}
	if obj.Name != "" {
		return obj.Name
	}
	if obj.Tool != "" {
		return obj.Tool
	}
	return "unknown-tool"
}

// Done exposes the connection-closed channel for tests and lifecycle joins.
func (c *AcpClient) Done() <-chan struct{} {
	return c.closed
}
