// Package codex implements the Codex CLI integration used by the orchestrator.
package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// AppServerEvent is a JSON-RPC notification emitted by a retained Codex
// app-server runtime. Params is kept verbatim so callers can render new Codex
// notification types without coupling the executor to every schema revision.
type AppServerEvent struct {
	At     time.Time       `json:"at"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
	Text   string          `json:"text,omitempty"`
}

// AppServerClient owns one JSON-RPC WebSocket connection to `codex app-server`.
// Calls are correlated by JSON-RPC ID while notifications are delivered to the
// optional event hook and retained in an in-memory bounded history by the
// orchestrator runtime manager.
type AppServerClient struct {
	conn *websocket.Conn

	writeMu     sync.Mutex
	mu          sync.Mutex
	nextID      atomic.Int64
	pending     map[int64]chan rpcEnvelope
	turns       map[string]chan turnCompletion
	completed   map[string]turnCompletion
	turnText    map[string]string
	terminalErr error
	closed      chan struct{}
	closeOnce   sync.Once

	onEvent func(AppServerEvent)
}

type rpcEnvelope struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type turnCompletion struct {
	TurnID string
	Status string
	Text   string
	Raw    json.RawMessage
	Err    error
}

// ThreadOptions controls a newly created or resumed Codex thread.
type ThreadOptions struct {
	Workspace       string
	Model           string
	ApprovalPolicy  string
	ReasoningEffort string
}

// DialAppServer connects to an already-listening `codex app-server` endpoint
// and performs the protocol initialize handshake.
func DialAppServer(ctx context.Context, endpoint string, onEvent func(AppServerEvent)) (*AppServerClient, error) {
	conn, _, err := websocket.DefaultDialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return nil, err
	}
	c := &AppServerClient{
		conn:      conn,
		pending:   make(map[int64]chan rpcEnvelope),
		turns:     make(map[string]chan turnCompletion),
		completed: make(map[string]turnCompletion),
		turnText:  make(map[string]string),
		closed:    make(chan struct{}),
		onEvent:   onEvent,
	}
	go c.readLoop()

	var ignored json.RawMessage
	if err := c.Call(ctx, "initialize", map[string]any{
		"clientInfo": map[string]any{
			"name":    "reasonix-orchestrator",
			"version": "1",
		},
		"capabilities": map[string]any{},
	}, &ignored); err != nil {
		c.Close()
		return nil, fmt.Errorf("codex app-server initialize: %w", err)
	}
	return c, nil
}

func (c *AppServerClient) readLoop() {
	defer c.Close()
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			c.failPending(err)
			return
		}
		var env rpcEnvelope
		if err := json.Unmarshal(data, &env); err != nil {
			c.emit(AppServerEvent{At: time.Now(), Method: "protocol/error", Text: err.Error()})
			continue
		}
		if len(env.ID) > 0 && string(env.ID) != "null" {
			var id int64
			if err := json.Unmarshal(env.ID, &id); err == nil {
				c.mu.Lock()
				ch := c.pending[id]
				delete(c.pending, id)
				c.mu.Unlock()
				if ch != nil {
					ch <- env
					close(ch)
					continue
				}
			}
		}
		if env.Method == "" {
			continue
		}
		event := AppServerEvent{At: time.Now(), Method: env.Method, Params: append(json.RawMessage(nil), env.Params...)}
		event.Text = extractEventText(env.Method, env.Params)
		c.recordTurnText(env.Method, env.Params, event.Text)
		c.emit(event)
		if env.Method == "turn/completed" {
			c.completeTurn(env.Params, event.Text)
		}
	}
}

func (c *AppServerClient) emit(event AppServerEvent) {
	if c.onEvent != nil {
		c.onEvent(event)
	}
}

func (c *AppServerClient) completeTurn(params json.RawMessage, text string) {
	var body struct {
		ThreadID string `json:"threadId"`
		Turn     struct {
			ID     string `json:"id"`
			Status string `json:"status"`
			Error  *struct {
				Message string `json:"message"`
			} `json:"error"`
		} `json:"turn"`
	}
	_ = json.Unmarshal(params, &body)
	if body.Turn.ID == "" {
		return
	}
	c.mu.Lock()
	if streamed := c.turnText[body.Turn.ID]; streamed != "" {
		text = streamed
	}
	delete(c.turnText, body.Turn.ID)
	completion := turnCompletion{TurnID: body.Turn.ID, Status: body.Turn.Status, Text: text, Raw: append(json.RawMessage(nil), params...)}
	if body.Turn.Error != nil && body.Turn.Error.Message != "" {
		completion.Err = fmt.Errorf("codex turn: %s", body.Turn.Error.Message)
	}
	// Store the result before waking a waiter. `turn/completed` is allowed to
	// arrive before the orchestrator enters WaitTurn, so WaitTurn consumes this
	// map as the authoritative completion record and performs final cleanup.
	if _, alreadyCompleted := c.completed[body.Turn.ID]; alreadyCompleted {
		c.mu.Unlock()
		return
	}
	c.completed[body.Turn.ID] = completion
	ch := c.turns[body.Turn.ID]
	c.mu.Unlock()
	if ch != nil {
		ch <- completion
		close(ch)
	}
}

func (c *AppServerClient) recordTurnText(method string, params json.RawMessage, text string) {
	if text == "" || !containsAgentMessage(method) {
		return
	}
	var body struct {
		TurnID string `json:"turnId"`
	}
	if json.Unmarshal(params, &body) != nil || body.TurnID == "" {
		return
	}
	c.mu.Lock()
	c.turnText[body.TurnID] += text
	c.mu.Unlock()
}

func containsAgentMessage(method string) bool {
	return method == "item/agentMessage/delta" || method == "item/agentMessage" || method == "item/completed"
}

func (c *AppServerClient) failPending(err error) {
	c.mu.Lock()
	if c.terminalErr == nil {
		c.terminalErr = err
	}
	pending := c.pending
	c.pending = make(map[int64]chan rpcEnvelope)
	turns := c.turns
	c.turns = make(map[string]chan turnCompletion)
	// Completed turns remain readable after a later connection close. A caller
	// that has not yet entered WaitTurn must still receive the terminal result.
	c.turnText = make(map[string]string)
	c.mu.Unlock()
	for _, ch := range pending {
		ch <- rpcEnvelope{Error: &rpcError{Code: -32000, Message: err.Error()}}
		close(ch)
	}
	for _, ch := range turns {
		ch <- turnCompletion{Err: err}
		close(ch)
	}
}

// Call sends one JSON-RPC request and waits for its correlated response.
func (c *AppServerClient) Call(ctx context.Context, method string, params any, out any) error {
	id := c.nextID.Add(1)
	response := make(chan rpcEnvelope, 1)
	c.mu.Lock()
	select {
	case <-c.closed:
		c.mu.Unlock()
		return fmt.Errorf("codex app-server connection is closed")
	default:
	}
	c.pending[id] = response
	c.mu.Unlock()

	request := map[string]any{"id": id, "method": method, "params": params}
	c.writeMu.Lock()
	err := c.conn.WriteJSON(request)
	c.writeMu.Unlock()
	if err != nil {
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return err
	}

	select {
	case env, ok := <-response:
		if !ok {
			return fmt.Errorf("codex app-server closed while waiting for %s", method)
		}
		if env.Error != nil {
			return fmt.Errorf("codex app-server %s: %s", method, env.Error.Message)
		}
		if out != nil && len(env.Result) != 0 && string(env.Result) != "null" {
			if err := json.Unmarshal(env.Result, out); err != nil {
				return fmt.Errorf("codex app-server %s response: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.pending, id)
		c.mu.Unlock()
		return ctx.Err()
	}
}

// StartThread creates a persisted Codex thread.
func (c *AppServerClient) StartThread(ctx context.Context, opts ThreadOptions) (string, error) {
	params := map[string]any{"cwd": opts.Workspace}
	if opts.Model != "" {
		params["model"] = opts.Model
	}
	if opts.ApprovalPolicy != "" {
		params["approvalPolicy"] = opts.ApprovalPolicy
	}
	var result struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := c.Call(ctx, "thread/start", params, &result); err != nil {
		return "", err
	}
	if result.Thread.ID == "" {
		return "", fmt.Errorf("codex app-server thread/start returned no thread id")
	}
	return result.Thread.ID, nil
}

// ResumeThread reattaches the runtime to a previously persisted Codex thread.
func (c *AppServerClient) ResumeThread(ctx context.Context, threadID string, opts ThreadOptions) (string, error) {
	if threadID == "" {
		return c.StartThread(ctx, opts)
	}
	params := map[string]any{"threadId": threadID}
	if opts.Workspace != "" {
		params["cwd"] = opts.Workspace
	}
	if opts.Model != "" {
		params["model"] = opts.Model
	}
	if opts.ApprovalPolicy != "" {
		params["approvalPolicy"] = opts.ApprovalPolicy
	}
	var result struct {
		Thread struct {
			ID string `json:"id"`
		} `json:"thread"`
	}
	if err := c.Call(ctx, "thread/resume", params, &result); err != nil {
		return "", err
	}
	if result.Thread.ID == "" {
		return "", fmt.Errorf("codex app-server thread/resume returned no thread id")
	}
	return result.Thread.ID, nil
}

// StartTurn submits a text input and returns as soon as Codex accepted it.
func (c *AppServerClient) StartTurn(ctx context.Context, threadID, prompt, model, effort string) (string, error) {
	params := map[string]any{
		"threadId": threadID,
		"input":    []map[string]string{{"type": "text", "text": prompt}},
	}
	if model != "" {
		params["model"] = model
	}
	if effort != "" {
		params["effort"] = effort
	}
	var result struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := c.Call(ctx, "turn/start", params, &result); err != nil {
		return "", err
	}
	if result.Turn.ID == "" {
		return "", fmt.Errorf("codex app-server turn/start returned no turn id")
	}
	c.mu.Lock()
	if c.terminalErr != nil {
		if _, completed := c.completed[result.Turn.ID]; !completed {
			err := c.terminalErr
			c.mu.Unlock()
			return "", fmt.Errorf("codex app-server closed after turn/start: %w", err)
		}
	}
	c.turns[result.Turn.ID] = make(chan turnCompletion, 1)
	c.mu.Unlock()
	return result.Turn.ID, nil
}

// WaitTurn waits for the turn/completed notification. A completion notification
// can contain final items, but the final answer is assembled from streamed
// agent-message notifications to preserve the exact visible response.
func (c *AppServerClient) WaitTurn(ctx context.Context, turnID string) (string, error) {
	c.mu.Lock()
	if done, ok := c.completed[turnID]; ok {
		delete(c.completed, turnID)
		delete(c.turns, turnID)
		c.mu.Unlock()
		return resolveTurnCompletion(turnID, done)
	}
	ch := c.turns[turnID]
	terminalErr := c.terminalErr
	c.mu.Unlock()
	if ch == nil {
		if terminalErr != nil {
			return "", fmt.Errorf("codex app-server closed before turn %q completed: %w", turnID, terminalErr)
		}
		return "", fmt.Errorf("codex app-server does not track turn %q", turnID)
	}
	select {
	case done, ok := <-ch:
		if !ok {
			return "", fmt.Errorf("codex app-server closed before turn %q completed", turnID)
		}
		c.mu.Lock()
		delete(c.completed, turnID)
		delete(c.turns, turnID)
		c.mu.Unlock()
		return resolveTurnCompletion(turnID, done)
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func resolveTurnCompletion(turnID string, done turnCompletion) (string, error) {
	if done.Err != nil {
		return done.Text, done.Err
	}
	status := strings.ToLower(strings.TrimSpace(done.Status))
	if status == "interrupted" || status == "cancelled" || status == "canceled" {
		return done.Text, fmt.Errorf("%w: turn %q", ErrAppServerTurnInterrupted, turnID)
	}
	if status != "" && status != "completed" && status != "complete" {
		return done.Text, fmt.Errorf("codex turn %q ended with status %s", turnID, done.Status)
	}
	return done.Text, nil
}

// InterruptTurn asks the retained server to stop the active turn without
// stopping the runtime or discarding its thread context.
func (c *AppServerClient) InterruptTurn(ctx context.Context, threadID, turnID string) error {
	return c.Call(ctx, "turn/interrupt", map[string]any{"threadId": threadID, "turnId": turnID}, nil)
}

func (c *AppServerClient) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		_ = c.conn.Close()
	})
}

// extractEventText intentionally tolerates app-server schema evolution. It
// extracts user-visible text from known notification shapes and otherwise keeps
// the raw event available to the console.
func extractEventText(method string, params json.RawMessage) string {
	if len(params) == 0 {
		return ""
	}
	var v any
	if json.Unmarshal(params, &v) != nil {
		return ""
	}
	var out []string
	var visit func(any)
	visit = func(x any) {
		switch x := x.(type) {
		case map[string]any:
			if t, _ := x["text"].(string); t != "" {
				out = append(out, t)
			}
			if d, _ := x["delta"].(string); d != "" {
				out = append(out, d)
			}
			for _, child := range x {
				visit(child)
			}
		case []any:
			for _, child := range x {
				visit(child)
			}
		}
	}
	visit(v)
	if len(out) == 0 {
		return ""
	}
	// A generic recursive scan can encounter the same field through wrapper
	// objects only once; joining preserves deltas while avoiding JSON dumping.
	return joinNonEmpty(out)
}

func joinNonEmpty(parts []string) string {
	result := ""
	for _, part := range parts {
		if part == "" {
			continue
		}
		result += part
	}
	return result
}
