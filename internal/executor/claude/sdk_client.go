package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

// Event is a provider-neutral notification record surfaced to the orchestrator
// Runtime Console. Type is the stream-json message type (system, assistant,
// stream_event, result, error, ...) and Text/Reasoning carry display text for
// the console coalescer.
type Event struct {
	At        time.Time
	Type      string
	Subtype   string
	SessionID string
	Text      string
	Reasoning string
	Payload   string
	IsDelta   bool
}

// TurnUsage mirrors the assistant message usage object.
type TurnUsage struct {
	InputTokens  int64 `json:"input_tokens"`
	OutputTokens int64 `json:"output_tokens"`
	CacheRead    int64 `json:"cache_read_input_tokens"`
	CacheWrite   int64 `json:"cache_creation_input_tokens"`
}

// TurnResult is the outcome of one stream-json turn. Text is the assistant
// answer accumulated from content blocks; Reasoning holds thinking blocks.
type TurnResult struct {
	SessionID  string
	Text       string
	Reasoning  string
	StopReason string
	IsError    bool
	Error      string
	Usage      *TurnUsage
	CostUSD    float64
	Raw        json.RawMessage
}

// PermissionPolicy decides how the client answers a CLI permission request
// (subtype "permission" / "can_use_tool"). A nil policy denies every request.
// Returning ErrPermissionPending parks the request until a human answers it
// through AnswerPermission; the control_response is delayed, the CLI keeps
// waiting.
type PermissionPolicy func(sessionID, toolName string, toolInput json.RawMessage) (bool, error)

// ErrPermissionPending marks a permission request that the policy cannot
// answer automatically. The client parks the request (no control_response
// yet) and hands it to the onPermission hook so a human can decide later.
var ErrPermissionPending = errors.New("permission pending human decision")

// PermissionRequest is one CLI permission prompt parked for a human decision.
type PermissionRequest struct {
	RequestID string
	SessionID string
	ToolName  string
	ToolInput json.RawMessage
	AskedAt   time.Time
}

type activeTurn struct {
	text        strings.Builder
	reasoning   strings.Builder
	stop        string
	usage       *TurnUsage
	finished    chan struct{}
	result      *TurnResult
	err         error
	interrupted bool
	sawDelta    bool
}

// SdkClient owns one stream-json stdio connection to a retained `claude -p`
// process. The read loop parses JSON Lines from stdout; Prompt writes one user
// message and waits for the correlated result line.
//
// The Claude CLI processes one user message at a time and emits exactly one
// result line per turn, so the orchestrator runtime serializes turns through
// its reserveTurn gate. Prompt registers the active turn before writing the
// user message, which makes the completed-before-wait race impossible in the
// normal flow; a result that arrives after a waiter already gave up (context
// cancelled mid-turn) is stored as pending and drained by the next Prompt so
// it can never be attributed to a newer turn.
type SdkClient struct {
	r       io.Reader
	w       io.Writer
	onEvent func(Event)
	permit  PermissionPolicy
	// onPermission receives parked permission requests (ErrPermissionPending).
	// The hook must eventually call AnswerPermission with the request's
	// RequestID; until then the CLI waits and no control_response is written.
	onPermission func(PermissionRequest)

	mu          sync.Mutex
	active      *activeTurn
	pending     *TurnResult
	inFlight    bool
	settle      chan struct{}
	sessionID   string
	closed      chan struct{}
	closeOnce   sync.Once
	terminalErr error
	writeMu     sync.Mutex
}

// NewSdkClient creates a client over the given pipes and starts reading.
// Callers pass the write end of the stdin pipe and the read end of the stdout
// pipe of the spawned claude process.
func NewSdkClient(r io.Reader, w io.Writer, onEvent func(Event)) *SdkClient {
	c := &SdkClient{r: r, w: w, onEvent: onEvent, closed: make(chan struct{})}
	go c.readLoop()
	return c
}

// SetPermissionPolicy overrides the default deny-all permission handling.
func (c *SdkClient) SetPermissionPolicy(policy PermissionPolicy) {
	c.mu.Lock()
	c.permit = policy
	c.mu.Unlock()
}

// SetPermissionHook registers the receiver for parked permission requests
// (policy returned ErrPermissionPending). At most one hook is kept.
func (c *SdkClient) SetPermissionHook(hook func(PermissionRequest)) {
	c.mu.Lock()
	c.onPermission = hook
	c.mu.Unlock()
}

// AnswerPermission replies to a parked permission request with an allow/deny
// control_response. Safe to call from any goroutine; the request must still
// be pending (no reply written yet).
func (c *SdkClient) AnswerPermission(requestID string, allow bool) error {
	if strings.TrimSpace(requestID) == "" {
		return fmt.Errorf("claude sdk: empty permission request id")
	}
	resp := map[string]any{
		"type": "control_response",
		"response": map[string]any{
			"subtype":    "success",
			"request_id": requestID,
			"response": map[string]any{
				"behavior": "deny",
				"message":  "denied by orchestrator",
			},
		},
	}
	if allow {
		if inner, ok := resp["response"].(map[string]any); ok {
			inner["response"] = map[string]any{"behavior": "allow"}
		}
	}
	return c.write(resp)
}

func (c *SdkClient) write(msg any) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	select {
	case <-c.closed:
		return errors.New("claude: connection is closed")
	default:
	}
	_, err = c.w.Write(append(data, '\n'))
	return err
}

func (c *SdkClient) emit(event Event) {
	if c.onEvent != nil {
		c.onEvent(event)
	}
}

// Prompt sends one user message and streams the turn until the CLI emits its
// result line. Only one prompt may be active per client; the orchestrator
// enforces this with its per-runtime reserveTurn gate.
func (c *SdkClient) Prompt(ctx context.Context, text string) (*TurnResult, error) {
	c.mu.Lock()
	// A previous turn whose waiter left (context cancelled) may still be in
	// flight on the CLI. Wait for its result line before writing a new user
	// message, so a stale result can never be attributed to a newer turn.
	for c.inFlight {
		settle := c.settle
		c.mu.Unlock()
		select {
		case <-settle:
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-c.closed:
			c.mu.Lock()
			err := c.terminalErr
			c.mu.Unlock()
			if err == nil {
				err = errors.New("claude: connection closed")
			}
			return nil, err
		}
		c.mu.Lock()
	}
	if c.active != nil {
		c.mu.Unlock()
		return nil, errors.New("claude: a prompt turn is already active")
	}
	select {
	case <-c.closed:
		c.mu.Unlock()
		return nil, fmt.Errorf("claude: connection is closed: %w", c.terminalErr)
	default:
	}
	// Drain any stale result left over from an interrupted/abandoned previous
	// turn. The CLI is strictly sequential: one result per user message, so a
	// pending entry can only belong to an older turn whose waiter already left.
	pending := c.pending
	c.pending = nil
	sessionID := c.sessionID
	turn := &activeTurn{finished: make(chan struct{})}
	c.active = turn
	c.inFlight = true
	c.settle = make(chan struct{})
	c.mu.Unlock()

	if pending != nil {
		c.emit(Event{At: time.Now(), Type: "result", Subtype: "drained_pending", SessionID: pending.SessionID, Text: pending.Error, Payload: string(pending.Raw)})
	}

	userMsg := map[string]any{
		"type": "user",
		"message": map[string]any{
			"role":    "user",
			"content": []any{map[string]any{"type": "text", "text": text}},
		},
	}
	if sessionID != "" {
		userMsg["session_id"] = sessionID
	}
	if err := c.write(userMsg); err != nil {
		c.completeTurn(turn, nil, err)
		return nil, err
	}

	select {
	case <-turn.finished:
		return turn.result, turn.err
	case <-ctx.Done():
		// The operator or the loop cancelled the wait. The CLI keeps working on
		// the turn; its result line will arrive later and be stored as pending
		// so it can never complete a newer turn.
		c.mu.Lock()
		if c.active == turn {
			c.active = nil
		}
		c.mu.Unlock()
		return nil, ctx.Err()
	case <-c.closed:
		c.mu.Lock()
		err := c.terminalErr
		c.mu.Unlock()
		if err == nil {
			err = errors.New("claude: connection closed")
		}
		return nil, err
	}
}

// completeTurn stores the outcome on the active turn and wakes its waiter.
func (c *SdkClient) completeTurn(turn *activeTurn, result *TurnResult, err error) {
	turn.result = result
	turn.err = err
	close(turn.finished)
}

// EnablePermissionProtocol asks the CLI to route tool-approval decisions
// through the SDK control protocol (can_use_tool / permission requests) after
// the init handshake. Without it, non-interactive SDK sessions auto-deny tool
// calls, which would make a retained executor unable to edit files or run
// commands. The permission policy then answers every request.
func (c *SdkClient) EnablePermissionProtocol() error {
	return c.write(map[string]any{
		"type":       "control_request",
		"request_id": fmt.Sprintf("init_%d", time.Now().UnixNano()),
		"request": map[string]any{
			"subtype":         "initialize",
			"protocolVersion": "1.0",
			"features":        []any{},
		},
	})
}

// Interrupt asks the CLI to stop the current response. The retained session
// survives; the CLI emits a result line that completes the active turn.
func (c *SdkClient) Interrupt() error {
	c.mu.Lock()
	turn := c.active
	if turn == nil {
		c.mu.Unlock()
		return errors.New("claude: no active turn to interrupt")
	}
	turn.interrupted = true
	c.mu.Unlock()
	return c.write(map[string]any{
		"type":       "control_request",
		"request_id": fmt.Sprintf("interrupt_%d", time.Now().UnixNano()),
		"request":    map[string]any{"subtype": "interrupt"},
	})
}

// Close closes the stdin pipe (graceful shutdown) and fails any active turn.
// The owning runtime is responsible for killing the process tree afterwards.
func (c *SdkClient) Close() {
	c.closeOnce.Do(func() {
		close(c.closed)
		c.mu.Lock()
		turn := c.active
		c.active = nil
		c.inFlight = false
		if c.settle != nil {
			close(c.settle)
			c.settle = nil
		}
		if closer, ok := c.w.(io.Closer); ok {
			_ = closer.Close()
		}
		c.mu.Unlock()
		if turn != nil {
			c.completeTurn(turn, nil, errors.New("claude: connection closed"))
		}
	})
}

// Done exposes the connection-closed channel for lifecycle joins.
func (c *SdkClient) Done() <-chan struct{} { return c.closed }

// SessionID returns the init-reported session id.
func (c *SdkClient) SessionID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID
}

func (c *SdkClient) readLoop() {
	defer c.Close()
	scanner := bufio.NewScanner(c.r)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		c.handleLine(line)
	}
	if err := scanner.Err(); err != nil {
		c.mu.Lock()
		if c.terminalErr == nil {
			c.terminalErr = err
		}
		c.mu.Unlock()
	}
}

func (c *SdkClient) handleLine(line string) {
	var envelope struct {
		Type       string          `json:"type"`
		Subtype    string          `json:"subtype,omitempty"`
		SessionID  string          `json:"session_id,omitempty"`
		IsError    bool            `json:"is_error,omitempty"`
		Result     json.RawMessage `json:"result,omitempty"`
		Error      json.RawMessage `json:"error,omitempty"`
		Message    json.RawMessage `json:"message,omitempty"`
		Event      json.RawMessage `json:"event,omitempty"`
		Request    json.RawMessage `json:"request,omitempty"`
		Response   json.RawMessage `json:"response,omitempty"`
		TotalCost  float64         `json:"total_cost_usd,omitempty"`
		StopReason string          `json:"stop_reason,omitempty"`
		UUID       string          `json:"uuid,omitempty"`
	}
	if err := json.Unmarshal([]byte(line), &envelope); err != nil {
		c.emit(Event{At: time.Now(), Type: "protocol/error", Text: err.Error(), Payload: line})
		return
	}

	event := Event{At: time.Now(), Type: envelope.Type, Subtype: envelope.Subtype, SessionID: envelope.SessionID, Payload: line}
	if envelope.Type == "control_request" || envelope.Type == "sdk_control_request" {
		c.handleControlRequest(event, envelope.Request)
		return
	}
	if envelope.Type == "control_response" {
		// Acknowledgment of our own control requests (initialize/interrupt).
		c.emit(event)
		return
	}

	switch envelope.Type {
	case "system":
		c.handleSystem(event, envelope.Result)
	case "assistant":
		c.handleAssistant(event, envelope.Message, envelope.StopReason)
	case "stream", "stream_event":
		c.handleStreamDelta(event, envelope.Event)
	case "result":
		// The whole result line IS the result object: subtype/is_error/errors/
		// session_id/total_cost_usd all live at the top level.
		c.handleResult(event, line, envelope.IsError, json.RawMessage(line), envelope.TotalCost, envelope.Error)
	case "error":
		c.handleError(event, envelope.Error)
	default:
		c.emit(event)
	}
}

func (c *SdkClient) handleSystem(event Event, raw json.RawMessage) {
	event.Payload = string(raw)
	if event.Subtype == "init" {
		var body struct {
			SessionID string `json:"session_id"`
			Cwd       string `json:"cwd"`
			Model     string `json:"model"`
		}
		_ = json.Unmarshal(raw, &body)
		// The init event carries session_id at the top level; raw (the
		// envelope "result" field) is usually empty here, so keep the
		// top-level value parsed by the envelope when the nested one is empty.
		sid := body.SessionID
		if sid == "" {
			sid = event.SessionID
		}
		event.SessionID = sid
		c.mu.Lock()
		if sid != "" {
			c.sessionID = sid
		}
		c.mu.Unlock()
		event.Text = "claude ready (session " + sid + ", model " + body.Model + ")"
	}
	c.emit(event)
}

func (c *SdkClient) handleAssistant(event Event, message json.RawMessage, stopReason string) {
	var body struct {
		Content []struct {
			Type     string `json:"type"`
			Text     string `json:"text"`
			Thinking string `json:"thinking"`
		} `json:"content"`
		Usage struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
			CacheRead    int64 `json:"cache_read_input_tokens"`
			CacheWrite   int64 `json:"cache_creation_input_tokens"`
		} `json:"usage"`
	}
	_ = json.Unmarshal(message, &body)

	var text, reasoning strings.Builder
	for _, block := range body.Content {
		switch block.Type {
		case "text":
			if strings.TrimSpace(block.Text) != "" {
				text.WriteString(block.Text)
			}
		case "thinking", "reasoning":
			if strings.TrimSpace(block.Thinking) != "" {
				reasoning.WriteString(block.Thinking)
			}
		}
	}
	usage := &TurnUsage{
		InputTokens:  body.Usage.InputTokens,
		OutputTokens: body.Usage.OutputTokens,
		CacheRead:    body.Usage.CacheRead,
		CacheWrite:   body.Usage.CacheWrite,
	}

	c.mu.Lock()
	turn := c.active
	if turn != nil {
		// With --include-partial-messages the full assistant message duplicates
		// the deltas already accumulated into turn.text; only merge it when no
		// delta was seen (e.g. a compacted/summarized reply without streaming).
		if !turn.sawDelta {
			if text.Len() > 0 {
				if turn.text.Len() > 0 && !strings.HasSuffix(turn.text.String(), "\n") {
					turn.text.WriteString("\n")
				}
				turn.text.WriteString(text.String())
			}
			if reasoning.Len() > 0 {
				if turn.reasoning.Len() > 0 && !strings.HasSuffix(turn.reasoning.String(), "\n") {
					turn.reasoning.WriteString("\n")
				}
				turn.reasoning.WriteString(reasoning.String())
			}
		}
		if stopReason != "" {
			turn.stop = stopReason
		}
		if usage.InputTokens > 0 || usage.OutputTokens > 0 || usage.CacheRead > 0 || usage.CacheWrite > 0 {
			turn.usage = usage
		}
	}
	c.mu.Unlock()

	// Full assistant message: flush-boundary event for the console. The text is
	// delivered through the coalescer via stream deltas when partial messages
	// are enabled; emit the block text here only for the final result so the
	// console does not duplicate the streamed answer.
	event.Text = text.String()
	event.Subtype = "message"
	c.emit(event)
}

func (c *SdkClient) handleStreamDelta(event Event, raw json.RawMessage) {
	var body struct {
		Type      string `json:"type"`
		EventType string `json:"event_type"`
		Delta     struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"delta"`
	}
	_ = json.Unmarshal(raw, &body)
	deltaType := body.Delta.Type
	if deltaType == "" {
		deltaType = body.EventType
	}
	if deltaType == "" {
		deltaType = body.Type
	}
	switch deltaType {
	case "text_delta":
		event.IsDelta = true
		event.Text = body.Delta.Text
	case "thinking_delta", "signature_delta":
		event.IsDelta = true
		event.Reasoning = body.Delta.Text
	}
	if !event.IsDelta {
		// content_block_start/stop, tool_use_start, message_start ... are
		// flush boundaries so accumulated delta blocks render promptly.
		c.emit(event)
		return
	}
	c.mu.Lock()
	turn := c.active
	if turn != nil {
		if event.Text != "" {
			turn.text.WriteString(event.Text)
			turn.sawDelta = true
		}
		if event.Reasoning != "" {
			turn.reasoning.WriteString(event.Reasoning)
			turn.sawDelta = true
		}
	}
	c.mu.Unlock()
	c.emit(event)
}

func (c *SdkClient) handleResult(event Event, rawLine string, isError bool, resultRaw json.RawMessage, totalCost float64, errorRaw json.RawMessage) {
	var body struct {
		Subtype   string   `json:"subtype"`
		IsError   bool     `json:"is_error"`
		Errors    []string `json:"errors"`
		SessionID string   `json:"session_id"`
	}
	_ = json.Unmarshal(resultRaw, &body)
	if body.SessionID == "" {
		// The result line itself carries session_id at the top level.
		var top struct {
			SessionID string `json:"session_id"`
		}
		_ = json.Unmarshal([]byte(rawLine), &top)
		body.SessionID = top.SessionID
	}
	event.SessionID = body.SessionID
	event.Subtype = body.Subtype
	if event.Subtype == "" {
		event.Subtype = "success"
	}
	var errText string
	if len(body.Errors) > 0 {
		errText = strings.Join(body.Errors, "; ")
	} else if len(errorRaw) > 0 && string(errorRaw) != "null" {
		var e struct {
			Message string `json:"message"`
		}
		_ = json.Unmarshal(errorRaw, &e)
		errText = e.Message
	}

	c.mu.Lock()
	turn := c.active
	if turn != nil {
		result := &TurnResult{
			SessionID:  body.SessionID,
			Text:       turn.text.String(),
			Reasoning:  turn.reasoning.String(),
			StopReason: turn.stop,
			IsError:    isError || body.IsError || body.Subtype == "error_max_turns" || body.Subtype == "error_during_execution",
			Error:      errText,
			Usage:      turn.usage,
			CostUSD:    totalCost,
			Raw:        append(json.RawMessage(nil), []byte(rawLine)...),
		}
		var err error
		if result.IsError {
			if result.Error == "" {
				result.Error = "claude turn failed: " + body.Subtype
			}
			err = fmt.Errorf("claude turn failed: %s", result.Error)
		} else if turn.interrupted {
			err = ErrTurnInterrupted
		}
		c.active = nil
		c.inFlight = false
		if c.settle != nil {
			close(c.settle)
			c.settle = nil
		}
		c.mu.Unlock()
		event.Text = result.Text
		c.emit(event)
		c.completeTurn(turn, result, err)
		return
	}
	// No active waiter (previous turn was interrupted/cancelled). Store the
	// result so the next Prompt can drain it instead of misattributing it.
	c.inFlight = false
	if c.settle != nil {
		close(c.settle)
		c.settle = nil
	}
	c.pending = &TurnResult{
		SessionID:  body.SessionID,
		StopReason: "stale",
		IsError:    isError || body.IsError,
		Error:      errText,
		Raw:        append(json.RawMessage(nil), []byte(rawLine)...),
	}
	c.mu.Unlock()
	event.Text = errText
	c.emit(event)
}

func (c *SdkClient) handleError(event Event, raw json.RawMessage) {
	var body struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(raw, &body)
	event.Text = body.Message
	if event.Text == "" {
		event.Text = string(raw)
	}
	event.Subtype = body.Type
	c.mu.Lock()
	turn := c.active
	c.mu.Unlock()
	if turn != nil {
		c.completeTurn(turn, nil, fmt.Errorf("claude api error: %s", event.Text))
	}
	c.emit(event)
}

// handleControlRequest answers CLI-initiated requests. Permission requests
// (subtype "permission" or "can_use_tool") are answered through the policy;
// everything else (mcp_message, hook_callback, exit_plan_mode, ...) is
// acknowledged without an action so the CLI never waits forever.
func (c *SdkClient) handleControlRequest(event Event, request json.RawMessage) {
	var body struct {
		Subtype     string          `json:"subtype"`
		RequestID   string          `json:"request_id"`
		ToolName    string          `json:"tool_name"`
		ToolUseID   string          `json:"tool_use_id"`
		ToolInput   json.RawMessage `json:"tool_input"`
		Input       json.RawMessage `json:"input"`
		ToolNameAlt string          `json:"toolName"`
	}
	_ = json.Unmarshal(request, &body)
	requestID := body.RequestID
	if requestID == "" {
		requestID = fmt.Sprintf("req_%d", time.Now().UnixNano())
	}
	event.Subtype = body.Subtype
	event.Text = body.ToolName
	if event.Text == "" {
		event.Text = body.ToolNameAlt
	}

	switch body.Subtype {
	case "permission", "can_use_tool":
		c.mu.Lock()
		policy := c.permit
		hook := c.onPermission
		sessionID := c.sessionID
		c.mu.Unlock()
		allow := false
		if policy != nil {
			input := body.ToolInput
			if len(input) == 0 {
				input = body.Input
			}
			var err error
			allow, err = policy(sessionID, body.ToolName, input)
			if errors.Is(err, ErrPermissionPending) {
				// Park the request: no control_response yet. The hook (usually
				// the orchestrator runtime) stores it and answers via
				// AnswerPermission once a human decides.
				if hook != nil {
					hook(PermissionRequest{
						RequestID: requestID,
						SessionID: sessionID,
						ToolName:  body.ToolName,
						ToolInput: input,
						AskedAt:   time.Now(),
					})
					return
				}
				// No hook to answer later: stay conservative and deny.
				allow = false
			} else if err != nil {
				allow = false
			}
		}
		resp := map[string]any{
			"type": "control_response",
			"response": map[string]any{
				"subtype":    "success",
				"request_id": requestID,
				"response": map[string]any{
					"behavior": "deny",
					"message":  "denied by orchestrator",
				},
			},
		}
		if allow {
			if inner, ok := resp["response"].(map[string]any); ok {
				inner["response"] = map[string]any{"behavior": "allow"}
			}
		}
		_ = c.write(resp)
	default:
		// Acknowledge unknown request types so the CLI does not block.
		_ = c.write(map[string]any{
			"type": "control_response",
			"response": map[string]any{
				"subtype":    "success",
				"request_id": requestID,
			},
		})
	}
	c.emit(event)
}
