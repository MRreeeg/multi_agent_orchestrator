package mimo

import (
	"bufio"
	"context"
	"encoding/json"

	"net"
	"strings"
	"testing"
	"time"
)

// fakeACPServer serves one ACP connection over net.Pipe. The handler receives
// every JSON line the client writes and returns the raw reply lines to write
// back (responses and/or notifications).
func fakeACPServer(t *testing.T, handler func(request map[string]any) string) (*AcpClient, func()) {
	t.Helper()
	c1, c2 := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer c2.Close()
		reader := bufio.NewReader(c2)
		for {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				return
			}
			var request map[string]any
			if err := json.Unmarshal(line, &request); err != nil {
				continue
			}
			reply := handler(request)
			if strings.TrimSpace(reply) == "" {
				continue
			}
			if _, err := c2.Write([]byte(reply)); err != nil {
				return
			}
		}
	}()
	client := NewAcpClient(c1, c1, nil)
	cleanup := func() {
		client.Close()
		_ = c1.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	return client, cleanup
}

func replyJSON(id any, result any) string {
	data, _ := json.Marshal(map[string]any{"id": id, "result": result})
	return string(data) + "\n"
}

func replyError(id any, code int, message string) string {
	data, _ := json.Marshal(map[string]any{"id": id, "error": map[string]any{"code": code, "message": message}})
	return string(data) + "\n"
}

func notification(method string, params any) string {
	data, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "method": method, "params": params})
	return string(data) + "\n"
}

func requestID(request map[string]any) any {
	return request["id"]
}

func TestAcpClientInitializeAndNewSession(t *testing.T) {
	client, cleanup := fakeACPServer(t, func(request map[string]any) string {
		switch request["method"] {
		case "initialize":
			return replyJSON(requestID(request), map[string]any{"protocolVersion": 1, "agentInfo": map[string]any{"name": "mimo", "version": "0.1.9"}})
		case "session/new":
			return replyJSON(requestID(request), map[string]any{"sessionId": "ses-abc"})
		default:
			t.Errorf("unexpected request %v", request["method"])
			return ""
		}
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	sessionID, err := client.NewSession(ctx, "G:\\work")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	if sessionID != "ses-abc" {
		t.Fatalf("NewSession = %q, want ses-abc", sessionID)
	}
}

func TestAcpClientPromptAccumulatesTextAndUsage(t *testing.T) {
	client, cleanup := fakeACPServer(t, func(request map[string]any) string {
		switch request["method"] {
		case "initialize":
			return replyJSON(requestID(request), map[string]any{})
		case "session/prompt":
			// Streaming notifications, then the correlated response. Chunks for
			// reasoning must not leak into the final assistant text.
			return notification("session/update", map[string]any{
				"sessionId": "ses-abc",
				"update": map[string]any{
					"sessionUpdate": "agent_thought_chunk",
					"messageId":     "msg-1",
					"content":       map[string]any{"type": "text", "text": "thinking"},
				},
			}) +
				notification("session/update", map[string]any{
					"sessionId": "ses-abc",
					"update": map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"messageId":     "msg-1",
						"content":       map[string]any{"type": "text", "text": "审查"},
					},
				}) +
				notification("session/update", map[string]any{
					"sessionId": "ses-abc",
					"update": map[string]any{
						"sessionUpdate": "agent_message_chunk",
						"messageId":     "msg-1",
						"content":       map[string]any{"type": "text", "text": "通过"},
					},
				}) +
				replyJSON(requestID(request), map[string]any{
					"stopReason": "end_turn",
					"usage": map[string]any{
						"totalTokens": 100, "inputTokens": 90, "outputTokens": 10,
						"thoughtTokens": 5, "cachedReadTokens": 4,
					},
				})
		default:
			t.Errorf("unexpected request %v", request["method"])
			return ""
		}
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.Prompt(ctx, "ses-abc", "只回复OK")
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if result.Text != "审查通过" {
		t.Fatalf("Prompt text = %q, want %q", result.Text, "审查通过")
	}
	if result.StopReason != "end_turn" {
		t.Fatalf("stopReason = %q", result.StopReason)
	}
	if result.Usage == nil || result.Usage.Total != 100 || result.Usage.Output != 10 {
		t.Fatalf("usage = %#v", result.Usage)
	}
}

func TestAcpClientPromptCompletedBeforeWait(t *testing.T) {
	// The ACP server may emit the completion immediately after the request;
	// the client must not lose chunks that arrive before its own waiter setup.
	client, cleanup := fakeACPServer(t, func(request map[string]any) string {
		if request["method"] == "session/prompt" {
			return notification("session/update", map[string]any{
				"sessionId": "ses-abc",
				"update": map[string]any{
					"sessionUpdate": "agent_message_chunk",
					"messageId":     "msg-1",
					"content":       map[string]any{"type": "text", "text": "OK"},
				},
			}) + replyJSON(requestID(request), map[string]any{"stopReason": "end_turn"})
		}
		return replyJSON(requestID(request), map[string]any{})
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.Prompt(ctx, "ses-abc", "hi")
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if result.Text != "OK" {
		t.Fatalf("Prompt text = %q, want OK", result.Text)
	}
}

func TestAcpClientCallError(t *testing.T) {
	client, cleanup := fakeACPServer(t, func(request map[string]any) string {
		return replyError(requestID(request), -32601, "Method not found")
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Initialize(ctx)
	if err == nil || !strings.Contains(err.Error(), "Method not found") {
		t.Fatalf("Initialize error = %v, want method-not-found", err)
	}
}

func TestAcpClientConnectionCloseFailsPending(t *testing.T) {
	c1, c2 := net.Pipe()
	client := NewAcpClient(c1, c1, nil)
	defer func() {
		client.Close()
		_ = c1.Close()
	}()
	// Server closes the connection without responding.
	go func() {
		_, _ = c2.Write([]byte("{\"jsonrpc\":\"2.0\",\"method\":\"session/update\",\"params\":{\"sessionId\":\"s\",\"update\":{\"sessionUpdate\":\"usage_update\"}}}\n"))
		_ = c2.Close()
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err := client.Initialize(ctx)
	if err == nil {
		t.Fatal("Initialize succeeded on a closed connection")
	}
}

func TestAcpClientCancelSendsNotification(t *testing.T) {
	sawCancel := make(chan string, 1)
	client, cleanup := fakeACPServer(t, func(request map[string]any) string {
		if method, _ := request["method"].(string); method == "session/cancel" {
			params, _ := request["params"].(map[string]any)
			sessionID, _ := params["sessionId"].(string)
			sawCancel <- sessionID
		}
		return ""
	})
	defer cleanup()
	if err := client.Cancel("ses-abc"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	select {
	case sessionID := <-sawCancel:
		if sessionID != "ses-abc" {
			t.Fatalf("cancel session = %q", sessionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("session/cancel notification not received")
	}
}

func TestAcpClientRejectsSecondConcurrentPrompt(t *testing.T) {
	client, cleanup := fakeACPServer(t, func(request map[string]any) string {
		if request["method"] == "session/prompt" {
			time.Sleep(100 * time.Millisecond)
			return replyJSON(requestID(request), map[string]any{"stopReason": "end_turn"})
		}
		return replyJSON(requestID(request), map[string]any{})
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	first := make(chan error, 1)
	go func() {
		_, err := client.Prompt(ctx, "ses-abc", "first")
		first <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if _, err := client.Prompt(ctx, "ses-abc", "second"); err == nil {
		t.Fatal("second Prompt succeeded while one was active")
	}
	if err := <-first; err != nil {
		t.Fatalf("first Prompt: %v", err)
	}
}
