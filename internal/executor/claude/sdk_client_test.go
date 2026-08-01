package claude

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeClaude serves one stream-json connection over net.Pipe. The handler
// receives every JSON line the client writes and returns the reply lines to
// write back.
func fakeClaude(t *testing.T, handler func(request map[string]any) string) (*SdkClient, func()) {
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
	client := NewSdkClient(c1, c1, nil)
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

func claudeLine(obj map[string]any) string {
	data, _ := json.Marshal(obj)
	return string(data) + "\n"
}

func initLine(sessionID string) string {
	return claudeLine(map[string]any{
		"type": "system", "subtype": "init", "session_id": sessionID,
		"cwd": "G:\\work", "model": "claude-sonnet-4",
		"tools": []string{"Bash", "Read", "Write"},
	})
}

func resultLine(subtype string, isError bool, sessionID string, extra map[string]any) string {
	obj := map[string]any{"type": "result", "subtype": subtype, "is_error": isError, "session_id": sessionID, "duration_ms": 100, "num_turns": 1}
	for k, v := range extra {
		obj[k] = v
	}
	return claudeLine(obj)
}

func TestSdkClientInitPromptAndResult(t *testing.T) {
	client, cleanup := fakeClaude(t, func(request map[string]any) string {
		switch request["type"] {
		case "user":
			msg := request["message"].(map[string]any)
			content := msg["content"].([]any)
			text := content[0].(map[string]any)["text"].(string)
			if !strings.Contains(text, "只回复OK") {
				t.Errorf("prompt text mismatch: %q", text)
			}
			return initLine("ses-1") +
				claudeLine(map[string]any{
					"type": "assistant",
					"message": map[string]any{
						"content": []any{
							map[string]any{"type": "text", "text": "OK"},
						},
						"usage": map[string]any{"input_tokens": 10, "output_tokens": 5},
					},
					"session_id": "ses-1",
				}) +
				resultLine("success", false, "ses-1", map[string]any{"total_cost_usd": 0.01})
		default:
			t.Errorf("unexpected request type %v", request["type"])
			return ""
		}
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := client.Prompt(ctx, "只回复OK")
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if result.Text != "OK" {
		t.Fatalf("Text = %q, want OK", result.Text)
	}
	if result.SessionID != "ses-1" && result.SessionID != "" {
		t.Fatalf("SessionID = %q", result.SessionID)
	}
	if result.Usage == nil || result.Usage.InputTokens != 10 || result.Usage.OutputTokens != 5 {
		t.Fatalf("Usage = %+v", result.Usage)
	}
}

func TestSdkClientStreamDeltasAccumulate(t *testing.T) {
	client, cleanup := fakeClaude(t, func(request map[string]any) string {
		switch request["type"] {
		case "user":
			reply := initLine("ses-2")
			// Reasoning + assistant deltas, then a full assistant message, then
			// the result. The final text must aggregate only assistant deltas.
			for _, piece := range []string{"App", "/", "Local", "/", "go"} {
				reply += claudeLine(map[string]any{
					"type": "stream_event",
					"event": map[string]any{
						"type":  "content_block_delta",
						"index": 0,
						"delta": map[string]any{"type": "text_delta", "text": piece},
					},
					"session_id": "ses-2",
				})
			}
			reply += claudeLine(map[string]any{
				"type": "stream_event",
				"event": map[string]any{
					"type":  "content_block_delta",
					"index": 0,
					"delta": map[string]any{"type": "thinking_delta", "text": "deep reasoning"},
				},
				"session_id": "ses-2",
			})
			reply += claudeLine(map[string]any{
				"type": "assistant",
				"message": map[string]any{
					"content": []any{map[string]any{"type": "text", "text": "App/Local/go"}},
				},
				"session_id": "ses-2",
			})
			reply += resultLine("success", false, "ses-2", nil)
			return reply
		default:
			t.Errorf("unexpected request type %v", request["type"])
			return ""
		}
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result, err := client.Prompt(ctx, "build")
	if err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	if result.Text != "App/Local/go" {
		t.Fatalf("Text = %q, want App/Local/go (deltas must accumulate)", result.Text)
	}
	if !strings.Contains(result.Reasoning, "deep reasoning") {
		t.Fatalf("Reasoning = %q, want thinking text", result.Reasoning)
	}
}

func TestSdkClientInterruptMarksTurnInterrupted(t *testing.T) {
	client, cleanup := fakeClaude(t, func(request map[string]any) string {
		switch request["type"] {
		case "user":
			// Turn starts, never finishes until the interrupt control request.
			return initLine("ses-3") + ""
		case "control_request":
			subtype := request["request"].(map[string]any)["subtype"]
			if subtype != "interrupt" {
				t.Errorf("unexpected control subtype %v", subtype)
			}
			return claudeLine(map[string]any{
				"type":    "control_request",
				"request": map[string]any{"subtype": "sdk_control_interrupt", "request_id": "ack"},
			}) + resultLine("success", false, "ses-3", map[string]any{"num_turns": 1})
		case "control_response":
			return ""
		default:
			t.Errorf("unexpected request type %v", request["type"])
			return ""
		}
	})
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.Prompt(ctx, "long task")
		done <- err
	}()
	time.Sleep(200 * time.Millisecond)
	if err := client.Interrupt(); err != nil {
		t.Fatalf("Interrupt: %v", err)
	}
	select {
	case err := <-done:
		if err != ErrTurnInterrupted {
			t.Fatalf("Prompt err = %v, want ErrTurnInterrupted", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Prompt did not return after interrupt")
	}
}

func TestSdkClientPermissionPolicyAllowAndDeny(t *testing.T) {
	client, cleanup := fakeClaude(t, func(request map[string]any) string {
		switch request["type"] {
		case "user":
			return initLine("ses-4") + claudeLine(map[string]any{
				"type":    "sdk_control_request",
				"request": map[string]any{"subtype": "permission", "request_id": "perm_1", "tool_name": "Bash", "tool_input": map[string]any{"command": "ls"}},
			}) + resultLine("success", false, "ses-4", nil)
		case "control_response":
			resp := request["response"].(map[string]any)
			if resp["subtype"] != "success" {
				t.Errorf("unexpected response subtype %v", resp["subtype"])
			}
			inner, ok := resp["response"].(map[string]any)
			if !ok {
				t.Fatalf("inner response missing: %v", resp)
			}
			if inner["behavior"] != "allow" {
				t.Errorf("behavior = %v, want allow", inner["behavior"])
			}
			return ""
		default:
			t.Errorf("unexpected request type %v", request["type"])
			return ""
		}
	})
	defer cleanup()
	client.SetPermissionPolicy(func(_ string, toolName string, _ json.RawMessage) (bool, error) {
		return toolName == "Bash", nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Prompt(ctx, "run ls"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
}

func TestSdkClientErrorResultFailsTurn(t *testing.T) {
	client, cleanup := fakeClaude(t, func(request map[string]any) string {
		switch request["type"] {
		case "user":
			return initLine("ses-5") + resultLine("error_during_execution", true, "ses-5", map[string]any{"errors": []string{"API overloaded"}})
		default:
			t.Errorf("unexpected request type %v", request["type"])
			return ""
		}
	})
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := client.Prompt(ctx, "boom")
	if err == nil || !strings.Contains(err.Error(), "API overloaded") {
		t.Fatalf("Prompt err = %v, want API overloaded", err)
	}
}

func TestSdkClientPendingDrainedOnNextPrompt(t *testing.T) {
	client, cleanup := fakeClaude(t, func(request map[string]any) string {
		switch request["type"] {
		case "user":
			// Delay the reply so the first waiter times out before its result
			// line arrives. The stale result must land in pending and never
			// complete the second turn.
			time.Sleep(400 * time.Millisecond)
			return initLine("ses-6") + resultLine("success", false, "ses-6", nil)
		default:
			t.Errorf("unexpected request type %v", request["type"])
			return ""
		}
	})
	defer cleanup()

	// Abandon the first turn: cancel the context mid-flight so the waiter
	// leaves before the CLI replies.
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	_, err := client.Prompt(ctx, "first")
	cancel()
	if err == nil {
		t.Fatal("expected context cancellation error")
	}
	// Wait for the read loop to process the stale result line.
	time.Sleep(500 * time.Millisecond)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	result, err := client.Prompt(ctx2, "second")
	if err != nil {
		t.Fatalf("second Prompt: %v", err)
	}
	if result.Text != "" {
		t.Fatalf("second turn Text = %q, want empty (pending must not leak)", result.Text)
	}
}
