package mimo

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

func TestAcpClientPermissionPolicyAutoAllow(t *testing.T) {
	permissionSeen := make(chan string, 1)
	promptID := json.RawMessage(nil)
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
			method, _ := request["method"].(string)
			switch method {
			case "initialize":
				writeJSON(c2, map[string]any{"id": request["id"], "result": map[string]any{}})
			case "session/prompt":
				// The agent asks the client for permission before running.
				promptID, _ = json.Marshal(request["id"])
				writeJSON(c2, map[string]any{
					"id":     77,
					"method": "session/request_permission",
					"params": map[string]any{
						"sessionId": "ses-abc",
						"toolCall":  map[string]any{"toolCallId": "call-1", "title": "bash"},
					},
				})
			default:
				// The client's reply to request_permission lands here. The agent
				// then completes the pending prompt turn.
				if result, ok := request["result"].(map[string]any); ok {
					if outcome, ok := result["outcome"].(map[string]any); ok {
						permissionSeen <- fmt.Sprint(outcome["optionId"])
					}
				}
				if promptID != nil {
					writeJSON(c2, map[string]any{"id": json.RawMessage(promptID), "result": map[string]any{"stopReason": "end_turn"}})
					promptID = nil
				}
			}
		}
	}()
	client := NewAcpClient(c1, c1, nil)
	defer func() {
		client.Close()
		_ = c1.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}()
	client.SetPermissionPolicy(func(sessionID string, toolCall json.RawMessage) (string, error) {
		return "allow_always", nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	if _, err := client.Prompt(ctx, "ses-abc", "hi"); err != nil {
		t.Fatalf("Prompt: %v", err)
	}
	select {
	case optionID := <-permissionSeen:
		if optionID != "allow_always" {
			t.Fatalf("permission option = %q, want allow_always", optionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permission reply not observed by the server")
	}
}

// TestAcpClientPermissionParkAndAnswer verifies the ask path: the policy
// returns ErrPermissionPending, the request is parked and surfaced through
// the hook (no reply written yet), and a later AnswerPermission delivers the
// chosen option to the agent.
func TestAcpClientPermissionParkAndAnswer(t *testing.T) {
	permissionSeen := make(chan string, 1)
	hookSeen := make(chan PermissionRequest, 1)
	promptID := json.RawMessage(nil)
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
			method, _ := request["method"].(string)
			switch method {
			case "initialize":
				writeJSON(c2, map[string]any{"id": request["id"], "result": map[string]any{}})
			case "session/prompt":
				promptID, _ = json.Marshal(request["id"])
				writeJSON(c2, map[string]any{
					"id":     77,
					"method": "session/request_permission",
					"params": map[string]any{
						"sessionId": "ses-abc",
						"toolCall":  map[string]any{"toolCallId": "call-1", "title": "bash"},
					},
				})
			default:
				if result, ok := request["result"].(map[string]any); ok {
					if outcome, ok := result["outcome"].(map[string]any); ok {
						permissionSeen <- fmt.Sprint(outcome["optionId"])
					}
				}
				if promptID != nil {
					writeJSON(c2, map[string]any{"id": json.RawMessage(promptID), "result": map[string]any{"stopReason": "end_turn"}})
					promptID = nil
				}
			}
		}
	}()
	client := NewAcpClient(c1, c1, nil)
	defer func() {
		client.Close()
		_ = c1.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}()
	client.SetPermissionPolicy(func(sessionID string, toolCall json.RawMessage) (string, error) {
		return "", ErrPermissionPending
	})
	client.SetPermissionHook(func(req PermissionRequest) { hookSeen <- req })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Initialize(ctx); err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	promptDone := make(chan error, 1)
	go func() { _, err := client.Prompt(ctx, "ses-abc", "hi"); promptDone <- err }()

	var parked PermissionRequest
	select {
	case parked = <-hookSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("parked permission request not delivered to the hook")
	}
	if parked.ToolName != "bash" {
		t.Fatalf("parked tool name = %q, want bash", parked.ToolName)
	}
	// The agent must still be waiting: nothing should have reached it yet.
	select {
	case id := <-permissionSeen:
		t.Fatalf("agent got a reply before AnswerPermission: %q", id)
	case <-time.After(150 * time.Millisecond):
	}

	if err := client.AnswerPermission(parked.EnvID, "allow_once"); err != nil {
		t.Fatalf("AnswerPermission: %v", err)
	}
	select {
	case optionID := <-permissionSeen:
		if optionID != "allow_once" {
			t.Fatalf("permission option = %q, want allow_once", optionID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("permission reply not observed by the server after AnswerPermission")
	}
	select {
	case err := <-promptDone:
		if err != nil {
			t.Fatalf("Prompt: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("prompt did not complete after permission answer")
	}
}

func writeJSON(w interface{ Write([]byte) (int, error) }, v any) {
	data, _ := json.Marshal(v)
	_, _ = w.Write(append(data, '\n'))
}
