package codex

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type appServerRequest struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

var appServerTestUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func newAppServerTestServer(t *testing.T, handle func(*websocket.Conn, appServerRequest)) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := appServerTestUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var request appServerRequest
			if err := conn.ReadJSON(&request); err != nil {
				return
			}
			handle(conn, request)
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http")
}

func appServerReply(t *testing.T, conn *websocket.Conn, id json.RawMessage, result any) {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{"id": json.RawMessage(id), "result": result}); err != nil {
		t.Errorf("write app-server response: %v", err)
	}
}

func TestAppServerWaitTurnConsumesCompletionDeliveredBeforeWait(t *testing.T) {
	endpoint := newAppServerTestServer(t, func(conn *websocket.Conn, request appServerRequest) {
		switch request.Method {
		case "initialize":
			appServerReply(t, conn, request.ID, map[string]any{})
		case "turn/start":
			// A JSON-RPC notification is allowed to arrive before the response to
			// turn/start. This is the production race that previously lost the
			// completion before WaitTurn registered its waiter.
			if err := conn.WriteJSON(map[string]any{
				"method": "item/agentMessage/delta",
				"params": map[string]any{"turnId": "turn-1", "delta": "审查"},
			}); err != nil {
				t.Errorf("write agent-message notification: %v", err)
				return
			}
			if err := conn.WriteJSON(map[string]any{
				"method": "turn/completed",
				"params": map[string]any{"threadId": "thread-1", "turn": map[string]any{"id": "turn-1", "status": "completed"}},
			}); err != nil {
				t.Errorf("write turn/completed notification: %v", err)
				return
			}
			appServerReply(t, conn, request.ID, map[string]any{"turn": map[string]any{"id": "turn-1"}})
		default:
			t.Errorf("unexpected request %q", request.Method)
		}
	})

	client, err := DialAppServer(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatalf("DialAppServer: %v", err)
	}
	defer client.Close()

	turnID, err := client.StartTurn(context.Background(), "thread-1", "please review", "", "")
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	output, err := client.WaitTurn(waitCtx, turnID)
	if err != nil {
		t.Fatalf("WaitTurn after an already-delivered completion: %v", err)
	}
	if output != "审查" {
		t.Fatalf("WaitTurn output = %q, want %q", output, "审查")
	}
}

func TestAppServerThreadLifecycleAndInterrupt(t *testing.T) {
	requests := make(chan appServerRequest, 8)
	endpoint := newAppServerTestServer(t, func(conn *websocket.Conn, request appServerRequest) {
		requests <- request
		switch request.Method {
		case "initialize":
			appServerReply(t, conn, request.ID, map[string]any{})
		case "thread/start":
			appServerReply(t, conn, request.ID, map[string]any{"thread": map[string]any{"id": "thread-new"}})
		case "thread/resume":
			appServerReply(t, conn, request.ID, map[string]any{"thread": map[string]any{"id": "thread-old"}})
		case "turn/interrupt":
			appServerReply(t, conn, request.ID, map[string]any{})
		default:
			t.Errorf("unexpected request %q", request.Method)
		}
	})

	client, err := DialAppServer(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatalf("DialAppServer: %v", err)
	}
	defer client.Close()

	opts := ThreadOptions{Workspace: `G:\work`, Model: "model-x", ApprovalPolicy: "never"}
	threadID, err := client.StartThread(context.Background(), opts)
	if err != nil || threadID != "thread-new" {
		t.Fatalf("StartThread = (%q, %v), want (thread-new, nil)", threadID, err)
	}
	resumedID, err := client.ResumeThread(context.Background(), "thread-old", opts)
	if err != nil || resumedID != "thread-old" {
		t.Fatalf("ResumeThread = (%q, %v), want (thread-old, nil)", resumedID, err)
	}
	if err := client.InterruptTurn(context.Background(), "thread-old", "turn-9"); err != nil {
		t.Fatalf("InterruptTurn: %v", err)
	}

	seen := map[string]appServerRequest{}
	for len(seen) < 4 {
		select {
		case request := <-requests:
			seen[request.Method] = request
		case <-time.After(time.Second):
			t.Fatalf("requests seen = %v, want initialize/thread-start/thread-resume/turn-interrupt", seen)
		}
	}
	var startParams, resumeParams, interruptParams map[string]any
	if err := json.Unmarshal(seen["thread/start"].Params, &startParams); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(seen["thread/resume"].Params, &resumeParams); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(seen["turn/interrupt"].Params, &interruptParams); err != nil {
		t.Fatal(err)
	}
	if startParams["cwd"] != opts.Workspace || startParams["model"] != opts.Model || startParams["approvalPolicy"] != opts.ApprovalPolicy {
		t.Fatalf("thread/start params = %#v", startParams)
	}
	if resumeParams["threadId"] != "thread-old" || resumeParams["cwd"] != opts.Workspace || resumeParams["model"] != opts.Model {
		t.Fatalf("thread/resume params = %#v", resumeParams)
	}
	if interruptParams["threadId"] != "thread-old" || interruptParams["turnId"] != "turn-9" {
		t.Fatalf("turn/interrupt params = %#v", interruptParams)
	}
}

func TestAppServerConnectionCloseFailsTrackedTurn(t *testing.T) {
	closeConnection := make(chan struct{})
	endpoint := newAppServerTestServer(t, func(conn *websocket.Conn, request appServerRequest) {
		switch request.Method {
		case "initialize":
			appServerReply(t, conn, request.ID, map[string]any{})
		case "turn/start":
			appServerReply(t, conn, request.ID, map[string]any{"turn": map[string]any{"id": "turn-close"}})
			go func() {
				<-closeConnection
				_ = conn.Close()
			}()
		default:
			t.Errorf("unexpected request %q", request.Method)
		}
	})
	client, err := DialAppServer(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatalf("DialAppServer: %v", err)
	}
	defer client.Close()
	turnID, err := client.StartTurn(context.Background(), "thread-1", "please review", "", "")
	if err != nil {
		t.Fatalf("StartTurn: %v", err)
	}
	close(closeConnection)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.WaitTurn(ctx, turnID); err == nil {
		t.Fatal("WaitTurn after connection close succeeded, want error")
	}
}

func TestAppServerCallReportsJSONRPCError(t *testing.T) {
	endpoint := newAppServerTestServer(t, func(conn *websocket.Conn, request appServerRequest) {
		if request.Method == "initialize" {
			appServerReply(t, conn, request.ID, map[string]any{})
			return
		}
		if err := conn.WriteJSON(map[string]any{"id": json.RawMessage(request.ID), "error": map[string]any{"code": -32601, "message": "unsupported"}}); err != nil {
			t.Errorf("write JSON-RPC error: %v", err)
		}
	})
	client, err := DialAppServer(context.Background(), endpoint, nil)
	if err != nil {
		t.Fatalf("DialAppServer: %v", err)
	}
	defer client.Close()

	err = client.Call(context.Background(), "unknown/method", map[string]any{}, nil)
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("Call error = %v, want JSON-RPC error containing unsupported", err)
	}
}

func TestExtractAppServerEventText(t *testing.T) {
	for _, tt := range []struct {
		name   string
		params string
		want   string
	}{
		{name: "delta", params: `{"turnId":"turn-1","delta":"hello"}`, want: "hello"},
		{name: "nested text", params: `{"item":{"content":[{"text":"first"},{"text":" second"}]}}`, want: "first second"},
		{name: "without text", params: `{"turnId":"turn-1","status":"completed"}`, want: ""},
		{name: "invalid JSON", params: `{`, want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractEventText("item/agentMessage/delta", json.RawMessage(tt.params)); got != tt.want {
				t.Fatalf("extractEventText() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestAppServerInterruptedTurnIsClassifiedSeparately(t *testing.T) {
	_, err := resolveTurnCompletion("turn-1", turnCompletion{TurnID: "turn-1", Status: "interrupted"})
	if !errors.Is(err, ErrAppServerTurnInterrupted) {
		t.Fatalf("resolveTurnCompletion interrupted error = %v, want ErrAppServerTurnInterrupted", err)
	}
}
