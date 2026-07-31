package orchestrator

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// TestRetainedRuntimeCommandSurvivesRequestCancellation guards the lifecycle
// boundary that caused Loop Agents to show "reconnecting" after each node:
// canceling a node request must not terminate its retained serve process.
func TestRetainedRuntimeCommandSurvivesRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		cmd = newRetainedRuntimeCommand(ctx, "cmd.exe", "/d", "/c", "ping -n 4 127.0.0.1 > nul")
	} else {
		cmd = newRetainedRuntimeCommand(ctx, "sh", "-c", "sleep 2")
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start retained runtime: %v", err)
	}

	cancel()
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		t.Fatalf("retained runtime exited when request context was canceled: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	_ = cmd.Process.Kill()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("retained runtime did not stop after explicit kill")
	}
}

// TestRuntimeManagerDropsExitedRuntime verifies that an externally exited
// serve process is removed from the registry instead of being reused by the
// next Loop iteration.
func TestRuntimeManagerDropsExitedRuntime(t *testing.T) {
	m := newMimoRuntimeManager()
	cmd := shortLivedRuntimeCommand()
	if err := cmd.Start(); err != nil {
		t.Fatalf("start short-lived runtime: %v", err)
	}
	rt := &mimoRuntime{
		ID:   "mimo_rt_test_exit",
		Cmd:  cmd,
		done: make(chan struct{}),
	}
	m.runtimes["test-key"] = rt
	go m.watchRuntime("test-key", rt)

	select {
	case <-rt.done:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime exit watcher did not observe process termination")
	}
	if _, ok := m.Get(rt.ID); ok {
		t.Fatal("exited runtime remained borrowable in manager")
	}
}

// TestSubmitTaskPollsStatusWhileSSEIsQuiet guards the timeout boundary in
// submitTask. A quiet/missed SSE stream must not consume five minutes before
// status polling starts; otherwise a long final executor can hit the node
// timeout immediately after finishing and its runtime gets stopped.
func TestSubmitTaskPollsStatusWhileSSEIsQuiet(t *testing.T) {
	var statusCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submit":
			w.WriteHeader(http.StatusAccepted)
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			if statusCalls.Add(1) < 2 {
				_, _ = w.Write([]byte(`{"status":"running"}`))
				return
			}
			_, _ = w.Write([]byte(`{"status":"idle","lastUsage":{"promptTokens":1,"completionTokens":2,"totalTokens":3}}`))
		case "/history":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]string{{"role": "assistant", "content": "done"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	start := time.Now()
	output, usage, err := submitTask(ctx, portFromEndpoint(srv.URL), "test task")
	if err != nil {
		t.Fatalf("submitTask: %v", err)
	}
	if output != "done" {
		t.Fatalf("output = %q, want done", output)
	}
	if usage == nil || usage.TotalTokens != 3 {
		t.Fatalf("usage = %+v, want total tokens 3", usage)
	}
	if elapsed := time.Since(start); elapsed > 4*time.Second {
		t.Fatalf("submitTask waited too long with quiet SSE: %v", elapsed)
	}
}

// TestSubmitTaskUsesRunningFieldFromServeStatus matches the real serve API.
// /status reports a boolean `running`, not the legacy string `status`. A missing
// legacy field must not be interpreted as idle while the asynchronous turn is
// still executing.
func TestSubmitTaskUsesRunningFieldFromServeStatus(t *testing.T) {
	var statusCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submit":
			w.WriteHeader(http.StatusAccepted)
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			<-r.Context().Done()
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			if statusCalls.Add(1) == 1 {
				_, _ = w.Write([]byte(`{"running":true}`))
				return
			}
			_, _ = w.Write([]byte(`{"running":false,"lastUsage":{"promptTokens":1,"completionTokens":2,"totalTokens":3}}`))
		case "/history":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode([]map[string]string{{"role": "assistant", "content": "done"}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	output, usage, err := submitTask(ctx, portFromEndpoint(srv.URL), "test task")
	if err != nil {
		t.Fatalf("submitTask: %v", err)
	}
	if output != "done" {
		t.Fatalf("output = %q, want done", output)
	}
	if usage == nil || usage.TotalTokens != 3 {
		t.Fatalf("usage = %+v, want total tokens 3", usage)
	}
	if calls := statusCalls.Load(); calls < 2 {
		t.Fatalf("status calls = %d, want at least 2 (running=true then running=false)", calls)
	}
}

func shortLivedRuntimeCommand() *exec.Cmd {
	if runtime.GOOS == "windows" {
		return newRetainedRuntimeCommand(context.Background(), "cmd.exe", "/d", "/c", "exit 0")
	}
	return newRetainedRuntimeCommand(context.Background(), "sh", "-c", "exit 0")
}
