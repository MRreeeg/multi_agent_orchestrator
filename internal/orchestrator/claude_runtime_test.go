package orchestrator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	claudeclient "reasonix/internal/executor/claude"
)

func newTestClaudeRuntime(id string) *claudeRuntime {
	return &claudeRuntime{
		ID: id, NodeID: "executor", ModelRef: "sonnet", DisplayModel: "sonnet",
		ProviderRoute: "", Workspace: "G:\\work", ApprovalMode: "auto",
		Endpoint: "stdio://claude", StartedAt: time.Now(), LastUsedAt: time.Now(),
		status: RuntimeIdle, sessionID: "ses-1",
	}
}

func TestClaudeRuntimeStateForPreservesNodeAndModel(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_metadata")
	state := manager.stateFor(runtime, ExecSpec{}, CleanupRetained)
	if state.NodeID != "executor" || state.Model != "sonnet" || state.AccessMode != "runtime_console" {
		t.Fatalf("runtime state = %#v", state)
	}
	if state.Executor != "claude" || state.ThreadID != "ses-1" || state.ApprovalMode != "auto" {
		t.Fatalf("runtime console metadata = %#v", state)
	}
	if state.Port != 0 || state.Endpoint != "stdio://claude" {
		t.Fatalf("claude runtime must not expose a port/endpoint to browsers: %#v", state)
	}
}

func TestClaudeRuntimeReserveTurnMarksBusyAndRejectsConcurrentTurn(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_busy")
	runtime.client = &claudeclient.SdkClient{}
	updates := make(chan RuntimeState, 2)
	manager.SetUpdateSink(func(state RuntimeState) { updates <- state })

	client, err := manager.reserveTurn(runtime)
	if err != nil || client == nil {
		t.Fatalf("reserveTurn() = (%v, %v)", client, err)
	}
	if runtime.status != RuntimeBusy {
		t.Fatalf("status after reserve = %q, want busy", runtime.status)
	}
	select {
	case state := <-updates:
		if state.Status != RuntimeBusy || state.ThreadID != "ses-1" {
			t.Fatalf("runtime update = %#v", state)
		}
	case <-time.After(time.Second):
		t.Fatal("reserving a turn did not notify the runtime bridge")
	}
	if _, err := manager.reserveTurn(runtime); err == nil {
		t.Fatal("second reserveTurn() succeeded while a turn is active")
	}
}

func TestClaudeRuntimeFinishTurnRetainsSession(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_finish")
	runtime.turnID = "turn-1"
	runtime.status = RuntimeBusy
	manager.finishTurn(runtime, "ses-1", nil)
	if runtime.status != RuntimeIdle || runtime.sessionID != "ses-1" || runtime.turnID != "" {
		t.Fatalf("finishTurn state = status:%q session:%q turn:%q", runtime.status, runtime.sessionID, runtime.turnID)
	}
}

func TestClaudeRuntimeInterruptedTurnRemainsIdle(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_interrupt")
	runtime.status = RuntimeBusy
	manager.finishTurn(runtime, "ses-1", claudeclient.ErrTurnInterrupted)
	if runtime.status != RuntimeIdle || runtime.sessionID != "ses-1" || runtime.lastErr != "" {
		t.Fatalf("interrupted finishTurn = status:%q session:%q err:%q", runtime.status, runtime.sessionID, runtime.lastErr)
	}
}

func TestClaudeRuntimeFailedTurnMarksError(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_err")
	runtime.status = RuntimeBusy
	manager.finishTurn(runtime, "ses-1", context.DeadlineExceeded)
	if runtime.status != RuntimeError || runtime.lastErr == "" {
		t.Fatalf("failed finishTurn = status:%q err:%q", runtime.status, runtime.lastErr)
	}
}

func TestClaudeStreamPartClassifiesDeltas(t *testing.T) {
	if method, key, category, ok := claudeStreamPart(claudeclient.Event{IsDelta: true, Text: "OK", SessionID: "s1"}); !ok || method != "claude_message" || category != "assistant" || key != "s1" {
		t.Fatalf("text delta = %q %q %q %v", method, key, category, ok)
	}
	if method, _, category, ok := claudeStreamPart(claudeclient.Event{IsDelta: true, Reasoning: "think", SessionID: "s1"}); !ok || method != "claude_thought" || category != "reasoning" {
		t.Fatalf("thinking delta = %q %q %v", method, category, ok)
	}
	if _, _, _, ok := claudeStreamPart(claudeclient.Event{IsDelta: false, Type: "assistant"}); ok {
		t.Fatal("non-delta event classified as delta")
	}
}

func TestClaudeRuntimeCoalescesStreamDeltasIntoConsole(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_stream")
	manager.runtimes["node|sonnet|G:\\work|"] = runtime
	runtime.stream = newConsoleStreamCoalescer(0, func(evt RuntimeConsoleEvent) {
		runtime.mu.Lock()
		runtime.events = append(runtime.events, evt)
		runtime.mu.Unlock()
		manager.notify(runtime)
	})
	// Tiny text deltas must collapse into one assistant block.
	manager.recordEvent(runtime, claudeclient.Event{At: time.Now(), Type: "stream_event", IsDelta: true, Text: "App", SessionID: "s1"})
	manager.recordEvent(runtime, claudeclient.Event{At: time.Now(), Type: "stream_event", IsDelta: true, Text: "/", SessionID: "s1"})
	manager.recordEvent(runtime, claudeclient.Event{At: time.Now(), Type: "stream_event", IsDelta: true, Text: "Local", SessionID: "s1"})
	manager.recordEvent(runtime, claudeclient.Event{At: time.Now(), Type: "stream_event", IsDelta: true, Text: "/go", SessionID: "s1"})
	manager.recordEvent(runtime, claudeclient.Event{At: time.Now(), Type: "assistant", Subtype: "message"})
	snapshot, ok := manager.Snapshot(runtime.ID)
	if !ok {
		t.Fatal("snapshot missing")
	}
	var assistant string
	for _, evt := range snapshot.Events {
		if evt.Category == "assistant" {
			assistant += evt.Text
		}
	}
	if assistant != "App/Local/go" {
		t.Fatalf("coalesced assistant text = %q", assistant)
	}
}

func TestClaudePermissionPolicyAllowsAutoDeniesAsk(t *testing.T) {
	runtime := newTestClaudeRuntime("claude_rt_perm")
	runtime.ApprovalMode = "auto"
	policy := claudePermissionPolicy(runtime)
	if allow, _ := policy("", "Bash", json.RawMessage(`{"command":"ls"}`)); !allow {
		t.Fatal("auto policy must allow tool calls")
	}
	runtime.ApprovalMode = "ask"
	if allow, _ := policy("", "Bash", json.RawMessage(`{"command":"ls"}`)); allow {
		t.Fatal("ask policy must deny tool calls")
	}
}

func TestClaudeRuntimeModelOmitsForCCSwitch(t *testing.T) {
	if got := claudeRuntimeModel(ExecSpec{ModelRef: "sonnet"}); got != "sonnet" {
		t.Fatalf("self model = %q", got)
	}
	if got := claudeRuntimeModel(ExecSpec{ProviderRoute: "ccswitch", ModelRef: "ccs"}); got != "" {
		t.Fatalf("ccswitch model = %q, want empty", got)
	}
}

func TestClaudeRuntimeManagerStopCleansRegistry(t *testing.T) {
	manager := newClaudeRuntimeManager()
	runtime := newTestClaudeRuntime("claude_rt_stop")
	runtime.done = make(chan struct{})
	close(runtime.done) // already exited
	manager.runtimes["node|sonnet|G:\\work|"] = runtime
	if err := manager.Stop(runtime.ID); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if _, ok := manager.Get(runtime.ID); ok {
		t.Fatal("runtime still registered after Stop")
	}
}
