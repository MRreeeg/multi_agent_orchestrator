package orchestrator

import (
	"encoding/json"
	"testing"
	"time"

	mimoclient "reasonix/internal/executor/mimo"
)

func TestMimoRuntimeManagerEventUpdatesConsoleAndSSEBridge(t *testing.T) {
	manager := newMimoRuntimeManager()
	runtime := &mimoRuntime{
		ID: "mimo_rt_test", ModelRef: "xiaomi/mimo-v2.5", Endpoint: "http://127.0.0.1:12345",
		StartedAt: time.Now(), LastUsedAt: time.Now(), status: RuntimeIdle,
	}
	manager.runtimes["node|xiaomi/mimo-v2.5|work|build"] = runtime
	runtime.stream = newConsoleStreamCoalescer(0, func(evt RuntimeConsoleEvent) {
		runtime.mu.Lock()
		runtime.events = append(runtime.events, evt)
		runtime.mu.Unlock()
		manager.notify(runtime)
	})
	updates := make(chan RuntimeState, 4)
	manager.SetUpdateSink(func(state RuntimeState) { updates <- state })

	manager.recordEvent(runtime, mimoclient.AcpEvent{At: time.Now(), Method: "session/update", Update: "agent_message_chunk", MessageID: "msg-1", Text: "OK", Payload: `{"update":{"sessionUpdate":"agent_message_chunk"}}`})
	// A non-delta boundary (part completion) flushes the coalesced block.
	manager.recordEvent(runtime, mimoclient.AcpEvent{At: time.Now(), Method: "session/update", Update: "message_part_completed", MessageID: "msg-1", Payload: `{"update":{"sessionUpdate":"message_part_completed"}}`})

	select {
	case state := <-updates:
		if state.RuntimeID != runtime.ID || state.AccessMode != "runtime_console" || state.Executor != "mimo" {
			t.Fatalf("SSE state = %#v", state)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime event did not notify the SSE bridge")
	}
	snapshot, ok := manager.Snapshot(runtime.ID)
	// One consolidated assistant block + one boundary marker.
	if !ok || len(snapshot.Events) != 2 || snapshot.Events[0].Text != "OK" || snapshot.Events[0].Category != "assistant" {
		t.Fatalf("console snapshot = %#v, %v", snapshot, ok)
	}
}

func TestMimoRuntimeStateForPreservesNodeAndModel(t *testing.T) {
	manager := newMimoRuntimeManager()
	runtime := &mimoRuntime{
		ID: "mimo_rt_metadata", NodeID: "executor", ModelRef: "xiaomi/mimo-v2.5",
		ApprovalMode: "auto", Endpoint: "http://127.0.0.1:12345",
		StartedAt: time.Now(), LastUsedAt: time.Now(), status: RuntimeIdle, sessionID: "ses-1",
	}
	state := manager.stateFor(runtime, ExecSpec{}, CleanupRetained)
	if state.NodeID != "executor" || state.Model != "xiaomi/mimo-v2.5" || state.AccessMode != "runtime_console" {
		t.Fatalf("runtime state = %#v", state)
	}
	if state.ApprovalMode != "auto" || state.ThreadID != "ses-1" {
		t.Fatalf("runtime console metadata = %#v", state)
	}
}

func TestMimoRuntimeFinishTurnRetainsSession(t *testing.T) {
	manager := newMimoRuntimeManager()
	runtime := &mimoRuntime{sessionID: "ses-1", turnID: "turn-1", status: RuntimeBusy}
	manager.finishTurn(runtime, "ses-1", nil)
	if runtime.status != RuntimeIdle || runtime.sessionID != "ses-1" || runtime.turnID != "" {
		t.Fatalf("finishTurn state = status:%q session:%q turn:%q", runtime.status, runtime.sessionID, runtime.turnID)
	}
}

func TestMimoRuntimeManagerReserveTurnMarksBusyAndRejectsConcurrentTurn(t *testing.T) {
	manager := newMimoRuntimeManager()
	runtime := &mimoRuntime{
		ID: "mimo_rt_busy", client: &mimoclient.AcpClient{}, sessionID: "ses-1",
		StartedAt: time.Now(), LastUsedAt: time.Now(), status: RuntimeIdle,
	}
	updates := make(chan RuntimeState, 2)
	manager.SetUpdateSink(func(state RuntimeState) { updates <- state })

	client, err := manager.reserveTurn(runtime)
	if err != nil || client == nil {
		t.Fatalf("reserveTurn() = (%v, %v), want client and nil", client, err)
	}
	if runtime.status != RuntimeBusy || runtime.sessionID != "ses-1" {
		t.Fatalf("runtime after reserve = status:%q session:%q", runtime.status, runtime.sessionID)
	}
	select {
	case state := <-updates:
		if state.Status != RuntimeBusy || state.ThreadID != "ses-1" {
			t.Fatalf("runtime update = %#v, want busy ses-1", state)
		}
	case <-time.After(time.Second):
		t.Fatal("reserving a turn did not notify the runtime bridge")
	}
	if _, err := manager.reserveTurn(runtime); err == nil {
		t.Fatal("second reserveTurn() succeeded while a turn is active")
	}
}

func TestMimoRuntimeFinishInterruptedTurnRemainsIdle(t *testing.T) {
	manager := newMimoRuntimeManager()
	runtime := &mimoRuntime{sessionID: "ses-1", turnID: "turn-1", status: RuntimeBusy}
	manager.finishTurn(runtime, "ses-1", mimoclient.ErrTurnInterrupted)
	if runtime.status != RuntimeIdle || runtime.sessionID != "ses-1" || runtime.turnID != "" || runtime.lastErr != "" {
		t.Fatalf("interrupted finish state = status:%q session:%q turn:%q error:%q", runtime.status, runtime.sessionID, runtime.turnID, runtime.lastErr)
	}
}

func TestMimoPermissionPolicyMatchesApprovalMode(t *testing.T) {
	ask := &mimoRuntime{ApprovalMode: "ask"}
	option, err := mimoPermissionPolicy(ask)("ses-1", json.RawMessage(`{}`))
	if err != nil || option != "reject" {
		t.Fatalf("ask policy = %q, %v", option, err)
	}
	auto := &mimoRuntime{ApprovalMode: "auto"}
	option, err = mimoPermissionPolicy(auto)("ses-1", json.RawMessage(`{}`))
	if err != nil || option != "allow_always" {
		t.Fatalf("auto policy = %q, %v", option, err)
	}
}
