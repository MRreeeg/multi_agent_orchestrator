package orchestrator

import (
	"testing"
	"time"

	codexclient "reasonix/internal/executor/codex"
)

func TestCodexRuntimeManagerEventUpdatesConsoleAndSSEBridge(t *testing.T) {
	manager := newCodexRuntimeManager()
	runtime := &codexRuntime{
		ID: "codex_rt_test", ModelRef: "ccs", Endpoint: "ws://127.0.0.1:12345",
		StartedAt: time.Now(), LastUsedAt: time.Now(), status: RuntimeIdle,
	}
	manager.runtimes["node|ccs|work|ccswitch"] = runtime
	updates := make(chan RuntimeState, 1)
	manager.SetUpdateSink(func(state RuntimeState) { updates <- state })

	manager.recordEvent(runtime, codexclient.AppServerEvent{At: time.Now(), Method: "item/agentMessage/delta", Text: "hello", Params: []byte(`{"turnId":"turn-1","delta":"hello"}`)})

	select {
	case state := <-updates:
		if state.RuntimeID != runtime.ID || state.AccessMode != "runtime_console" {
			t.Fatalf("SSE state = %#v", state)
		}
	case <-time.After(time.Second):
		t.Fatal("runtime event did not notify the SSE bridge")
	}
	snapshot, ok := manager.Snapshot(runtime.ID)
	if !ok || len(snapshot.Events) != 1 || snapshot.Events[0].Text != "hello" {
		t.Fatalf("console snapshot = %#v, %v", snapshot, ok)
	}
}

func TestCodexRuntimeStateForPreservesNodeAndDisplayModel(t *testing.T) {
	manager := newCodexRuntimeManager()
	runtime := &codexRuntime{
		ID: "codex_rt_metadata", NodeID: "reviewer", ModelRef: "", DisplayModel: "ccs",
		ApprovalMode: "auto", ExecutionMode: "task", Endpoint: "ws://127.0.0.1:12345",
		StartedAt: time.Now(), LastUsedAt: time.Now(), status: RuntimeIdle,
	}
	state := manager.stateFor(runtime, ExecSpec{}, CleanupRetained)
	if state.NodeID != "reviewer" || state.Model != "ccs" || state.AccessMode != "runtime_console" {
		t.Fatalf("runtime state = %#v", state)
	}
	if state.ApprovalMode != "auto" || state.ExecutionMode != "task" {
		t.Fatalf("runtime execution metadata = %#v", state)
	}
}

func TestCodexRuntimeModelOmitsCCSwitchModel(t *testing.T) {
	for _, spec := range []ExecSpec{
		{ModelRef: "ccs"},
		{ModelRef: "ccswitch"},
		{ModelRef: "any-model", ProviderRoute: "ccswitch"},
	} {
		if got := codexRuntimeModel(spec); got != "" {
			t.Fatalf("codexRuntimeModel(%#v) = %q, want empty", spec, got)
		}
	}
	if got := codexRuntimeModel(ExecSpec{ModelRef: "gpt-5"}); got != "gpt-5" {
		t.Fatalf("normal model = %q", got)
	}
}

func TestCodexRuntimeFinishTurnRetainsThread(t *testing.T) {
	manager := newCodexRuntimeManager()
	runtime := &codexRuntime{threadID: "thread-1", turnID: "turn-1", status: RuntimeBusy}
	manager.finishTurn(runtime, "thread-1", nil)
	if runtime.status != RuntimeIdle || runtime.threadID != "thread-1" || runtime.turnID != "" {
		t.Fatalf("finishTurn state = status:%q thread:%q turn:%q", runtime.status, runtime.threadID, runtime.turnID)
	}
}

func TestCodexRuntimeManagerReserveTurnMarksBusyAndRejectsConcurrentTurn(t *testing.T) {
	manager := newCodexRuntimeManager()
	runtime := &codexRuntime{
		ID: "codex_rt_busy", client: &codexclient.AppServerClient{}, threadID: "thread-1",
		StartedAt: time.Now(), LastUsedAt: time.Now(), status: RuntimeIdle,
	}
	updates := make(chan RuntimeState, 2)
	manager.SetUpdateSink(func(state RuntimeState) { updates <- state })

	client, err := manager.reserveTurn(runtime)
	if err != nil || client == nil {
		t.Fatalf("reserveTurn() = (%v, %v), want client and nil", client, err)
	}
	if runtime.status != RuntimeBusy || runtime.threadID != "thread-1" {
		t.Fatalf("runtime after reserve = status:%q thread:%q", runtime.status, runtime.threadID)
	}
	select {
	case state := <-updates:
		if state.Status != RuntimeBusy || state.ThreadID != "thread-1" {
			t.Fatalf("runtime update = %#v, want busy thread-1", state)
		}
	case <-time.After(time.Second):
		t.Fatal("reserving a turn did not notify the runtime bridge")
	}
	if _, err := manager.reserveTurn(runtime); err == nil {
		t.Fatal("second reserveTurn() succeeded while a turn is active")
	}
}

func TestCodexRuntimeFinishInterruptedTurnRemainsIdle(t *testing.T) {
	manager := newCodexRuntimeManager()
	runtime := &codexRuntime{threadID: "thread-1", turnID: "turn-1", status: RuntimeBusy}
	manager.finishTurn(runtime, "thread-1", codexclient.ErrAppServerTurnInterrupted)
	if runtime.status != RuntimeIdle || runtime.threadID != "thread-1" || runtime.turnID != "" || runtime.lastErr != "" {
		t.Fatalf("interrupted finish state = status:%q thread:%q turn:%q error:%q", runtime.status, runtime.threadID, runtime.turnID, runtime.lastErr)
	}
}
