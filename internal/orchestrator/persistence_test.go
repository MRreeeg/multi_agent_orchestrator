package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSessionStateBackfillsExecutorAndModeWithoutForcingMimoAgent(t *testing.T) {
	t.Setenv("USERPROFILE", t.TempDir())
	t.Setenv("HOME", t.TempDir())

	store := NewStore()
	state := SessionState{
		Nodes: []AgentNode{
			{
				ID:    "n1",
				Label: "Architect",
				Type:  NodeArchitect,
				Model: "deepseek-pro",
			},
			{
				ID:       "n2",
				Label:    "Executor",
				Type:     NodeExecutor,
				Model:    "mimo-v2.5-pro",
				Executor: ExecutorMimo,
			},
		},
	}
	if err := store.SaveSessionState("sess-test", state); err != nil {
		t.Fatalf("SaveSessionState: %v", err)
	}

	loaded, err := store.LoadSessionState("sess-test")
	if err != nil {
		t.Fatalf("LoadSessionState: %v", err)
	}
	if got := loaded.Nodes[0].Executor; got != ExecutorReasonix {
		t.Fatalf("node1 executor = %q, want %q", got, ExecutorReasonix)
	}
	if got := loaded.Nodes[0].Mode; got != "serve" {
		t.Fatalf("node1 mode = %q, want serve", got)
	}
	if got := loaded.Nodes[1].Mode; got != "serve" {
		t.Fatalf("node2 mode = %q, want serve", got)
	}
	if got := loaded.Nodes[1].Agent; got != "" {
		t.Fatalf("node2 agent = %q, want empty", got)
	}
}

func TestSaveSessionStatePersistsModeField(t *testing.T) {
	t.Setenv("USERPROFILE", t.TempDir())
	t.Setenv("HOME", t.TempDir())

	store := NewStore()
	state := SessionState{
		Nodes: []AgentNode{{
			ID:       "n1",
			Label:    "Executor",
			Type:     NodeExecutor,
			Model:    "mimo-v2.5-pro",
			Executor: ExecutorMimo,
			Mode:     "run",
			Agent:    "coder",
		}},
	}
	if err := store.SaveSessionState("sess-mode", state); err != nil {
		t.Fatalf("SaveSessionState: %v", err)
	}

	path := filepath.Join(sessionStateDir(), "sess-mode.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(data) == "" {
		t.Fatal("saved session state file is empty")
	}

	loaded, err := store.LoadSessionState("sess-mode")
	if err != nil {
		t.Fatalf("LoadSessionState: %v", err)
	}
	if got := loaded.Nodes[0].Mode; got != "run" {
		t.Fatalf("loaded mode = %q, want run", got)
	}
}

func TestLoadAllDataRecoversSessionReferencesFromEntityFiles(t *testing.T) {
	t.Setenv("USERPROFILE", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	root := t.TempDir()
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)

	store := NewStore()
	sess, err := store.CreateOrchSession("recover history", "")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "recover pipeline",
		Nodes: []AgentNode{{ID: "exec", Type: NodeExecutor}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CreateRun(sess.ID, rev.ID, "recover task", "", "manual", ""); err != nil {
		t.Fatal(err)
	}

	// Simulate the crash window where entity documents were written but the
	// session/index pointer update was lost. The run and revision files remain.
	stale := *sess
	stale.PipelineRevisionIDs = nil
	stale.CurrentPipelineID = ""
	stale.RunIDs = nil
	stale.CurrentRunID = ""
	if err := saveSessionJSON(sessionDir(sess.ID), "session.json", &stale); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	store.orchSessions[sess.ID] = &stale
	store.mu.Unlock()
	if err := store.saveIndex(); err != nil {
		t.Fatal(err)
	}

	reloaded := NewStore()
	if err := reloaded.LoadAllData(); err != nil {
		t.Fatal(err)
	}
	gotSess, ok := reloaded.GetOrchSession(sess.ID)
	if !ok {
		t.Fatal("session disappeared after recovery")
	}
	if gotSess.CurrentPipelineID != rev.ID {
		t.Fatalf("current pipeline = %q, want %q", gotSess.CurrentPipelineID, rev.ID)
	}
	if len(gotSess.RunIDs) != 1 {
		t.Fatalf("run references = %d, want 1", len(gotSess.RunIDs))
	}
	if runs := reloaded.ListRunsForSession(sess.ID); len(runs) != 1 || runs[0].ID != gotSess.RunIDs[0] {
		t.Fatalf("recovered runs = %+v", runs)
	}
}

func TestMarkInterruptedStopsPersistedClaudeRuntimeButPreservesSession(t *testing.T) {
	t.Setenv("USERPROFILE", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())

	store := NewStore()
	sess, err := store.CreateOrchSession("claude recovery", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRuntimeState(RuntimeState{
		RuntimeID: "claude_rt_stale", SessionID: sess.ID, Executor: string(ExecutorClaude),
		Status: RuntimeIdle, Endpoint: "stdio://claude", ThreadID: "ses-persisted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.markInterrupted(); err != nil {
		t.Fatal(err)
	}
	got, ok := store.GetRuntimeState("claude_rt_stale")
	if !ok {
		t.Fatal("runtime missing")
	}
	if got.Status != RuntimeStopped || got.Endpoint != "" || got.Port != 0 || got.ThreadID != "ses-persisted" || got.TurnID != "" {
		t.Fatalf("recovered runtime = %#v", got)
	}
}

func TestMarkInterruptedStopsPersistedCodexRuntimeButPreservesThread(t *testing.T) {
	t.Setenv("USERPROFILE", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())

	store := NewStore()
	sess, err := store.CreateOrchSession("codex recovery", "")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.CreateRuntimeState(RuntimeState{
		RuntimeID: "codex_rt_stale", SessionID: sess.ID, Executor: string(ExecutorCodex),
		Status: RuntimeIdle, Endpoint: "ws://127.0.0.1:61234", Port: 61234, ThreadID: "thread-persisted",
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.markInterrupted(); err != nil {
		t.Fatal(err)
	}
	got, ok := store.GetRuntimeState("codex_rt_stale")
	if !ok {
		t.Fatal("runtime missing")
	}
	if got.Status != RuntimeStopped || got.Endpoint != "" || got.Port != 0 || got.ThreadID != "thread-persisted" || got.TurnID != "" {
		t.Fatalf("recovered runtime = %#v", got)
	}
}
