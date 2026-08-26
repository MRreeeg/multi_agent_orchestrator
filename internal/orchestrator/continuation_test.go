package orchestrator

import (
	"strings"
	"testing"
)

func newNoteTestStore(t *testing.T) (*Store, *Pipeline, *PipelineRun) {
	t.Helper()
	store := NewStore()
	sess, err := store.CreateOrchSession("notes test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "arch → exec → review",
		Nodes: []AgentNode{
			{ID: "1", Type: NodeArchitect, Label: "arch", Executor: ExecutorReasonix, Mode: "run"},
			{ID: "2", Type: NodeExecutor, Label: "exec", Executor: ExecutorReasonix, Mode: "run"},
			{ID: "3", Type: NodeReviewer, Label: "review", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{{ID: "e1", FromID: "1", ToID: "2"}, {ID: "e2", FromID: "2", ToID: "3"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run := &PipelineRun{
		ID:                 "run_notes",
		PipelineRevisionID: rev.ID,
		SessionID:          sess.ID,
		Task:               "原始任务",
		NodeStates:         map[string]RunState{},
		NodeAttemptIDs:     []string{},
	}
	store.runs[run.ID] = run
	pipe := &Pipeline{Nodes: rev.Nodes, Edges: rev.Edges}
	return store, pipe, run
}

// TestNotesOnlyConsumedByExecutor verifies the injection contract: pending
// operator notes appear in the EXECUTOR node's input exactly once; the
// architect and reviewer must not consume (or even see) them.
func TestNotesOnlyConsumedByExecutor(t *testing.T) {
	store, pipe, run := newNoteTestStore(t)
	if err := store.AppendContinuationNote(run.ID, "优先补齐 round2 证据再动代码"); err != nil {
		t.Fatal(err)
	}

	// Reviewer input must NOT contain or consume the note.
	reviewInput := store.gatherInputV2(pipe, run, "3")
	if strings.Contains(reviewInput, "人工补充指令") || strings.Contains(reviewInput, "round2 证据") {
		t.Fatalf("reviewer input must not include operator notes, got: %s", reviewInput)
	}
	// Architect input must NOT consume it either.
	archInput := store.gatherInputV2(pipe, run, "1")
	if strings.Contains(archInput, "round2 证据") {
		t.Fatalf("architect input must not include operator notes, got: %s", archInput)
	}
	if len(run.ContinuationNotes) != 1 || run.ContinuationNotes[0].Consumed {
		t.Fatal("note must stay pending until an executor consumes it")
	}

	// Executor input contains AND consumes the note.
	execInput := store.gatherInputV2(pipe, run, "2")
	if !strings.Contains(execInput, "## 人工补充指令") || !strings.Contains(execInput, "round2 证据") {
		t.Fatalf("executor input missing injected note, got: %s", execInput)
	}
	if !run.ContinuationNotes[0].Consumed || run.ContinuationNotes[0].ConsumedByNodeID != "2" {
		t.Fatal("note must be marked consumed by node 2")
	}
	// Second assembly must not repeat it.
	execAgain := store.gatherInputV2(pipe, run, "2")
	if strings.Contains(execAgain, "round2 证据") {
		t.Fatalf("note consumed twice, got: %s", execAgain)
	}
	// Empty note text is rejected.
	if err := store.AppendContinuationNote(run.ID, "   "); err == nil {
		t.Fatal("empty note must error")
	}
}

// TestLoopIterationNotesConsumedByExecutor covers the Loop path
// (gatherInputForIteration): reviewer never sees them; executor does, once.
func TestLoopIterationNotesConsumedByExecutor(t *testing.T) {
	store, pipe, run := newNoteTestStore(t)
	if err := store.AppendContinuationNote(run.ID, "墙约束在 PATH_FOLLOW 中必须持续生效"); err != nil {
		t.Fatal(err)
	}
	reviewIn := store.gatherInputForIteration(pipe, run, "", "3")
	if strings.Contains(reviewIn, "墙约束") && strings.Contains(reviewIn, "人工补充指令") {
		t.Fatalf("loop reviewer input must not include notes section")
	}
	if len(run.ContinuationNotes) != 1 || run.ContinuationNotes[0].Consumed {
		t.Fatal("reviewer gathering must not consume notes")
	}
	execIn := store.gatherInputForIteration(pipe, run, "", "2")
	if !strings.Contains(execIn, "人工补充指令") || !strings.Contains(execIn, "墙约束在 PATH_FOLLOW") {
		t.Fatalf("loop executor input missing note, got: %s", execIn)
	}
	if !run.ContinuationNotes[0].Consumed {
		t.Fatal("loop executor must consume the note")
	}
}

// TestMovePendingContinuationNotes verifies retry hand-off: pending notes move
// to the child run and are marked consumed at the source (exactly-once).
func TestMovePendingContinuationNotes(t *testing.T) {
	store, _, src := newNoteTestStore(t)
	_ = store.AppendContinuationNote(src.ID, "第一条补充")
	_ = store.AppendContinuationNote(src.ID, "第二条补充")
	src.ContinuationNotes[0].Consumed = true // 已消费的不搬

	dst := &PipelineRun{ID: "run_child", SessionID: src.SessionID}
	n := store.MovePendingContinuationNotes(src, dst)
	if n != 1 {
		t.Fatalf("moved = %d, want 1 (only the pending one)", n)
	}
	if len(dst.ContinuationNotes) != 1 || dst.ContinuationNotes[0].Text != "第二条补充" {
		t.Fatalf("child notes = %+v", dst.ContinuationNotes)
	}
	if !src.ContinuationNotes[1].Consumed || src.ContinuationNotes[1].ConsumedByNodeID != "moved:"+dst.ID {
		t.Fatal("source note must be marked moved to prevent double injection")
	}
}
