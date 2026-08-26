package orchestrator

import (
	"context"
	"errors"
	"strings"
	"testing"
)

// TestSeedRetryRunSeedsOnlyUpstream verifies that SeedRetryRun copies complete
// attempts/states strictly from the transitive upstream of the retry node.
func TestSeedRetryRunSeedsOnlyUpstream(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("retry test", "/tmp")
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
	pipe := &Pipeline{Nodes: rev.Nodes, Edges: rev.Edges}

	src := &PipelineRun{
		ID:                 "run_src",
		PipelineRevisionID: rev.ID,
		SessionID:          sess.ID,
		NodeStates:         map[string]RunState{},
		NodeAttemptIDs:     []string{},
	}
	// architect complete (attempt + state); executor failed; reviewer pending.
	// Attempts are injected directly into the store map to avoid persistence
	// side effects in unit scope.
	mkAttempt := func(id, nodeID, status, output string) {
		store.attempts[id] = &NodeAttempt{ID: id, RunID: src.ID, NodeID: nodeID, Status: status, Output: output}
		src.NodeAttemptIDs = append(src.NodeAttemptIDs, id)
	}
	mkAttempt("att_a1", "1", "complete", "架构方案")
	mkAttempt("att_e1", "2", "failed", "")
	src.NodeStates["1"] = RunState{Status: NodeComplete, Output: "架构方案"}
	src.NodeStates["2"] = RunState{Status: NodeFailed, Error: "context deadline"}

	dst := &PipelineRun{
		ID:                 "run_dst",
		PipelineRevisionID: rev.ID,
		SessionID:          sess.ID,
		NodeStates:         map[string]RunState{},
		NodeAttemptIDs:     []string{},
	}
	if err := store.SeedRetryRun(dst, src, pipe, "2"); err != nil {
		t.Fatalf("SeedRetryRun: %v", err)
	}

	if !dst.SeededNodes["1"] {
		t.Fatal("upstream completed architect must be seeded")
	}
	if dst.SeededNodes["2"] || dst.SeededNodes["3"] {
		t.Fatal("the retry node itself and downstream nodes must not be seeded")
	}
	// The seeded attempt carries the output into dst so gatherInputV2 finds it.
	var seededOut string
	for _, attID := range dst.NodeAttemptIDs {
		a, ok := store.attempts[attID]
		if ok && a.NodeID == "1" {
			seededOut = a.Output
		}
	}
	if seededOut != "架构方案" {
		t.Fatalf("seeded attempt output = %q, want 架构方案", seededOut)
	}
	// Failed executor attempt must NOT be cloned into dst.
	for _, attID := range dst.NodeAttemptIDs {
		if a, _ := store.attempts[attID]; a.NodeID == "2" {
			t.Fatal("failed node attempt must not be seeded")
		}
	}
	if st := dst.NodeStates["2"]; st.Status != NodePending && st.Status != "" {
		t.Fatalf("retry node state should stay pending, got %q", st.Status)
	}
	// gatherInputV2 for the retry node must include the seeded upstream output.
	input := store.gatherInputV2(pipe, dst, "2")
	if !strings.Contains(input, "架构方案") {
		t.Fatalf("gatherInputV2 for retry node should contain seeded upstream output, got: %s", input)
	}
	// Unknown retry node fails loudly.
	if err := store.SeedRetryRun(dst, src, pipe, "nope"); err == nil {
		t.Fatal("unknown retry node must error")
	}
}

// TestFirstBrokenNodePicksTopologicalFirst checks the auto-pick fallback used
// by the run-list retry button.
func TestFirstBrokenNodePicksTopologicalFirst(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("broken pick", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "chain",
		Nodes: []AgentNode{{ID: "a", Type: NodeArchitect, Label: "arch"}, {ID: "b", Type: NodeExecutor, Label: "exec"}},
		Edges: []Edge{{ID: "e", FromID: "a", ToID: "b"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run := &PipelineRun{
		ID:                 "run_broken",
		PipelineRevisionID: rev.ID,
		SessionID:          sess.ID,
		NodeStates:         map[string]RunState{"a": {Status: NodeFailed}, "b": {Status: NodeFailed}},
	}
	store.runs[run.ID] = run
	if got := store.FirstBrokenNodeID(run.ID); got != "a" {
		t.Fatalf("first broken node = %q, want a (topologically first)", got)
	}
	run.NodeStates["a"] = RunState{Status: NodeComplete}
	if got := store.FirstBrokenNodeID(run.ID); got != "b" {
		t.Fatalf("first broken node = %q, want b after a completes", got)
	}
}

// TestStallErrorsAndBusySentinelAreDistinctFromContextErrors guards the
// loop.go force-stop branch: only genuine scheduler cancellations may tear
// down retained runtimes; stall and busy errors must not match them.
func TestStallErrorsAndBusySentinelAreDistinctFromContextErrors(t *testing.T) {
	errs := []error{ErrTurnIdleTimeout, ErrTurnMaxDuration, ErrRuntimeBusy}
	for _, err := range errs {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%v must not match a context error", err)
		}
	}
	if !strings.Contains(ErrRuntimeBusy.Error(), "busy") {
		t.Fatalf("ErrRuntimeBusy text should mention busy, got %q", ErrRuntimeBusy.Error())
	}
}
