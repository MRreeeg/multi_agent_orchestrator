package orchestrator

import (
	"context"
	"testing"
)

func TestDisabledLoopConfigIsCanonicalized(t *testing.T) {
	got, err := NormalizeLoopConfig(&LoopConfig{
		Mode:            "fixed",
		MaxIterations:   9,
		FixedIterations: 4,
		ReviewNodeID:    "stale-reviewer",
		Protocol:        "loop-review-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Enabled || got.Mode != "" || got.MaxIterations != 0 || got.FixedIterations != 0 ||
		got.ReviewNodeID != "" || got.Protocol != "" || len(got.TargetNodeIDs) != 0 {
		t.Fatalf("disabled config = %+v, want zero config", got)
	}
}
func TestLoopConfigValidationUsesSameRulesForSaveAndRun(t *testing.T) {
	nodes := []AgentNode{{ID: "review", Type: NodeReviewer}}
	cases := []LoopConfig{
		{Enabled: true, Mode: "review_decides", MaxIterations: 0, ReviewNodeID: "review", Protocol: "loop-review-v1"},
		{Enabled: true, Mode: "review_decides", MaxIterations: 11, ReviewNodeID: "review", Protocol: "loop-review-v1"},
		{Enabled: true, Mode: "fixed", FixedIterations: 0, ReviewNodeID: "review", Protocol: "loop-review-v1"},
	}
	for _, cfg := range cases {
		if err := ValidateLoopConfig(&cfg, nodes); err == nil {
			t.Fatalf("invalid config %+v was accepted", cfg)
		}
		if _, err := NormalizeLoopConfig(&cfg); err == nil {
			t.Fatalf("invalid config %+v was accepted by runtime normalization", cfg)
		}
	}
}

func TestFixedLoopConfigLegacyMaxIterationsCompatibility(t *testing.T) {
	cfg := LoopConfig{Enabled: true, Mode: "fixed", MaxIterations: 4, ReviewNodeID: "review", Protocol: "loop-review-v1"}
	normalized, err := NormalizeLoopConfig(&cfg)
	if err != nil {
		t.Fatal(err)
	}
	if normalized.FixedIterations != 4 {
		t.Fatalf("FixedIterations = %d, want 4", normalized.FixedIterations)
	}
}

func TestInvalidLoopConfigRejectedAtomically(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("atomic", "")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "original",
		Nodes: []AgentNode{{ID: "review", Type: NodeReviewer}},
	})
	if err != nil {
		t.Fatal(err)
	}
	beforeID := sess.CurrentPipelineID
	before, _ := store.GetPipelineRevision(rev.ID)
	_, err = store.UpdatePipelineRevisionWithLoopConfig(sess.ID, "changed", []AgentNode{{ID: "executor", Type: NodeExecutor}}, nil, "manual_edit", &LoopConfig{
		Enabled: true, Mode: "review_decides", MaxIterations: 2, ReviewNodeID: "missing", Protocol: "loop-review-v1",
	})
	if err == nil {
		t.Fatal("invalid loop config was accepted")
	}
	afterSess, _ := store.GetOrchSession(sess.ID)
	after, _ := store.GetPipelineRevision(rev.ID)
	if afterSess.CurrentPipelineID != beforeID {
		t.Fatalf("current revision changed from %q to %q", beforeID, afterSess.CurrentPipelineID)
	}
	if after.Nodes[0].ID != before.Nodes[0].ID || after.Name != before.Name {
		t.Fatalf("revision changed after rejected save: before=%+v after=%+v", before, after)
	}
}

func TestPipelineRevisionLoopConfigRoundTripAndRunCopy(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("round trip", "")
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "loop",
		Nodes: []AgentNode{{ID: "review", Type: NodeReviewer}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := &LoopConfig{Enabled: true, Mode: "fixed", MaxIterations: 3, ReviewNodeID: "review", Protocol: "loop-review-v1"}
	rev, err = store.UpdatePipelineRevisionWithLoopConfig(sess.ID, "loop", rev.Nodes, rev.Edges, "manual_edit", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if rev.LoopConfig.FixedIterations != 3 || rev.LoopConfig.MaxIterations != 3 {
		t.Fatalf("saved config was not normalized: %+v", rev.LoopConfig)
	}
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	if !loopConfigEqual(run.LoopConfig, rev.LoopConfig) {
		t.Fatalf("run config = %+v, revision config = %+v", run.LoopConfig, rev.LoopConfig)
	}
}

func TestResumeRunIDReusesExistingRun(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("resume", "")
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "loop", Nodes: []AgentNode{{ID: "review", Type: NodeReviewer}},
		LoopConfig: LoopConfig{Enabled: true, Mode: "fixed", FixedIterations: 2, ReviewNodeID: "review", Protocol: "loop-review-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	iter := LoopIteration{ID: "iter_resume_api", RunID: run.ID, Number: 1, Status: "interrupted", InputTask: "task"}
	if err := store.CreateIteration(iter); err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	run.Status = "interrupted"
	run.IterationIDs = []string{iter.ID}
	store.mu.Unlock()
	if err := store.persistRun(run, sess.ID); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resumed, err := store.ExecutePipelineV2(ctx, sess.ID, "ignored-current-revision", "", "", ExecutionOptions{ResumeRunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != run.ID || resumed.Trigger != "resume" {
		t.Fatalf("resumed run = id %q trigger %q", resumed.ID, resumed.Trigger)
	}
	store.mu.RLock()
	count := len(store.runs)
	store.mu.RUnlock()
	if count != 1 {
		t.Fatalf("run count = %d, want 1", count)
	}
}

func TestResumeRejectsRunFromAnotherSession(t *testing.T) {
	store := NewStore()
	sess1, _ := store.CreateOrchSession("one", "")
	sess2, _ := store.CreateOrchSession("two", "")
	rev, _ := store.CreatePipelineRevision(sess1.ID, PipelineRevision{
		Nodes:      []AgentNode{{ID: "review", Type: NodeReviewer}},
		LoopConfig: LoopConfig{Enabled: true, Mode: "fixed", FixedIterations: 1, ReviewNodeID: "review", Protocol: "loop-review-v1"},
	})
	run, _ := store.CreateRun(sess1.ID, rev.ID, "task", "", "manual", "")
	store.mu.Lock()
	run.Status = "interrupted"
	store.mu.Unlock()
	_, err := store.ExecutePipelineV2(context.Background(), sess2.ID, "", "", "", ExecutionOptions{ResumeRunID: run.ID})
	if err == nil {
		t.Fatal("cross-session resume was accepted")
	}
}

func TestIterationListIsOrdered(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("iterations", "")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{Nodes: []AgentNode{{ID: "n", Type: NodeExecutor}}})
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{3, 1, 2} {
		if err := store.CreateIteration(LoopIteration{ID: "iter_order_" + string(rune('0'+n)), RunID: run.ID, Number: n, Status: "passed"}); err != nil {
			t.Fatal(err)
		}
	}
	iterations := store.ListIterations(run.ID)
	if len(iterations) != 3 || iterations[0].Number != 1 || iterations[1].Number != 2 || iterations[2].Number != 3 {
		t.Fatalf("iterations not ordered: %+v", iterations)
	}
}
