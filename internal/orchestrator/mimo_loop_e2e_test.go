package orchestrator

import (
	"context"

	"os"
	"strings"
	"testing"
	"time"
)

// TestMimoAcpServeFixedLoopAndManualTurnEndToEnd drives a real `mimo acp`
// retained runtime through a fixed=2 Loop, then runs one Runtime Console
// manual turn against the surviving session. It is environment-gated because
// it calls the user's mimo provider and consumes tokens.
func TestMimoAcpServeFixedLoopAndManualTurnEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end mimo acp loop test in short mode")
	}
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("skipping: set RUN_INTEGRATION=1 to run the mimo acp loop integration test")
	}
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	defer mimoRuntimeMgr.cleanupAll()

	store := NewStore()
	old := executors[ExecutorMimo]
	executors[ExecutorMimo] = &MimoExecutor{}
	t.Cleanup(func() { executors[ExecutorMimo] = old })

	workspace := detectWorkspace()
	modelRef := resolveExecutorModelRef(workspace, ExecutorMimo, "serve", "mimo-v2.5")
	if modelRef == "" || modelRef == "mimo-v2.5" {
		t.Fatalf("resolved mimo model ref = %q, want installed provider/model ref", modelRef)
	}

	sess, err := store.CreateOrchSession("mimo acp e2e", workspace)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "mimo acp fixed 2",
		Nodes: []AgentNode{
			{ID: "s1", Type: NodeExecutor, Label: "执行者", Model: modelRef, Executor: ExecutorMimo, Mode: "serve"},
			{ID: "s2", Type: NodeReviewer, Label: "审查者", Model: modelRef, Executor: ExecutorMimo, Mode: "serve"},
		},
		Edges: []Edge{{ID: "e1", FromID: "s1", ToID: "s2"}},
		LoopConfig: LoopConfig{
			Enabled:         true,
			Mode:            "fixed",
			FixedIterations: 2,
			ReviewNodeID:    "s2",
			Protocol:        "loop-review-v1",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "请只回复 OK，然后结束。", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(15 * time.Minute)
	var final *PipelineRun
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			cp := r
			final = &cp
			break
		}
		time.Sleep(2 * time.Second)
	}
	if final == nil {
		t.Fatal("mimo acp fixed loop did not finish within 15 minutes")
	}
	if final.Status != "complete" {
		t.Fatalf("run.Status = %q, want complete; error: %s", final.Status, final.Error)
	}
	if final.TerminationReason != "fixed_limit" {
		t.Fatalf("TerminationReason = %q, want fixed_limit", final.TerminationReason)
	}
	if len(final.IterationIDs) != 2 {
		t.Fatalf("iteration count = %d, want 2", len(final.IterationIDs))
	}

	// Runtime Console manual turn against the surviving retained session. It
	// must answer without creating a new Run/Iteration.
	runtimes := mimoRuntimeMgr.List()
	if len(runtimes) == 0 {
		t.Fatal("no retained mimo runtime survived the loop")
	}
	turnID, err := SendMimoRuntimeMessage(ctx, runtimes[0].RuntimeID, "只回复OK")
	if err != nil {
		t.Fatalf("SendMimoRuntimeMessage: %v", err)
	}
	if turnID == "" {
		t.Fatal("manual turn returned an empty turnID")
	}
	seen := ""
	pollDeadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(pollDeadline) {
		snapshot, ok := GetMimoRuntimeConsole(runtimes[0].RuntimeID)
		if !ok {
			t.Fatalf("runtime console snapshot missing for %q", runtimes[0].RuntimeID)
		}
		if strings.Contains(snapshot.Output, "OK") {
			seen = snapshot.Output
			break
		}
		time.Sleep(2 * time.Second)
	}
	if seen == "" {
		t.Fatalf("manual turn did not produce visible output; snapshot: %#v", func() any {
			s, _ := GetMimoRuntimeConsole(runtimes[0].RuntimeID)
			return s
		}())
	}
}
