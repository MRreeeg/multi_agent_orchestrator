package orchestrator

import (
	"context"
	"testing"
	"time"
)

func TestRunSnapshotsSessionWorkspace(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("workspace snapshot", `G:\workspace-before`)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{Name: "empty"})
	if err != nil {
		t.Fatal(err)
	}

	run, err := store.ExecutePipelineV2(context.Background(), sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	if got := run.ExecOptions.Workspace; got != `G:\workspace-before` {
		t.Fatalf("run workspace = %q, want session workspace", got)
	}

	if err := store.UpdateOrchSession(sess.ID, func(s *OrchestrationSession) {
		s.Workspace = `G:\workspace-after`
	}); err != nil {
		t.Fatal(err)
	}
	persisted, ok := store.GetRun(run.ID)
	if !ok {
		t.Fatal("run disappeared after session update")
	}
	if got := persisted.ExecOptions.Workspace; got != `G:\workspace-before` {
		t.Fatalf("historical run workspace changed to %q after session update", got)
	}
}

func TestCreateRunSnapshotsSessionWorkspace(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("workspace create run", `G:\workspace-create-run`)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{Name: "empty"})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := run.ExecOptions.Workspace; got != `G:\workspace-create-run` {
		t.Fatalf("CreateRun workspace = %q, want session workspace", got)
	}
}

func TestProviderSessionIsNotReusedAcrossWorkspaces(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("workspace provider", `G:\workspace-a`)
	if err != nil {
		t.Fatal(err)
	}
	node := AgentNode{ID: "exec", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"}
	_, first, err := store.FindOrCreateBindingAndProviderSession(sess.ID, node.ID, node, string(node.Executor), `G:\workspace-a`, "reuse", true)
	if err != nil {
		t.Fatal(err)
	}
	_, second, err := store.FindOrCreateBindingAndProviderSession(sess.ID, node.ID, node, string(node.Executor), `G:\workspace-b`, "reuse", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID == second.ID {
		t.Fatalf("provider session %q was reused across workspaces", first.ID)
	}
	if second.Workspace != `G:\workspace-b` {
		t.Fatalf("new provider workspace = %q, want workspace-b", second.Workspace)
	}
}

func TestRunExecutionUsesSessionWorkspace(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("workspace execution", `G:\workspace-execution`)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "executor",
		Nodes: []AgentNode{{ID: "exec", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	specCh := make(chan ExecSpec, 1)
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = stubExecutorFunc(func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		specCh <- spec
		return &ExecResult{FinalText: "workspace-ok"}, nil
	})
	defer func() { executors[ExecutorReasonix] = old }()

	if _, err := store.ExecutePipelineV2(context.Background(), sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"}); err != nil {
		t.Fatal(err)
	}
	select {
	case spec := <-specCh:
		if spec.Workspace != `G:\workspace-execution` {
			t.Fatalf("executor workspace = %q, want session workspace", spec.Workspace)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("executor was not called")
	}
}

func TestResumeKeepsOriginalWorkspace(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("workspace resume", `G:\workspace-original`)
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:       "loop",
		Nodes:      []AgentNode{{ID: "review", Type: NodeReviewer}},
		LoopConfig: LoopConfig{Enabled: true, Mode: "fixed", FixedIterations: 1, ReviewNodeID: "review", Protocol: "loop-review-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	iter := LoopIteration{ID: "iter_workspace_resume", RunID: run.ID, Number: 1, Status: IterationInterrupted, InputTask: "task"}
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
	if err := store.UpdateOrchSession(sess.ID, func(s *OrchestrationSession) {
		s.Workspace = `G:\workspace-changed`
	}); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	resumed, err := store.ExecutePipelineV2(ctx, sess.ID, "ignored", "", "", ExecutionOptions{ResumeRunID: run.ID})
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != run.ID {
		t.Fatalf("resume created a new run: got %q, want %q", resumed.ID, run.ID)
	}
	if got := resumed.ExecOptions.Workspace; got != `G:\workspace-original` {
		t.Fatalf("resumed workspace = %q, want original workspace", got)
	}
}
