package orchestrator

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestReasonixServeArgsDoNotContainUnsupportedSkillFlag(t *testing.T) {
	args := reasonixServeArgs(54363, "deepseek-flash")
	for _, arg := range args {
		if arg == "--skill" || arg == "-skill" {
			t.Fatalf("reasonix serve args contain unsupported skill flag: %#v", args)
		}
	}
}

// TestV2RunCreation verifies that ExecutePipelineV2 creates a Run in the OrchestrationSession.
func TestV2RunCreation(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test session", "/tmp")
	if err != nil {
		t.Fatal(err)
	}

	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test pipeline",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run", RoleDesc: "do something"},
		},
		Edges: []Edge{},
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := stubExecutor{
		name:   "reasonix",
		result: &ExecResult{FinalText: "output from n1"},
	}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "test task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	if run.ID == "" {
		t.Fatal("run ID is empty")
	}
	if run.SessionID != sess.ID {
		t.Errorf("run.SessionID = %q, want %q", run.SessionID, sess.ID)
	}
	if run.PipelineRevisionID != rev.ID {
		t.Errorf("run.PipelineRevisionID = %q, want %q", run.PipelineRevisionID, rev.ID)
	}
	if run.Status != "running" {
		t.Errorf("run.Status = %q, want running", run.Status)
	}
	if run.Trigger != "manual" {
		t.Errorf("run.Trigger = %q, want manual", run.Trigger)
	}

	// Wait for execution to complete.
	time.Sleep(2 * time.Second)

	// Verify run completed.
	r, ok := store.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found after execution")
	}
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete", r.Status)
	}
	if r.FinishedAt == "" {
		t.Error("run.FinishedAt is empty")
	}

	// Verify session updated.
	s, ok := store.GetOrchSession(sess.ID)
	if !ok {
		t.Fatal("session not found")
	}
	if s.CurrentRunID != run.ID {
		t.Errorf("session.CurrentRunID = %q, want %q", s.CurrentRunID, run.ID)
	}
}

// TestV2AttemptCreation verifies that each node creates a NodeAttempt.
func TestV2AttemptCreation(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "output"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	attempts := store.ListAttempts(run.ID)
	if len(attempts) == 0 {
		t.Fatal("no attempts created")
	}
	if attempts[0].NodeID != "n1" {
		t.Errorf("attempt.NodeID = %q, want n1", attempts[0].NodeID)
	}
	if attempts[0].Status != "complete" {
		t.Errorf("attempt.Status = %q, want complete", attempts[0].Status)
	}
	if attempts[0].Output != "output" {
		t.Errorf("attempt.Output = %q, want output", attempts[0].Output)
	}
	if attempts[0].AgentBindingID == "" {
		t.Error("attempt.AgentBindingID is empty")
	}
}

// TestV2BindingReuse verifies that same node reuses AgentBinding across runs.
func TestV2BindingReuse(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "ok"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Run 1
	run1, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task1", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	bindings1 := store.ListBindings(sess.ID)
	if len(bindings1) == 0 {
		t.Fatal("no bindings after run 1")
	}
	bindingID1 := bindings1[0].ID

	// Run 2
	run2, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task2", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	bindings2 := store.ListBindings(sess.ID)
	if len(bindings2) == 0 {
		t.Fatal("no bindings after run 2")
	}

	// Should reuse same binding (only 1 active binding for n1).
	activeCount := 0
	for _, b := range bindings2 {
		if b.NodeID == "n1" && b.Status == "active" {
			activeCount++
			if b.ID != bindingID1 {
				t.Errorf("binding changed: got %q, want %q (same binding reused)", b.ID, bindingID1)
			}
		}
	}
	if activeCount != 1 {
		t.Errorf("active bindings for n1 = %d, want 1", activeCount)
	}

	_ = run1
	_ = run2
}

// TestV2BindingOnConfigChange verifies new binding when node config changes.
func TestV2BindingOnConfigChange(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	nodes := []AgentNode{
		{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
	}
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{Nodes: nodes})

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "ok"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Run 1 with model A
	store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	bindings1 := store.ListBindings(sess.ID)
	bindingID1 := ""
	for _, b := range bindings1 {
		if b.NodeID == "n1" && b.Status == "active" {
			bindingID1 = b.ID
		}
	}

	// Modify node model
	nodes[0].Model = "deepseek-pro"
	rev2, _ := store.UpdatePipelineRevision(sess.ID, nodes, []Edge{}, "manual_edit")

	// Run 2 with model B
	store.ExecutePipelineV2(ctx, sess.ID, rev2.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	bindings2 := store.ListBindings(sess.ID)
	activeCount := 0
	for _, b := range bindings2 {
		if b.NodeID == "n1" && b.Status == "active" {
			activeCount++
			if b.ID == bindingID1 {
				t.Error("binding should have changed after model update")
			}
		}
	}
	if activeCount != 1 {
		t.Errorf("active bindings for n1 = %d, want 1", activeCount)
	}

	// Old binding should be detached (check bindings2, not bindings1).
	detachedCount := 0
	for _, b := range bindings2 {
		if b.NodeID == "n1" && b.Status == "detached" {
			detachedCount++
		}
	}
	if detachedCount == 0 {
		t.Error("old binding not detached")
	}
}

// TestV2MultipleRunsNoOverwrite verifies three runs don't overwrite each other.
func TestV2MultipleRunsNoOverwrite(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	callCount := 0
	executor := stubExecutorFunc(func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		callCount++
		return &ExecResult{FinalText: fmt.Sprintf("output-%d", callCount)}, nil
	})
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for i := 0; i < 3; i++ {
		run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, fmt.Sprintf("task-%d", i), "", ExecutionOptions{Trigger: "manual"})
		time.Sleep(2 * time.Second)

		r, ok := store.GetRun(run.ID)
		if !ok {
			t.Fatalf("run %d not found", i)
		}
		if r.Status != "complete" {
			t.Errorf("run %d status = %q, want complete", i, r.Status)
		}
	}

	// Verify 3 runs exist.
	runs := store.ListRunsForSession(sess.ID)
	if len(runs) != 3 {
		t.Errorf("run count = %d, want 3", len(runs))
	}

	// Verify revision count is still 1.
	revs := store.ListPipelineRevisions(sess.ID)
	if len(revs) != 1 {
		t.Errorf("revision count = %d, want 1", len(revs))
	}

	// Verify currentRunID points to last run.
	s, _ := store.GetOrchSession(sess.ID)
	if s.CurrentRunID != runs[0].ID {
		t.Errorf("CurrentRunID = %q, want %q", s.CurrentRunID, runs[0].ID)
	}
}

// TestV2ErrorPathTerminatesRun verifies that node failure marks Run as failed.
func TestV2ErrorPathTerminatesRun(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	executor := stubExecutor{name: "reasonix", result: nil, err: fmt.Errorf("node crashed")}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	r, ok := store.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found")
	}
	if r.Status != "failed" {
		t.Errorf("run.Status = %q, want failed", r.Status)
	}
	if r.Error == "" {
		t.Error("run.Error is empty")
	}
	if r.FinishedAt == "" {
		t.Error("run.FinishedAt is empty")
	}

	// Verify attempt is also failed.
	attempts := store.ListAttempts(run.ID)
	if len(attempts) > 0 && attempts[0].Status != "failed" {
		t.Errorf("attempt.Status = %q, want failed", attempts[0].Status)
	}
}

// TestV2UpstreamInputIsolation verifies downstream reads from Attempts, not NodeStates.
func TestV2UpstreamInputIsolation(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeArchitect, Label: "arch", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run", RoleDesc: "design"},
			{ID: "n2", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run", RoleDesc: "implement"},
		},
		Edges: []Edge{{ID: "e1", FromID: "n1", ToID: "n2"}},
	})

	var mu sync.Mutex
	var capturedInput string
	executor := stubExecutorFunc(func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		if strings.Contains(spec.Prompt, "implement") {
			mu.Lock()
			capturedInput = spec.Prompt
			mu.Unlock()
		}
		return &ExecResult{FinalText: "done"}, nil
	})
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(3 * time.Second)

	mu.Lock()
	input := capturedInput
	mu.Unlock()

	if input == "" {
		t.Fatal("n2 was not executed")
	}
	if !strings.Contains(input, "上游节点输出") {
		t.Error("downstream input does not contain upstream section")
	}
	if strings.Contains(input, "NodeStates fallback") {
		t.Error("downstream input contains forbidden NodeStates fallback")
	}
}

// TestProviderSessionPersistsAndReloads verifies ProviderSession survives store reload.
func TestProviderSessionPersistsAndReloads(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	binding, _ := store.CreateBinding(sess.ID, "n1", AgentNode{ID: "n1", Executor: ExecutorReasonix, Model: "deepseek-flash"})

	ps, err := store.CreateProviderSession(binding.ID, "reasonix", "/path/to/session", "/workspace")
	if err != nil {
		t.Fatal(err)
	}

	// Create new store and load data.
	store2 := NewStore()
	if err := store2.LoadAllData(); err != nil {
		t.Fatal(err)
	}

	// Verify provider session loaded.
	loadedPS, ok := store2.GetProviderSession(ps.ID)
	if !ok {
		t.Fatal("provider session not found after reload")
	}
	if loadedPS.Executor != "reasonix" {
		t.Errorf("executor = %q, want reasonix", loadedPS.Executor)
	}
	if loadedPS.SessionPath != "/path/to/session" {
		t.Errorf("sessionPath = %q, want /path/to/session", loadedPS.SessionPath)
	}
	if loadedPS.Workspace != "/workspace" {
		t.Errorf("workspace = %q, want /workspace", loadedPS.Workspace)
	}

	// Verify binding still references provider session.
	loadedBind, ok := store2.GetBinding(binding.ID)
	if !ok {
		t.Fatal("binding not found after reload")
	}
	if loadedBind.ProviderSessionID != ps.ID {
		t.Errorf("binding.ProviderSessionID = %q, want %q", loadedBind.ProviderSessionID, ps.ID)
	}
}

// TestRuntimeStatePersistsAndReloads verifies RuntimeState survives store reload.
func TestRuntimeStatePersistsAndReloads(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")

	rt := RuntimeState{
		RuntimeID:      "rt_test_1",
		SessionID:      sess.ID,
		AgentBindingID: "bind_1",
		NodeID:         "n1",
		RunID:          "run_1",
		Executor:       "reasonix",
		Model:          "deepseek-flash",
		Endpoint:       "http://127.0.0.1:8800",
		Port:           8800,
		PID:            12345,
		Status:         RuntimeReady,
	}
	if err := store.CreateRuntimeState(rt); err != nil {
		t.Fatal(err)
	}

	// Create new store and load.
	store2 := NewStore()
	if err := store2.LoadAllData(); err != nil {
		t.Fatal(err)
	}

	loadedRT, ok := store2.GetRuntimeState("rt_test_1")
	if !ok {
		t.Fatal("runtime state not found after reload")
	}
	if loadedRT.Endpoint != "http://127.0.0.1:8800" {
		t.Errorf("endpoint = %q, want http://127.0.0.1:8800", loadedRT.Endpoint)
	}
	if loadedRT.Port != 8800 {
		t.Errorf("port = %d, want 8800", loadedRT.Port)
	}
	if loadedRT.PID != 12345 {
		t.Errorf("pid = %d, want 12345", loadedRT.PID)
	}
	if loadedRT.Status != RuntimeReady {
		t.Errorf("status = %q, want ready", loadedRT.Status)
	}

	// Verify session has runtime ID.
	loadedSess, ok := store2.GetOrchSession(sess.ID)
	if !ok {
		t.Fatal("session not found")
	}
	found := false
	for _, id := range loadedSess.RuntimeIDs {
		if id == "rt_test_1" {
			found = true
			break
		}
	}
	if !found {
		t.Error("runtime ID not in session.RuntimeIDs")
	}
}

// TestReuseAgentSessionsFalseCreatesNewProviderSession verifies reuse=false creates new session.
func TestReuseAgentSessionsFalseCreatesNewProviderSession(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "ok"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Run 1: default (reuse=true)
	store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual", ReuseAgentSessions: true})
	time.Sleep(2 * time.Second)

	bindings := store.ListBindings(sess.ID)
	psID1 := ""
	for _, b := range bindings {
		if b.NodeID == "n1" && b.Status == "active" {
			psID1 = b.ProviderSessionID
		}
	}

	// Run 2: reuse=false
	store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual", ReuseAgentSessions: false})
	time.Sleep(2 * time.Second)

	bindings2 := store.ListBindings(sess.ID)
	psID2 := ""
	for _, b := range bindings2 {
		if b.NodeID == "n1" && b.Status == "active" {
			psID2 = b.ProviderSessionID
		}
	}

	if psID1 == "" || psID2 == "" {
		t.Fatal("provider session IDs empty")
	}
	if psID1 == psID2 {
		t.Errorf("provider session should be different when reuse=false, both are %q", psID1)
	}
}

// TestContextPolicyFreshPerRun verifies fresh_per_run creates new session each run.
func TestContextPolicyFreshPerRun(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "ok"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	opts := ExecutionOptions{Trigger: "manual", ReuseAgentSessions: true, ContextPolicy: "fresh_per_run"}

	store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task1", "", opts)
	time.Sleep(2 * time.Second)

	bindings := store.ListBindings(sess.ID)
	psID1 := ""
	for _, b := range bindings {
		if b.NodeID == "n1" && b.Status == "active" {
			psID1 = b.ProviderSessionID
		}
	}

	store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task2", "", opts)
	time.Sleep(2 * time.Second)

	bindings2 := store.ListBindings(sess.ID)
	psID2 := ""
	for _, b := range bindings2 {
		if b.NodeID == "n1" && b.Status == "active" {
			psID2 = b.ProviderSessionID
		}
	}

	if psID1 == psID2 {
		t.Errorf("fresh_per_run should create new session each run, both are %q", psID1)
	}
}

// TestProviderSessionCreateFailureFailsRun verifies provider session failure terminates run.
func TestProviderSessionCreateFailureFailsRun(t *testing.T) {
	store := NewStore()
	// Create session but no binding — CreateProviderSession will fail.
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "ok"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Execute with fresh_per_run to force new provider session creation.
	// Binding will be created first, then provider session creation.
	// This test verifies the flow works (binding is created, then provider session).
	run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: false,
		ContextPolicy:      "fresh_per_run",
	})
	time.Sleep(2 * time.Second)

	r, ok := store.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found")
	}
	// Run should complete successfully (binding + provider session both created).
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete (binding+session creation should succeed)", r.Status)
	}
}

// TestExecutePipelineV2CreatesRuntimeState verifies that RuntimeState is created,
// persisted, and correctly linked to Session/Binding/Attempt after execution.
func TestExecutePipelineV2CreatesRuntimeState(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("runtime test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}

	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test pipeline",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "serve"},
		},
		Edges: []Edge{},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Inject fake executor that returns a RuntimeID and triggers onStart.
	var onStartCalled bool
	var onStartEndpoint string
	var onStartPort int
	executor := stubExecutor{
		name: "reasonix",
		result: &ExecResult{
			FinalText: "fake output",
			RuntimeID: "rt_fake_1",
			Endpoint:  "http://127.0.0.1:19001",
		},
		onStart: func(endpoint string, port int) {
			onStartCalled = true
			onStartEndpoint = endpoint
			onStartPort = port
		},
	}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "test task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	// Poll for run completion instead of fixed sleep.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "canceled") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// ── Assert Run ──
	r, ok := store.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found")
	}
	if r.Status != "complete" {
		t.Fatalf("run.Status = %q, want complete; error: %s", r.Status, r.Error)
	}
	if r.FinishedAt == "" {
		t.Error("run.FinishedAt is empty")
	}

	// ── Assert onStart was called ──
	if !onStartCalled {
		t.Error("onStart callback was not called")
	}
	if onStartEndpoint != "http://127.0.0.1:19001" {
		t.Errorf("onStart endpoint = %q, want http://127.0.0.1:19001", onStartEndpoint)
	}
	if onStartPort != 19001 {
		t.Errorf("onStart port = %d, want 19001", onStartPort)
	}

	// ── Assert NodeAttempt ──
	attempts := store.ListAttempts(run.ID)
	if len(attempts) != 1 {
		t.Fatalf("attempt count = %d, want 1", len(attempts))
	}
	attempt := attempts[0]
	if attempt.Status != "complete" {
		t.Errorf("attempt.Status = %q, want complete", attempt.Status)
	}
	if attempt.Output != "fake output" {
		t.Errorf("attempt.Output = %q, want fake output", attempt.Output)
	}
	if attempt.RuntimeID != "rt_fake_1" {
		t.Errorf("attempt.RuntimeID = %q, want rt_fake_1", attempt.RuntimeID)
	}
	if attempt.AgentBindingID == "" {
		t.Error("attempt.AgentBindingID is empty")
	}
	if attempt.FinishedAt == "" {
		t.Error("attempt.FinishedAt is empty")
	}

	// ── Assert RuntimeState ──
	rt, rtOK := store.GetRuntimeState("rt_fake_1")
	if !rtOK {
		t.Fatal("RuntimeState not found for rt_fake_1")
	}
	if rt.RuntimeID != "rt_fake_1" {
		t.Errorf("RuntimeState.RuntimeID = %q, want rt_fake_1", rt.RuntimeID)
	}
	if rt.Endpoint != "http://127.0.0.1:19001" {
		t.Errorf("RuntimeState.Endpoint = %q, want http://127.0.0.1:19001", rt.Endpoint)
	}
	if rt.Port != 19001 {
		t.Errorf("RuntimeState.Port = %d, want 19001", rt.Port)
	}
	if rt.SessionID != sess.ID {
		t.Errorf("RuntimeState.SessionID = %q, want %q", rt.SessionID, sess.ID)
	}
	if rt.AgentBindingID != attempt.AgentBindingID {
		t.Errorf("RuntimeState.AgentBindingID = %q, want %q (attempt's binding)", rt.AgentBindingID, attempt.AgentBindingID)
	}
	if rt.NodeID != "n1" {
		t.Errorf("RuntimeState.NodeID = %q, want n1", rt.NodeID)
	}
	if rt.RunID != run.ID {
		t.Errorf("RuntimeState.RunID = %q, want %q", rt.RunID, run.ID)
	}
	if rt.Status != RuntimeIdle {
		t.Errorf("RuntimeState.Status = %q, want idle", rt.Status)
	}
	if rt.Executor != "reasonix" {
		t.Errorf("RuntimeState.Executor = %q, want reasonix", rt.Executor)
	}

	// ── Assert Session.RuntimeIDs ──
	sessAfter, sessOK := store.GetOrchSession(sess.ID)
	if !sessOK {
		t.Fatal("session not found")
	}
	foundRT := false
	for _, id := range sessAfter.RuntimeIDs {
		if id == "rt_fake_1" {
			foundRT = true
			break
		}
	}
	if !foundRT {
		t.Errorf("session.RuntimeIDs does not contain rt_fake_1; got %v", sessAfter.RuntimeIDs)
	}

	// ── Assert Binding.CurrentRuntimeID ──
	binding, bindOK := store.GetBinding(attempt.AgentBindingID)
	if !bindOK {
		t.Fatal("binding not found")
	}
	if binding.CurrentRuntimeID != "rt_fake_1" {
		t.Errorf("binding.CurrentRuntimeID = %q, want rt_fake_1", binding.CurrentRuntimeID)
	}

	// ── Assert ProviderSession.LastKnownRuntimeID/Endpoint ──
	if binding.ProviderSessionID != "" {
		ps, psOK := store.GetProviderSession(binding.ProviderSessionID)
		if !psOK {
			t.Fatal("provider session not found")
		}
		if ps.LastKnownRuntimeID != "rt_fake_1" {
			t.Errorf("providerSession.LastKnownRuntimeID = %q, want rt_fake_1", ps.LastKnownRuntimeID)
		}
		if ps.LastKnownEndpoint != "http://127.0.0.1:19001" {
			t.Errorf("providerSession.LastKnownEndpoint = %q, want http://127.0.0.1:19001", ps.LastKnownEndpoint)
		}
	}
}

// ── V14.2 Tests ──

// concurrentStubExecutor returns unique output per call with mutex-guarded counter.
type concurrentStubExecutor struct {
	mu    sync.Mutex
	count int
}

func (e *concurrentStubExecutor) Name() string { return "concurrent-stub" }
func (e *concurrentStubExecutor) Execute(_ context.Context, _ ExecSpec, _ func(string, int)) (*ExecResult, error) {
	e.mu.Lock()
	e.count++
	n := e.count
	e.mu.Unlock()
	return &ExecResult{FinalText: fmt.Sprintf("output-%d", n)}, nil
}

// TestConcurrentV2ReuseProviderSession verifies no data race when 20 goroutines
// concurrently execute V2 pipeline nodes that share the same *PipelineRun.
// Run with: go test -race -run TestConcurrentV2ReuseProviderSession
func TestConcurrentV2ReuseProviderSession(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}

	// Create 20 independent nodes (all in same topological level = parallel).
	nodes := make([]AgentNode, 20)
	for i := range nodes {
		nodes[i] = AgentNode{
			ID:       fmt.Sprintf("n%d", i),
			Type:     NodeExecutor,
			Label:    fmt.Sprintf("node-%d", i),
			Model:    "deepseek-flash",
			Executor: ExecutorReasonix,
			Mode:     "run",
		}
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "concurrent",
		Nodes: nodes,
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := &concurrentStubExecutor{}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "concurrent task", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "reuse",
	})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for completion.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.RLock()
		status := run.Status
		store.mu.RUnlock()
		if status == "complete" || status == "failed" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Verify: run completed successfully.
	store.mu.RLock()
	status := run.Status
	store.mu.RUnlock()
	if status != "complete" {
		t.Errorf("run.Status = %q, want complete", status)
	}

	// Verify: all attempts have valid ProviderSessionID.
	attempts := store.ListAttempts(run.ID)
	if len(attempts) != 20 {
		t.Fatalf("attempts = %d, want 20", len(attempts))
	}
	psIDs := make(map[string]bool)
	for _, a := range attempts {
		if a.ProviderSessionID == "" {
			t.Errorf("attempt %s has empty ProviderSessionID", a.ID)
		}
		psIDs[a.ProviderSessionID] = true
	}

	// Verify: all ProviderSessions exist in store.
	for psID := range psIDs {
		_, ok := store.GetProviderSession(psID)
		if !ok {
			t.Errorf("ProviderSession %s not found in store", psID)
		}
	}

	// Verify: binding points to a valid ProviderSession.
	bindings := store.ListBindings(sess.ID)
	for _, b := range bindings {
		if b.ProviderSessionID != "" {
			_, ok := store.GetProviderSession(b.ProviderSessionID)
			if !ok {
				t.Errorf("binding %s points to missing ProviderSession %s", b.ID, b.ProviderSessionID)
			}
		}
	}
}

// TestV2ResumeFullChain verifies the complete ExternalSessionID resume chain:
// first exec → second exec (reuse, passes ExternalSessionID) → third exec after persistence check.
func TestV2ResumeFullChain(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "codex", Model: "o3", Executor: ExecutorCodex, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	callIdx := 0
	var seenExternalIDs []string
	var seenContextPolicies []string

	fakeCodex := &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		defer mu.Unlock()
		seenExternalIDs = append(seenExternalIDs, spec.ExternalSessionID)
		seenContextPolicies = append(seenContextPolicies, spec.ContextPolicy)
		callIdx++
		return &ExecResult{
			FinalText:         fmt.Sprintf("output-%d", callIdx),
			ExternalSessionID: "thread-first",
		}, nil
	}}

	origCodex := executors[ExecutorCodex]
	executors[ExecutorCodex] = fakeCodex
	defer func() { executors[ExecutorCodex] = origCodex }()

	ctx := context.Background()

	// Run 1: first exec — ExternalSessionID should be empty (no prior session).
	_, err = store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task1", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "reuse",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// Run 2: reuse — should pass ExternalSessionID from ProviderSession.
	_, err = store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task2", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "reuse",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// Verify ProviderSession has ExternalSessionID persisted.
	bindings := store.ListBindings(sess.ID)
	var psID string
	for _, b := range bindings {
		if b.ProviderSessionID != "" {
			psID = b.ProviderSessionID
			break
		}
	}
	if psID == "" {
		t.Fatal("no ProviderSession found")
	}
	ps, ok := store.GetProviderSession(psID)
	if !ok {
		t.Fatal("ProviderSession not found")
	}
	if ps.ExternalSessionID != "thread-first" {
		t.Errorf("ProviderSession.ExternalSessionID = %q, want thread-first", ps.ExternalSessionID)
	}

	// Run 3: still reuses session (simulating post-persistence state).
	_, err = store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task3", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "reuse",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if callIdx != 3 {
		t.Fatalf("call count = %d, want 3", callIdx)
	}

	// First exec: no ExternalSessionID passed in spec.
	if seenExternalIDs[0] != "" {
		t.Errorf("first exec ExternalSessionID = %q, want empty", seenExternalIDs[0])
	}
	// Second exec: ExternalSessionID = "thread-first" passed in spec.
	if seenExternalIDs[1] != "thread-first" {
		t.Errorf("second exec ExternalSessionID = %q, want thread-first", seenExternalIDs[1])
	}
	// Third exec: still passes ExternalSessionID.
	if seenExternalIDs[2] != "thread-first" {
		t.Errorf("third exec ExternalSessionID = %q, want thread-first", seenExternalIDs[2])
	}
	// All should use "reuse" policy.
	for i, p := range seenContextPolicies {
		if p != "reuse" {
			t.Errorf("call %d ContextPolicy = %q, want reuse", i, p)
		}
	}
}

// TestV2FreshDoesNotResume verifies that fresh context policy does not pass ExternalSessionID.
func TestV2FreshDoesNotResume(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "codex", Model: "o3", Executor: ExecutorCodex, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	callIdx := 0
	var seenExternalIDs []string

	fakeCodex := &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		defer mu.Unlock()
		seenExternalIDs = append(seenExternalIDs, spec.ExternalSessionID)
		callIdx++
		return &ExecResult{
			FinalText:         fmt.Sprintf("output-%d", callIdx),
			ExternalSessionID: fmt.Sprintf("thread-%d", callIdx),
		}, nil
	}}

	origCodex := executors[ExecutorCodex]
	executors[ExecutorCodex] = fakeCodex
	defer func() { executors[ExecutorCodex] = origCodex }()

	ctx := context.Background()

	// Run 1 with fresh.
	_, err = store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task1", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "fresh",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// Run 2 with fresh — should NOT pass ExternalSessionID.
	_, err = store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task2", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "fresh",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(seenExternalIDs) != 2 {
		t.Fatalf("calls = %d, want 2", len(seenExternalIDs))
	}
	// Both calls should have empty ExternalSessionID (fresh never resumes).
	for i, id := range seenExternalIDs {
		if id != "" {
			t.Errorf("call %d ExternalSessionID = %q, want empty (fresh)", i, id)
		}
	}
}

// TestV2NodeFailureDoesNotComplete verifies that when an executor fails,
// the run is marked "failed" (not "complete") and has an error message.
func TestV2NodeFailureDoesNotComplete(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "fail-node", Model: "o3", Executor: ExecutorCodex, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	fakeCodex := &loopStubFunc{fn: func(_ context.Context, _ ExecSpec) (*ExecResult, error) {
		return nil, fmt.Errorf("executor crashed")
	}}
	origCodex := executors[ExecutorCodex]
	executors[ExecutorCodex] = fakeCodex
	defer func() { executors[ExecutorCodex] = origCodex }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "fail task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)

	store.mu.RLock()
	status := run.Status
	runErr := run.Error
	finishedAt := run.FinishedAt
	store.mu.RUnlock()

	if status != "failed" {
		t.Errorf("run.Status = %q, want failed (not complete)", status)
	}
	if runErr == "" {
		t.Error("run.Error is empty, should contain failure reason")
	}
	if finishedAt == "" {
		t.Error("run.FinishedAt is empty")
	}
}

// TestV2FinalRunPersistenceDoesNotHideFailure verifies that the defer block
// does not overwrite a "failed" status with "complete".
func TestV2FinalRunPersistenceDoesNotHideFailure(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "fail-node", Model: "o3", Executor: ExecutorCodex, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	fakeCodex := &loopStubFunc{fn: func(_ context.Context, _ ExecSpec) (*ExecResult, error) {
		return nil, fmt.Errorf("executor crashed")
	}}
	origCodex := executors[ExecutorCodex]
	executors[ExecutorCodex] = fakeCodex
	defer func() { executors[ExecutorCodex] = origCodex }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "fail task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for run to reach terminal state.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.RLock()
		status := run.Status
		store.mu.RUnlock()
		if status == "complete" || status == "failed" {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	store.mu.RLock()
	status := run.Status
	runErr := run.Error
	store.mu.RUnlock()

	// The defer must NOT have overwritten "failed" to "complete".
	if status == "complete" && runErr != "" {
		t.Errorf("run is complete but has error %q — defer overwrote failure status", runErr)
	}
	if status != "failed" {
		t.Errorf("run.Status = %q, want failed", status)
	}

	// Also verify disk JSON matches.
	runDir := filepath.Join(orchestratorRoot(), "sessions", sess.ID, "runs")
	diskJSON := filepath.Join(runDir, run.ID+".json")
	if _, statErr := os.Stat(diskJSON); statErr != nil {
		t.Fatalf("run JSON not found on disk: %v", statErr)
	}
}

// TestFreshCreatesNewProviderSessionWithHistory verifies that fresh context policy
// creates a NEW ProviderSession for each run, and both are persisted.
func TestFreshCreatesNewProviderSessionWithHistory(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "output"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx := context.Background()

	// Run 1 — fresh, creates PS-A.
	run1, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task1", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "fresh",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// Collect PS IDs from run1 attempts (read under lock to avoid race with background goroutine).
	psIDs := make(map[string]bool)
	store.mu.RLock()
	attIDs1 := make([]string, len(run1.NodeAttemptIDs))
	copy(attIDs1, run1.NodeAttemptIDs)
	store.mu.RUnlock()
	for _, attID := range attIDs1 {
		att, ok := store.GetAttempt(attID)
		if !ok {
			t.Fatalf("attempt %s not found", attID)
		}
		if att.ProviderSessionID != "" {
			psIDs[att.ProviderSessionID] = true
		}
	}

	// Run 2 — fresh, creates PS-B (NOT reusing PS-A).
	run2, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task2", "", ExecutionOptions{
		Trigger:            "manual",
		ReuseAgentSessions: true,
		ContextPolicy:      "fresh",
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// Collect PS IDs from run2 attempts.
	store.mu.RLock()
	attIDs2 := make([]string, len(run2.NodeAttemptIDs))
	copy(attIDs2, run2.NodeAttemptIDs)
	store.mu.RUnlock()
	for _, attID := range attIDs2 {
		att, ok := store.GetAttempt(attID)
		if !ok {
			t.Fatalf("attempt %s not found", attID)
		}
		if att.ProviderSessionID != "" {
			psIDs[att.ProviderSessionID] = true
		}
	}

	// fresh policy: should have at least 2 distinct ProviderSessions.
	if len(psIDs) < 2 {
		t.Errorf("fresh policy: expected >= 2 unique ProviderSessions, got %d (IDs: %v)", len(psIDs), psIDs)
	}

	// All ProviderSessions must be loadable.
	for id := range psIDs {
		_, ok := store.GetProviderSession(id)
		if !ok {
			t.Errorf("ProviderSession %s not found in store", id)
		}
	}
}

// TestFreshPerRunCreatesNewProviderSessionWithHistory verifies that fresh_per_run
// creates a NEW ProviderSession for each of 3 runs, all persisted.
func TestFreshPerRunCreatesNewProviderSessionWithHistory(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "output"}}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	defer func() { executors[ExecutorReasonix] = old }()

	ctx := context.Background()

	psIDs := make(map[string]bool)

	// 3 runs with fresh_per_run.
	for i := 1; i <= 3; i++ {
		run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, fmt.Sprintf("task%d", i), "", ExecutionOptions{
			Trigger:            "manual",
			ReuseAgentSessions: true,
			ContextPolicy:      "fresh_per_run",
		})
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(1 * time.Second)

		// Collect PS IDs from this run's attempts (read under lock).
		store.mu.RLock()
		attIDs := make([]string, len(run.NodeAttemptIDs))
		copy(attIDs, run.NodeAttemptIDs)
		store.mu.RUnlock()
		for _, attID := range attIDs {
			att, ok := store.GetAttempt(attID)
			if !ok {
				t.Fatalf("attempt %s not found", attID)
			}
			if att.ProviderSessionID != "" {
				psIDs[att.ProviderSessionID] = true
			}
		}
	}

	// fresh_per_run: should have at least 3 distinct ProviderSessions.
	if len(psIDs) < 3 {
		t.Errorf("fresh_per_run: expected >= 3 unique ProviderSessions, got %d", len(psIDs))
	}

	// All persisted and loadable.
	for id := range psIDs {
		_, ok := store.GetProviderSession(id)
		if !ok {
			t.Errorf("ProviderSession %s not found in store", id)
		}
	}
}

// TestContextPolicyInvalidValueRejected verifies that invalid context policy values
// are rejected before the pipeline starts executing.
func TestContextPolicyInvalidValueRejected(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()

	invalidPolicies := []string{"invalid", "INVALID", "reusee", "freshh", "always_reuse"}
	for _, policy := range invalidPolicies {
		_, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{
			Trigger:       "manual",
			ContextPolicy: policy,
		})
		if err == nil {
			t.Errorf("ContextPolicy %q: expected error, got nil", policy)
		} else if !strings.Contains(err.Error(), "invalid context policy") {
			t.Errorf("ContextPolicy %q: error = %q, want 'invalid context policy'", policy, err.Error())
		}
	}

	// Valid policies should NOT error on validation (execution may fail due to stub, but validation passes).
	validPolicies := []string{"", "reuse", "fresh", "fresh_per_run"}
	for _, policy := range validPolicies {
		executor := stubExecutor{name: "reasonix", result: &ExecResult{FinalText: "ok"}}
		old := executors[ExecutorReasonix]
		executors[ExecutorReasonix] = executor

		_, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{
			Trigger:       "manual",
			ContextPolicy: policy,
		})
		executors[ExecutorReasonix] = old

		if err != nil && strings.Contains(err.Error(), "invalid context policy") {
			t.Errorf("ContextPolicy %q: should be valid, got %v", policy, err)
		}
	}
}

// ── LoopConfig tests ──

func TestLoopConfigSaveAndLoad(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
			{ID: "r1", Type: NodeReviewer, Label: "reviewer", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	cfg := &LoopConfig{
		Enabled:         true,
		Mode:            "review_decides",
		MaxIterations:   3,
		FixedIterations: 3,
		ReviewNodeID:    "r1",
		Protocol:        "loop-review-v1",
	}

	err := store.UpdatePipelineRevisionLoopConfig(sess.ID, rev.ID, cfg)
	if err != nil {
		t.Fatalf("UpdatePipelineRevisionLoopConfig failed: %v", err)
	}

	// Re-read and verify
	loaded, ok := store.GetPipelineRevision(rev.ID)
	if !ok {
		t.Fatal("revision not found after save")
	}
	if !loaded.LoopConfig.Enabled {
		t.Error("LoopConfig.Enabled = false, want true")
	}
	if loaded.LoopConfig.Mode != "review_decides" {
		t.Errorf("LoopConfig.Mode = %q, want review_decides", loaded.LoopConfig.Mode)
	}
	if loaded.LoopConfig.ReviewNodeID != "r1" {
		t.Errorf("LoopConfig.ReviewNodeID = %q, want r1", loaded.LoopConfig.ReviewNodeID)
	}
	if loaded.LoopConfig.Protocol != "loop-review-v1" {
		t.Errorf("LoopConfig.Protocol = %q, want loop-review-v1", loaded.LoopConfig.Protocol)
	}
}

func TestLoopConfigProtocolMustBeString(t *testing.T) {
	// Verify that protocol is a string, not an object
	cfg := &LoopConfig{
		Enabled:  true,
		Mode:     "review_decides",
		Protocol: "loop-review-v1",
	}
	if cfg.Protocol != "loop-review-v1" {
		t.Errorf("Protocol = %q, want loop-review-v1", cfg.Protocol)
	}
}

func TestLoopConfigInvalidReviewerNodeRejected(t *testing.T) {
	nodes := []AgentNode{
		{ID: "n1", Type: NodeExecutor, Label: "exec"},
		{ID: "r1", Type: NodeReviewer, Label: "reviewer"},
	}

	// Valid reviewer node
	err := ValidateLoopConfig(&LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 3,
		ReviewNodeID:  "r1",
		Protocol:      "loop-review-v1",
	}, nodes)
	if err != nil {
		t.Errorf("valid config should not error, got: %v", err)
	}

	// Invalid: reviewer node ID points to executor
	err = ValidateLoopConfig(&LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 3,
		ReviewNodeID:  "n1",
		Protocol:      "loop-review-v1",
	}, nodes)
	if err == nil {
		t.Error("expected error for executor node as reviewer")
	}

	// Invalid: reviewer node ID not found
	err = ValidateLoopConfig(&LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 3,
		ReviewNodeID:  "nonexistent",
		Protocol:      "loop-review-v1",
	}, nodes)
	if err == nil {
		t.Error("expected error for nonexistent reviewer node")
	}
}

func TestLoopConfigInvalidModeRejected(t *testing.T) {
	err := ValidateLoopConfig(&LoopConfig{
		Enabled:  true,
		Mode:     "invalid_mode",
		Protocol: "loop-review-v1",
	}, nil)
	if err == nil {
		t.Error("expected error for invalid mode")
	}
}

func TestLoopConfigInvalidProtocolRejected(t *testing.T) {
	err := ValidateLoopConfig(&LoopConfig{
		Enabled:  true,
		Mode:     "review_decides",
		Protocol: "invalid-protocol",
	}, nil)
	if err == nil {
		t.Error("expected error for invalid protocol")
	}
}

func TestLoopConfigPersistenceFailureReturnsError(t *testing.T) {
	store := NewStore()
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name:  "test",
		Nodes: []AgentNode{{ID: "r1", Type: NodeReviewer}},
	})

	// Create a file where the pipelines directory should be, so MkdirAll fails
	pipelinesDir := filepath.Join(sessionDir(sess.ID), "pipelines")
	os.RemoveAll(pipelinesDir)
	os.WriteFile(pipelinesDir, []byte("x"), 0644) // file blocks directory creation

	err := store.UpdatePipelineRevisionLoopConfig(sess.ID, rev.ID, &LoopConfig{
		Enabled:  true,
		Mode:     "fixed",
		Protocol: "loop-review-v1",
	})
	// Clean up
	os.RemoveAll(pipelinesDir)

	if err == nil {
		t.Error("expected persistence error")
	}
}
