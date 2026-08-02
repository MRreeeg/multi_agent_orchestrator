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

// TestMain isolates the package test suite from the developer's real
// orchestrator history. Several persistence tests reload a fresh Store and
// must never accidentally read production session files.
func TestMain(m *testing.M) {
	root, err := os.MkdirTemp("", "reasonix-orchestrator-tests-")
	if err != nil {
		panic(err)
	}
	_ = os.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)
	code := m.Run()
	_ = os.RemoveAll(root)
	os.Exit(code)
}

// loopStubExecutor is a fake executor that returns configurable output.
type loopStubExecutor struct {
	mu      sync.Mutex
	outputs []string // each call returns the next output
	callN   int
}

func (e *loopStubExecutor) Name() string { return "loop-stub" }

func (e *loopStubExecutor) Execute(_ context.Context, _ ExecSpec, _ func(string, int)) (*ExecResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.callN >= len(e.outputs) {
		return &ExecResult{FinalText: "default output"}, nil
	}
	out := e.outputs[e.callN]
	e.callN++
	return &ExecResult{FinalText: out}, nil
}

// newLoopTestStore creates a test store with a fake executor.
func newLoopTestStore(t *testing.T) *Store {
	t.Helper()
	// Keep all loop tests isolated from the user's real orchestrator history.
	// Tests that reload a store must see only the entities created by that test.
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	store := NewStore()
	executor := &loopStubExecutor{
		outputs: []string{"execution output"},
	}
	old := executors[ExecutorReasonix]
	executors[ExecutorReasonix] = executor
	t.Cleanup(func() { executors[ExecutorReasonix] = old })
	return store
}

// createTestSession creates a session and pipeline revision for testing.
func createTestSession(t *testing.T, store *Store, loopConfig LoopConfig) (*OrchestrationSession, *PipelineRevision) {
	t.Helper()
	sess, err := store.CreateOrchSession("test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test pipeline",
		Nodes: []AgentNode{
			{ID: "s1", Type: NodeExecutor, Label: "执行者", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
			{ID: "s2", Type: NodeReviewer, Label: "审查者", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges:      []Edge{{ID: "e1", FromID: "s1", ToID: "s2"}},
		LoopConfig: loopConfig,
	})
	if err != nil {
		t.Fatal(err)
	}
	return sess, rev
}

// TestLoopReusesArchitectOutputAfterFirstIteration verifies that loop revisions
// re-run the executor/reviewer work but do not invoke the architect again.
func TestLoopReusesArchitectOutputAfterFirstIteration(t *testing.T) {
	store := newLoopTestStore(t)
	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"architecture plan",
			"implementation 1",
			`{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.9,"summary":"needs change","blockingIssues":[],"requiredChanges":["fix it"],"nextTask":"fix it","evidence":[]}`,
			"implementation 2",
			`{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`,
		},
	}
	sess, err := store.CreateOrchSession("architect reuse", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "architect reuse",
		Nodes: []AgentNode{
			{ID: "s1", Type: NodeArchitect, Label: "架构师", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
			{ID: "s2", Type: NodeExecutor, Label: "执行者", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
			{ID: "s3", Type: NodeReviewer, Label: "审查者", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges:      []Edge{{ID: "e1", FromID: "s1", ToID: "s2"}, {ID: "e2", FromID: "s2", ToID: "s3"}},
		LoopConfig: LoopConfig{Enabled: true, Mode: "review_decides", MaxIterations: 3, ReviewNodeID: "s3", Protocol: "loop-review-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.ExecutePipelineV2(context.Background(), sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, _ := store.GetRun(run.ID)
		if got.Status != "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, _ := store.GetRun(run.ID)
	if got.Status != "complete" || got.TerminationReason != "review_pass" {
		t.Fatalf("run = status %q reason %q error %q", got.Status, got.TerminationReason, got.Error)
	}
	var architectAttempts, executorAttempts int
	for _, id := range got.NodeAttemptIDs {
		att, ok := store.GetAttempt(id)
		if !ok {
			continue
		}
		switch att.NodeID {
		case "s1":
			architectAttempts++
		case "s2":
			executorAttempts++
		}
	}
	if architectAttempts != 1 {
		t.Fatalf("architect attempts = %d, want 1", architectAttempts)
	}
	if executorAttempts != 2 {
		t.Fatalf("executor attempts = %d, want 2", executorAttempts)
	}
}

// TestLoopFixedPassRunsExactIterations verifies fixed mode runs exactly N iterations.
func TestLoopFixedPassRunsExactIterations(t *testing.T) {
	store := newLoopTestStore(t)

	// Override executor to always return pass
	// 3 iterations × 2 nodes (s1 + s2) = 6 outputs needed
	// s2 outputs are the review JSONs
	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"exec 1", // iteration 1, s1
			`{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`, // iteration 1, s2
			"exec 2", // iteration 2, s1
			`{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`, // iteration 2, s2
			"exec 3", // iteration 3, s1
			`{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`, // iteration 3, s2
		},
	}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 3,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for run to complete
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// Verify
	r, _ := store.GetRun(run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete; error: %s", r.Status, r.Error)
	}
	if r.TerminationReason != "fixed_limit" {
		t.Errorf("TerminationReason = %q, want fixed_limit", r.TerminationReason)
	}
	if len(r.IterationIDs) != 3 {
		t.Errorf("iteration count = %d, want 3", len(r.IterationIDs))
	}
}

// TestLoopReviewDecidesPassStopsImmediately verifies review_decides + pass stops at first iteration.
func TestLoopReviewDecidesPassStopsImmediately(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"execution output",
			`{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.95,"summary":"完美","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["全部通过"]}`,
		},
	}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 5,
		ReviewNodeID:  "s2",
		Protocol:      "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	r, _ := store.GetRun(run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete; error: %s", r.Status, r.Error)
	}
	if r.TerminationReason != "review_pass" {
		t.Errorf("TerminationReason = %q, want review_pass", r.TerminationReason)
	}
	if len(r.IterationIDs) != 1 {
		t.Errorf("iteration count = %d, want 1 (pass stops immediately)", len(r.IterationIDs))
	}
}

// TestLoopReviewDecidesReviseThenPass verifies revise → pass stops at second iteration.
func TestLoopReviewDecidesReviseThenPass(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"output round 1",
			`{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.7,"summary":"需要修改","blockingIssues":[],"requiredChanges":["补充验证"],"nextTask":"补充文件写入验证","evidence":["缺少验证"]}`,
			"output round 2",
			`{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.95,"summary":"通过","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["验证完成"]}`,
		},
	}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 5,
		ReviewNodeID:  "s2",
		Protocol:      "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	r, _ := store.GetRun(run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete; error: %s", r.Status, r.Error)
	}
	if r.TerminationReason != "review_pass" {
		t.Errorf("TerminationReason = %q, want review_pass", r.TerminationReason)
	}
	if len(r.IterationIDs) != 2 {
		t.Errorf("iteration count = %d, want 2", len(r.IterationIDs))
	}

	// Verify second iteration had the revised task
	iters := store.ListIterations(run.ID)
	if len(iters) < 2 {
		t.Fatalf("iteration count = %d, want >= 2", len(iters))
	}
	if iters[0].Decision != "revise" {
		t.Errorf("iteration 1 decision = %q, want revise", iters[0].Decision)
	}
	if iters[1].Decision != "pass" {
		t.Errorf("iteration 2 decision = %q, want pass", iters[1].Decision)
	}
}

// TestLoopBlockedState verifies blocked stops the loop.
func TestLoopBlockedState(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"output",
			`{"schemaVersion":"loop-review-v1","decision":"blocked","confidence":0.9,"summary":"安全阻塞","blockingIssues":["权限不足"],"requiredChanges":[],"nextTask":"","evidence":["权限检查失败"]}`,
		},
	}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 5,
		ReviewNodeID:  "s2",
		Protocol:      "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	r, _ := store.GetRun(run.ID)
	if r.Status != "blocked" {
		t.Errorf("run.Status = %q, want blocked", r.Status)
	}
	if r.TerminationReason != "blocked" {
		t.Errorf("TerminationReason = %q, want blocked", r.TerminationReason)
	}
	if len(r.IterationIDs) != 1 {
		t.Errorf("iteration count = %d, want 1", len(r.IterationIDs))
	}

	// Verify iteration is blocked
	iters := store.ListIterations(run.ID)
	if len(iters) > 0 && iters[0].Status != IterationBlocked {
		t.Errorf("iteration status = %q, want %s", iters[0].Status, IterationBlocked)
	}
}

// TestLoopInvalidConfig verifies invalid config is rejected before execution.
func TestLoopInvalidConfig(t *testing.T) {
	store := newLoopTestStore(t)

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "s1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		LoopConfig: LoopConfig{
			Enabled:         true,
			Mode:            "fixed",
			FixedIterations: 0, // invalid
			ReviewNodeID:    "nonexistent",
			Protocol:        "loop-review-v1",
		},
	})

	ctx := context.Background()
	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	// Wait for run to complete with error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.RLock()
		status := run.Status
		store.mu.RUnlock()
		if status == "complete" || status == "failed" || status == "blocked" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	store.mu.RLock()
	status := run.Status
	errMsg := run.Error
	store.mu.RUnlock()

	if status != "failed" {
		t.Errorf("run.Status = %q, want failed (invalid config)", status)
	}
	if errMsg == "" {
		t.Error("expected error message for invalid config")
	}
}

// TestLoopFixedReviseRunsExactIterations verifies fixed + revise runs exactly N iterations.
func TestLoopFixedReviseRunsExactIterations(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"output 1",
			`{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.7,"summary":"需要修改","blockingIssues":[],"requiredChanges":["修改A"],"nextTask":"修改A","evidence":["未完成"]}`,
			"output 2",
			`{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.8,"summary":"还需修改","blockingIssues":[],"requiredChanges":["修改B"],"nextTask":"修改B","evidence":["仍需改进"]}`,
			"output 3",
			`{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.85,"summary":"基本完成","blockingIssues":[],"requiredChanges":["最后调整"],"nextTask":"最后调整","evidence":["接近完成"]}`,
		},
	}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 3,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	r, _ := store.GetRun(run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete; error: %s", r.Status, r.Error)
	}
	if r.TerminationReason != "fixed_limit" {
		t.Errorf("TerminationReason = %q, want fixed_limit", r.TerminationReason)
	}
	if len(r.IterationIDs) != 3 {
		t.Errorf("iteration count = %d, want 3", len(r.IterationIDs))
	}

	// Verify last iteration is completed_by_limit, not revising
	iters := store.ListIterations(run.ID)
	if len(iters) == 3 {
		last := iters[2]
		if last.Status != IterationCompletedByLimit {
			t.Errorf("last iteration status = %q, want %s", last.Status, IterationCompletedByLimit)
		}
	}
}

// TestLoopIterationInputIsolation verifies downstream reads from current iteration only.
func TestLoopIterationInputIsolation(t *testing.T) {
	store := newLoopTestStore(t)

	var mu sync.Mutex
	var inputs []string
	executors[ExecutorReasonix] = &loopStubFunc{
		fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
			mu.Lock()
			inputs = append(inputs, spec.Prompt)
			mu.Unlock()
			if strings.Contains(spec.Prompt, "审查") {
				return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`}, nil
			}
			return &ExecResult{FinalText: "output"}, nil
		},
	}
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		inputs = append(inputs, spec.Prompt)
		mu.Unlock()
		if strings.Contains(spec.Prompt, "审查") {
			return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`}, nil
		}
		return &ExecResult{FinalText: "output"}, nil
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 2,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, err := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		r, ok := store.GetRun(run.ID)
		if ok && (r.Status == "complete" || r.Status == "failed" || r.Status == "blocked") {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	r, _ := store.GetRun(run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete; error: %s", r.Status, r.Error)
	}

	// Verify inputs were isolated per iteration
	mu.Lock()
	inputCount := len(inputs)
	mu.Unlock()

	// Should have 2 iterations × 2 nodes (s1 + s2) = 4 inputs
	if inputCount < 4 {
		t.Errorf("total inputs = %d, want >= 4 (2 iterations × 2 nodes)", inputCount)
	}
}

// loopStubFunc wraps a function as a PipelineExecutor.
type loopStubFunc struct {
	fn func(context.Context, ExecSpec) (*ExecResult, error)
}

func (f *loopStubFunc) Name() string { return "loop-stub-func" }
func (f *loopStubFunc) Execute(ctx context.Context, spec ExecSpec, _ func(string, int)) (*ExecResult, error) {
	return f.fn(ctx, spec)
}

// waitRunTerminal waits for a run to reach a terminal state or times out.
func waitRunTerminal(t *testing.T, store *Store, runID string) PipelineRun {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		store.mu.RLock()
		r, ok := store.runs[runID]
		if ok {
			status := r.Status
			store.mu.RUnlock()
			if status == "complete" || status == "failed" || status == "blocked" || status == "canceled" || status == "interrupted" {
				store.mu.RLock()
				run := *r
				store.mu.RUnlock()
				return run
			}
		} else {
			store.mu.RUnlock()
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("run %s did not reach terminal state within 20s", runID)
	return PipelineRun{} // unreachable
}

// TestLoopContextCancellation verifies context cancel properly stops the loop.
func TestLoopContextCancellation(t *testing.T) {
	store := newLoopTestStore(t)

	// Slow executor to ensure cancel takes effect before loop completes
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(ctx context.Context, spec ExecSpec) (*ExecResult, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
			return &ExecResult{FinalText: "output"}, nil
		}
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 10,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	ctx, cancel := context.WithCancel(context.Background())
	run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})

	// Cancel immediately
	cancel()

	r := waitRunTerminal(t, store, run.ID)
	if r.Status != "canceled" {
		t.Errorf("run.Status = %q, want canceled", r.Status)
	}
}

// TestLoopReviewOutputIsolation verifies second round reads only current iteration output.
func TestLoopReviewOutputIsolation(t *testing.T) {
	store := newLoopTestStore(t)

	var mu sync.Mutex
	var prompts []string
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		prompts = append(prompts, spec.Prompt)
		callNum := len(prompts)
		mu.Unlock()

		if strings.Contains(spec.Prompt, "审查") {
			// Return different review for each iteration
			if callNum <= 2 {
				// First iteration review
				return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.7,"summary":"需要修改","blockingIssues":[],"requiredChanges":["修改A"],"nextTask":"修改A","evidence":["未完成"]}`}, nil
			}
			// Second iteration review
			return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.95,"summary":"通过","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["完成"]}`}, nil
		}
		// Return different output for each iteration
		if callNum <= 2 {
			return &ExecResult{FinalText: "ROUND_1_OUTPUT"}, nil
		}
		return &ExecResult{FinalText: "ROUND_2_OUTPUT"}, nil
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 2,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	r := waitRunTerminal(t, store, run.ID)

	if r.Status != "complete" {
		t.Errorf("run.Status = %q, want complete", r.Status)
	}

	// Verify isolation: second iteration should not contain ROUND_1_OUTPUT
	mu.Lock()
	defer mu.Unlock()

	// Find the second iteration's input (should be revision task, not original)
	if len(prompts) < 4 {
		t.Fatalf("expected at least 4 prompts, got %d", len(prompts))
	}

	// prompts[0] = iteration 1, s1 (should contain task)
	// prompts[1] = iteration 1, s2 (review)
	// prompts[2] = iteration 2, s1 (should contain revision task, not ROUND_1_OUTPUT)
	// prompts[3] = iteration 2, s2 (review)
	if strings.Contains(prompts[2], "ROUND_1_OUTPUT") {
		t.Error("second iteration s1 input contains ROUND_1_OUTPUT — cross-iteration pollution")
	}
	if !strings.Contains(prompts[2], "修改A") {
		t.Error("second iteration s1 input should contain revision task from first review")
	}
}

// TestLoopResumeStartsFromInterruptedIteration verifies resume starts from the interrupted iteration.
func TestLoopResumeStartsFromInterruptedIteration(t *testing.T) {
	store := newLoopTestStore(t)

	// First, run a normal 2-iteration loop to completion
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		if strings.Contains(spec.Prompt, "审查") {
			return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.7,"summary":"需要修改","blockingIssues":[],"requiredChanges":["修改A"],"nextTask":"修改A","evidence":["未完成"]}`}, nil
		}
		return &ExecResult{FinalText: "output"}, nil
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 3,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	run, _ := store.ExecutePipelineV2(ctx, sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	time.Sleep(2 * time.Second)

	// Cancel the original execution context to stop the loop goroutine
	cancel()

	// Wait for the run to reach a terminal state
	waitRunTerminal(t, store, run.ID)

	// Now set up normal executor for resume
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		if strings.Contains(spec.Prompt, "审查") {
			return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.95,"summary":"通过","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["完成"]}`}, nil
		}
		return &ExecResult{FinalText: "output"}, nil
	}}

	// Create a fake interrupted iteration and mark the run as interrupted
	iter := LoopIteration{
		ID:        "iter_interrupted_1",
		RunID:     run.ID,
		Number:    1,
		Status:    "interrupted",
		InputTask: "task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.CreateIteration(iter)

	// Set run to interrupted state (safe now — loop goroutine has stopped)
	store.mu.Lock()
	run.Status = "interrupted"
	run.Error = "interrupted by restart"
	run.CurrentIteration = 1
	run.IterationIDs = append(run.IterationIDs, iter.ID)
	store.mu.Unlock()

	// Resume
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()

	err := store.ResumeLoop(ctx2, run, rev, sess.ID)
	if err != nil {
		t.Fatalf("ResumeLoop failed: %v", err)
	}

	r := waitRunTerminal(t, store, run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status after resume = %q, want complete; error: %s", r.Status, r.Error)
	}
}

// TestGatherInputForIterationReviewerReferencesArchitectPlan verifies the
// reviewer receives a path reference to the architect's persisted design
// document (not the full text), so it can judge the whole plan instead of only
// the current-round code.
func TestGatherInputForIterationReviewerReferencesArchitectPlan(t *testing.T) {
	store := newLoopTestStore(t)
	ws := t.TempDir()
	planDir := filepath.Join(ws, ".reasonix", "plans")
	if err := os.MkdirAll(planDir, 0o755); err != nil {
		t.Fatal(err)
	}
	archPlan := "三轮总体计划：\n第一轮：基础功能\n第二轮：错误处理\n第三轮：最终验证"
	if err := os.WriteFile(filepath.Join(planDir, "arch.md"), []byte(archPlan), 0o644); err != nil {
		t.Fatal(err)
	}

	pipe := &Pipeline{ID: "p1", Name: "t", Nodes: []AgentNode{
		{ID: "arch", Type: NodeArchitect, Label: "架构师", Model: "m", Executor: ExecutorReasonix, Mode: "serve"},
		{ID: "exec", Type: NodeExecutor, Label: "执行者", Model: "m", Executor: ExecutorReasonix, Mode: "serve"},
		{ID: "rev", Type: NodeReviewer, Label: "审查者", Model: "m", Executor: ExecutorReasonix, Mode: "serve"},
	}, Edges: []Edge{{ID: "e1", FromID: "arch", ToID: "exec"}, {ID: "e2", FromID: "exec", ToID: "rev"}}}

	run := &PipelineRun{ID: "run1", SessionID: "s1", Task: "解析端口范围", ExecOptions: ExecutionOptions{Workspace: ws}, LoopConfig: LoopConfig{ReviewNodeID: "rev", Mode: "review_decides"}}
	iter := LoopIteration{ID: "iter1", RunID: run.ID, Number: 1, Status: "running", InputTask: "任务", StartedAt: time.Now().UTC().Format(time.RFC3339)}

	store.mu.Lock()
	store.runs[run.ID] = run
	store.iterations[iter.ID] = &iter
	run.IterationIDs = append(run.IterationIDs, iter.ID)
	run.NodeAttemptIDs = append(run.NodeAttemptIDs, "att_arch", "att_exec")
	store.attempts["att_arch"] = &NodeAttempt{ID: "att_arch", RunID: run.ID, NodeID: "arch", IterationID: "iter1", Status: "complete", Output: "完整架构设计：" + archPlan}
	store.attempts["att_exec"] = &NodeAttempt{ID: "att_exec", RunID: run.ID, NodeID: "exec", IterationID: "iter1", Status: "complete", Output: "func ParsePortRange(){...} 第一轮代码"}
	store.mu.Unlock()

	input := store.gatherInputForIteration(pipe, run, "iter1", "rev")
	if !strings.Contains(input, "架构师设计文档") {
		t.Fatalf("reviewer input missing architect plan section:\n%s", input)
	}
	if !strings.Contains(input, filepath.Join("plans", "arch.md")) {
		t.Fatalf("reviewer input missing plan path reference:\n%s", input)
	}
	if strings.Contains(input, "完整架构设计：") {
		t.Fatalf("reviewer input must not copy the architect full text:\n%s", input)
	}
	if !strings.Contains(input, "func ParsePortRange") {
		t.Fatalf("reviewer input must include the executor round output:\n%s", input)
	}
	if !strings.Contains(input, "必须 revise") {
		t.Fatalf("reviewer input should instruct plan-coverage decision:\n%s", input)
	}
}

func TestPersistArchitectPlanWritesDesignDoc(t *testing.T) {
	ws := t.TempDir()
	persistArchitectPlan(ws, "arch", "三轮计划内容")
	data, err := os.ReadFile(filepath.Join(ws, ".reasonix", "plans", "arch.md"))
	if err != nil || string(data) != "三轮计划内容" {
		t.Fatalf("plan file = %q, err=%v", string(data), err)
	}
	// Empty/whitespace output must not create a file.
	persistArchitectPlan(ws, "arch2", "   ")
	if _, err := os.Stat(filepath.Join(ws, ".reasonix", "plans", "arch2.md")); err == nil {
		t.Fatal("empty output should not persist a plan file")
	}
}

func TestReviewerProtocolPromptRequiresPlanAwareness(t *testing.T) {
	if !strings.Contains(loopReviewerProtocolPrompt, "总体计划") || !strings.Contains(loopReviewerProtocolPrompt, "必须 revise") {
		t.Fatalf("reviewer protocol prompt must require plan-coverage decisions:\n%s", loopReviewerProtocolPrompt)
	}
}

// TestResumeRejectsWrongRevision verifies ResumeLoop rejects mismatched revision.
func TestResumeRejectsWrongRevision(t *testing.T) {
	store := newLoopTestStore(t)

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 3,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	// Manually create an interrupted run (no pipeline execution needed)
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	run.Status = "interrupted"
	run.Error = "interrupted by restart"
	store.mu.Unlock()

	// Create a different revision
	wrongRev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "wrong pipeline",
		Nodes: []AgentNode{
			{ID: "x1", Type: NodeExecutor, Label: "wrong", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		LoopConfig: rev.LoopConfig,
	})

	ctx := context.Background()
	err = store.ResumeLoop(ctx, run, wrongRev, sess.ID)
	if err == nil {
		t.Fatal("expected error for wrong revision, got nil")
	}
	if !strings.Contains(err.Error(), "revision mismatch") {
		t.Errorf("error = %q, want 'revision mismatch'", err.Error())
	}
}

// TestResumeRejectsCompletedRun verifies ResumeLoop rejects non-interrupted runs.
func TestResumeRejectsCompletedRun(t *testing.T) {
	store := newLoopTestStore(t)

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 1,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	// Manually create a completed run (no pipeline execution needed)
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	store.mu.Lock()
	run.Status = "complete"
	run.TerminationReason = "fixed_limit"
	store.mu.Unlock()

	// Resume should fail — run is complete, not interrupted
	ctx := context.Background()
	err = store.ResumeLoop(ctx, run, rev, sess.ID)
	if err == nil {
		t.Fatal("expected error for completed run, got nil")
	}
	if !strings.Contains(err.Error(), "not interrupted") {
		t.Errorf("error = %q, want 'not interrupted'", err.Error())
	}
}

// TestResumeValidatesLoopConfig verifies ResumeLoop validates config on resume.
func TestResumeValidatesLoopConfig(t *testing.T) {
	store := newLoopTestStore(t)

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 3,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	// Manually create an interrupted run with an interrupted iteration
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}

	iter := LoopIteration{
		ID:        "iter_validatetest_1",
		RunID:     run.ID,
		Number:    1,
		Status:    "interrupted",
		InputTask: "task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	_ = store.CreateIteration(iter)

	store.mu.Lock()
	run.Status = "interrupted"
	run.Error = "interrupted by restart"
	run.IterationIDs = append(run.IterationIDs, iter.ID)
	store.mu.Unlock()

	// Build a revision that has correct ID/session but invalid config (no review node)
	invalidRev := &PipelineRevision{
		ID:        run.PipelineRevisionID,
		SessionID: sess.ID,
		Name:      "invalid config",
		Nodes: []AgentNode{
			{ID: "x1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		LoopConfig: LoopConfig{
			Enabled:      true,
			Mode:         "fixed",
			Protocol:     "loop-review-v1",
			ReviewNodeID: "s2", // doesn't exist in invalidRev's nodes
		},
	}

	ctx := context.Background()
	err = store.ResumeLoop(ctx, run, invalidRev, sess.ID)
	if err == nil {
		t.Fatal("expected error for invalid loop config, got nil")
	}
	if !strings.Contains(err.Error(), "review node") {
		t.Errorf("error = %q, want 'review node' error", err.Error())
	}
}

// TestLoadAllDataMarksRunningEntitiesInterrupted verifies that LoadAllData marks
// running runs/iterations/attempts as interrupted using real disk persistence.
func TestLoadAllDataMarksRunningEntitiesInterrupted(t *testing.T) {
	t.Setenv("USERPROFILE", t.TempDir())
	t.Setenv("HOME", t.TempDir())
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	store := NewStore()

	sess, err := store.CreateOrchSession("test-reload", "/tmp")
	if err != nil {
		t.Fatal(err)
	}

	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test pipeline",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Create a run
	run, err := store.CreateRun(sess.ID, rev.ID, "test task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}

	// Create an iteration
	iter := LoopIteration{
		ID:        "iter_test_1",
		RunID:     run.ID,
		Number:    1,
		Status:    IterationRunning,
		InputTask: "test task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := store.CreateIteration(iter); err != nil {
		t.Fatal(err)
	}

	// Create an attempt
	attempt, err := store.CreateAttemptWithIteration(run.ID, "n1", "bind_test", iter.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Verify they are running
	if run.Status != "running" {
		t.Fatalf("run.Status = %q, want running", run.Status)
	}
	gotIter, _ := store.GetIteration(iter.ID)
	if gotIter.Status != "running" {
		t.Fatalf("iter.Status = %q, want running", gotIter.Status)
	}
	gotAtt, _ := store.GetAttempt(attempt.ID)
	if gotAtt.Status != "running" {
		t.Fatalf("attempt.Status = %q, want running", gotAtt.Status)
	}

	// Create a NEW store and load from the same data
	store2 := NewStore()
	if err := store2.LoadAllData(); err != nil {
		t.Fatalf("LoadAllData failed: %v", err)
	}

	// Verify running entities are now interrupted
	r2, ok := store2.GetRun(run.ID)
	if !ok {
		t.Fatal("run not found after reload")
	}
	if r2.Status != "interrupted" {
		t.Errorf("run.Status after reload = %q, want interrupted", r2.Status)
	}

	iter2, ok := store2.GetIteration(iter.ID)
	if !ok {
		t.Fatal("iteration not found after reload")
	}
	if iter2.Status != "interrupted" {
		t.Errorf("iter.Status after reload = %q, want interrupted", iter2.Status)
	}

	att2, ok := store2.GetAttempt(attempt.ID)
	if !ok {
		t.Fatal("attempt not found after reload")
	}
	if att2.Status != "interrupted" {
		t.Errorf("attempt.Status after reload = %q, want interrupted", att2.Status)
	}
}

// TestResumeLoopAfterReload verifies the full restart-resume cycle with real persistence.
func TestResumeLoopAfterReload(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		if strings.Contains(spec.Prompt, "审查") {
			return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.95,"summary":"通过","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["完成"]}`}, nil
		}
		return &ExecResult{FinalText: "output from resume"}, nil
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled:         true,
		Mode:            "fixed",
		FixedIterations: 3,
		ReviewNodeID:    "s2",
		Protocol:        "loop-review-v1",
	})

	// Create an interrupted run with an interrupted iteration
	run, err := store.CreateRun(sess.ID, rev.ID, "resume task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}

	iter := LoopIteration{
		ID:        "iter_interruptedResume_1",
		RunID:     run.ID,
		Number:    1,
		Status:    "interrupted",
		InputTask: "resume task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	if err := store.CreateIteration(iter); err != nil {
		t.Fatal(err)
	}

	// Add iteration to run
	store.mu.Lock()
	run.IterationIDs = append(run.IterationIDs, iter.ID)
	run.Status = "interrupted"
	run.Error = "interrupted by restart"
	store.mu.Unlock()
	if err := store.persistRun(run, sess.ID); err != nil {
		t.Fatal(err)
	}

	// Create an old attempt for the interrupted iteration
	oldAttempt, err := store.CreateAttemptWithIteration(run.ID, "s2", "bind_test", iter.ID)
	if err != nil {
		t.Fatal(err)
	}

	// Resume
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = store.ResumeLoop(ctx, run, rev, sess.ID)
	if err != nil {
		t.Fatalf("ResumeLoop failed: %v", err)
	}

	r := waitRunTerminal(t, store, run.ID)
	if r.Status != "complete" {
		t.Errorf("run.Status after resume = %q, want complete; error: %s", r.Status, r.Error)
	}

	// Verify: old iteration was reused (same ID)
	iters := store.ListIterations(run.ID)
	foundOldIter := false
	for _, it := range iters {
		if it.ID == iter.ID {
			foundOldIter = true
			// Should be passed (since review decided pass)
			if it.Status != "passed" {
				t.Errorf("old iteration status = %q, want passed", it.Status)
			}
		}
	}
	if !foundOldIter {
		t.Error("old interrupted iteration not found after resume")
	}

	// Verify: old attempt still exists
	allAttempts := store.ListAttempts(run.ID)
	foundOldAtt := false
	for _, a := range allAttempts {
		if a.ID == oldAttempt.ID {
			foundOldAtt = true
		}
	}
	if !foundOldAtt {
		t.Error("old attempt not preserved after resume")
	}

	// Verify: run has both old and new attempts
	if len(allAttempts) < 2 {
		t.Errorf("expected >= 2 attempts after resume, got %d", len(allAttempts))
	}

	// Verify: old iteration was reused (iteration 1) + new iterations created
	// FixedIterations=3, resume from iter 1, so total iterations = 3
	if len(r.IterationIDs) != 3 {
		t.Errorf("expected 3 iterations after resume, got %d", len(r.IterationIDs))
	}

	// Verify: first iteration is the old reused one
	if r.IterationIDs[0] != iter.ID {
		t.Errorf("first iteration ID = %q, want reused %q", r.IterationIDs[0], iter.ID)
	}
}

// TestFailNodeReturnsPersistenceError verifies failNode returns error when persistRun fails.
func TestFailNodeReturnsPersistenceError(t *testing.T) {
	store := newLoopTestStore(t)

	// Use a session ID that creates a path under a non-writable location
	// On Windows, we can't easily make dirs read-only, so we use a path that
	// will fail MkdirAll (e.g., a file exists where the directory should be)
	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")

	// Create a file where the runs directory should be, preventing MkdirAll from working
	runDir := filepath.Join(sessionDir(sess.ID), "runs")
	os.RemoveAll(runDir)
	os.WriteFile(runDir, []byte("block"), 0644)
	defer os.RemoveAll(runDir)

	bizErr := fmt.Errorf("node execution failed")
	err := store.failNode(run, "n1", "exec", bizErr, sess.ID, "")

	if err == nil {
		t.Fatal("expected error from failNode when persist fails, got nil")
	}
	if !strings.Contains(err.Error(), "persist failed") {
		t.Errorf("error = %q, want 'persist failed'", err.Error())
	}
}

// TestExecuteNodeAttemptPropagatesPersistenceError verifies executeNodeAttempt propagates errors.
func TestExecuteNodeAttemptPropagatesPersistenceError(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		return &ExecResult{FinalText: "output"}, nil
	}}

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	iter := LoopIteration{
		ID:        "iter_persist_test_1",
		RunID:     run.ID,
		Number:    1,
		Status:    IterationRunning,
		InputTask: "task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	store.CreateIteration(iter)

	// Create a file where the runs directory should be, preventing MkdirAll
	runDir := filepath.Join(sessionDir(sess.ID), "runs")
	os.RemoveAll(runDir)
	os.WriteFile(runDir, []byte("block"), 0644)
	defer os.RemoveAll(runDir)

	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	err := store.executeNodeAttempt(context.Background(), run, pipe, sess.ID, iter.ID, "n1", rev.Nodes[0])
	if err == nil {
		t.Fatal("expected error from executeNodeAttempt when persist fails, got nil")
	}
	if !strings.Contains(err.Error(), "persist") {
		t.Errorf("error = %q, want 'persist' in error", err.Error())
	}
}

// TestExecutePipelineIterationAggregatesNodeError verifies parallel node errors are collected.
func TestExecutePipelineIterationAggregatesNodeError(t *testing.T) {
	store := newLoopTestStore(t)

	// Use an executor that returns an error (not empty output)
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		return nil, fmt.Errorf("node execution failed")
	}}

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")

	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	err := store.executePipelineIteration(context.Background(), run, pipe, sess.ID, "iter_test")
	if err == nil {
		t.Fatal("expected error from executePipelineIteration when node fails, got nil")
	}
	if !strings.Contains(err.Error(), "node execution") {
		t.Errorf("error = %q, want 'node execution' in error", err.Error())
	}
}

// TestExecuteLoopWithoutLoopPropagatesError verifies non-loop path propagates pipeline errors.
func TestExecuteLoopWithoutLoopPropagatesError(t *testing.T) {
	store := newLoopTestStore(t)

	// Use an executor that always fails
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		return nil, fmt.Errorf("pipeline node failed")
	}}

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{},
		LoopConfig: LoopConfig{
			Enabled: false, // non-loop path
		},
	})

	// Create run directly (not via ExecutePipelineV2) to avoid goroutine race
	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")

	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	err := store.ExecuteLoop(context.Background(), run, pipe, sess.ID)
	if err == nil {
		t.Fatal("expected error from ExecuteLoop when pipeline fails, got nil")
	}
	if !strings.Contains(err.Error(), "pipeline execution") {
		t.Errorf("error = %q, want 'pipeline execution' in error", err.Error())
	}
}

// TestMarkInterruptedPersistenceFailure verifies markInterrupted returns error on persistence failure.
func TestMarkInterruptedPersistenceFailure(t *testing.T) {
	store := NewStore()

	sess, _ := store.CreateOrchSession("test-persist-fail", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	iter := LoopIteration{
		ID:        "iter_persistfail_1",
		RunID:     run.ID,
		Number:    1,
		Status:    IterationRunning,
		InputTask: "task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	store.CreateIteration(iter)

	// Verify they are running
	if run.Status != "running" {
		t.Fatalf("run.Status = %q, want running", run.Status)
	}

	// Now block the runs directory so markInterrupted can't persist
	runDir := filepath.Join(sessionDir(sess.ID), "runs")
	os.RemoveAll(runDir)
	os.WriteFile(runDir, []byte("block"), 0644)
	defer os.RemoveAll(runDir)

	// Call markInterrupted directly — it should fail because persistRun can't write
	err := store.markInterrupted()
	if err == nil {
		t.Fatal("expected error from markInterrupted when persist fails, got nil")
	}
	if !strings.Contains(err.Error(), "mark run") {
		t.Errorf("error = %q, want 'mark run' in error", err.Error())
	}
}

// TestFailedNodeRuntimeIsMarkedError verifies RuntimeState is marked error when node fails.
func TestFailedNodeRuntimeIsMarkedError(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		return &ExecResult{
			FinalText: "",
			RuntimeID: "rt_test_1",
			Endpoint:  "http://localhost:8080",
		}, fmt.Errorf("provider timeout")
	}}

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	iter := LoopIteration{
		ID:        "iter_runtime_err_1",
		RunID:     run.ID,
		Number:    1,
		Status:    IterationRunning,
		InputTask: "task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	store.CreateIteration(iter)

	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	err := store.executeNodeAttempt(context.Background(), run, pipe, sess.ID, iter.ID, "n1", rev.Nodes[0])
	if err == nil {
		t.Fatal("expected error from executeNodeAttempt, got nil")
	}

	// Check RuntimeState
	rts := store.ListRuntimeStates(sess.ID)
	if len(rts) == 0 {
		t.Fatal("expected at least one RuntimeState")
	}
	rt := rts[0]
	if rt.Status != RuntimeError {
		t.Errorf("RuntimeState.Status = %q, want %q", rt.Status, RuntimeError)
	}
	if rt.Error == "" {
		t.Error("RuntimeState.Error should not be empty")
	}
}

// TestEmptyOutputRuntimeIsMarkedError verifies RuntimeState is marked error on empty output.
func TestEmptyOutputRuntimeIsMarkedError(t *testing.T) {
	store := newLoopTestStore(t)

	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		return &ExecResult{
			FinalText: "",
			RuntimeID: "rt_test_2",
			Endpoint:  "http://localhost:8081",
		}, nil // empty output, no exec error
	}}

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	iter := LoopIteration{
		ID:        "iter_runtime_empty_1",
		RunID:     run.ID,
		Number:    1,
		Status:    IterationRunning,
		InputTask: "task",
		StartedAt: time.Now().UTC().Format(time.RFC3339),
	}
	store.CreateIteration(iter)

	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	err := store.executeNodeAttempt(context.Background(), run, pipe, sess.ID, iter.ID, "n1", rev.Nodes[0])
	if err == nil {
		t.Fatal("expected error from executeNodeAttempt for empty output, got nil")
	}

	// Check RuntimeState
	rts := store.ListRuntimeStates(sess.ID)
	if len(rts) == 0 {
		t.Fatal("expected at least one RuntimeState")
	}
	rt := rts[0]
	if rt.Status != RuntimeError {
		t.Errorf("RuntimeState.Status = %q, want %q", rt.Status, RuntimeError)
	}
}

// TestExecuteNodeAttemptReturnsContextCanceled verifies context cancel returns error.
func TestExecuteNodeAttemptReturnsContextCanceled(t *testing.T) {
	store := newLoopTestStore(t)

	sess, _ := store.CreateOrchSession("test", "/tmp")
	rev, _ := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "test",
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "exec", Model: "deepseek-flash", Executor: ExecutorReasonix, Mode: "run"},
		},
		Edges: []Edge{},
	})

	run, _ := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")

	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	// Cancel context before calling executeNodeAttempt
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := store.executeNodeAttempt(ctx, run, pipe, sess.ID, "iter_test", "n1", rev.Nodes[0])
	if err == nil {
		t.Fatal("expected error from executeNodeAttempt with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestLoopForcesTaskExecutionMode(t *testing.T) {
	store := newLoopTestStore(t)
	var mu sync.Mutex
	var captured ExecSpec
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		captured = spec
		mu.Unlock()
		return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":1,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`}, nil
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled: true, Mode: "review_decides", MaxIterations: 1,
		ReviewNodeID: "s2", Protocol: "loop-review-v1",
	})
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	run.LoopConfig.Enabled = true
	run.LoopConfig.Mode = "review_decides"
	run.LoopConfig.MaxIterations = 1
	run.LoopConfig.ReviewNodeID = "s2"
	run.LoopConfig.Protocol = "loop-review-v1"
	iter := LoopIteration{ID: "iter_mode", RunID: run.ID, Number: 1, Status: "running", InputTask: "task", StartedAt: time.Now().UTC().Format(time.RFC3339)}
	if err := store.CreateIteration(iter); err != nil {
		t.Fatal(err)
	}
	pipe := &Pipeline{ID: rev.ID, Name: rev.Name, Nodes: rev.Nodes, Edges: rev.Edges}
	reviewer := rev.Nodes[1]
	reviewer.ExecutionMode = "goal"
	if err := store.executeNodeAttempt(context.Background(), run, pipe, sess.ID, iter.ID, "s2", reviewer); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	mode := captured.ExecutionMode
	mu.Unlock()
	if mode != "task" {
		t.Fatalf("Loop reviewer execution mode = %q, want task", mode)
	}
	if captured.Mode != "run" {
		t.Fatalf("Loop reviewer mode = %q, want one-shot run", captured.Mode)
	}
	if captured.MaxSteps != 3 {
		t.Fatalf("Loop reviewer max steps = %d, want 3", captured.MaxSteps)
	}
}

func TestLoopReviewerProtocolIsInjectedIntoReviewerPrompt(t *testing.T) {
	store := newLoopTestStore(t)
	var mu sync.Mutex
	var captured string
	executors[ExecutorReasonix] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		captured = spec.Prompt
		mu.Unlock()
		return &ExecResult{FinalText: `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":1,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`}, nil
	}}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled: true, Mode: "review_decides", MaxIterations: 1,
		ReviewNodeID: "s2", Protocol: "loop-review-v1",
	})
	run, err := store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	iter := LoopIteration{ID: "iter_prompt", RunID: run.ID, Number: 1, Status: "running", InputTask: "task", StartedAt: time.Now().UTC().Format(time.RFC3339)}
	if err := store.CreateIteration(iter); err != nil {
		t.Fatal(err)
	}
	pipe := &Pipeline{ID: rev.ID, Name: rev.Name, Nodes: rev.Nodes, Edges: rev.Edges}

	if err := store.executeNodeAttempt(context.Background(), run, pipe, sess.ID, iter.ID, "s2", rev.Nodes[1]); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	prompt := captured
	mu.Unlock()
	for _, want := range []string{"LOOP REVIEW PROTOCOL", "纯 JSON", "loop-review-v1", `"decision"`, "禁止修改"} {
		if !strings.Contains(prompt, want) {
			t.Errorf("reviewer prompt missing %q; prompt=%q", want, prompt)
		}
	}
}

// TestLoopReviewerToolOutputGetsOneProtocolCorrection verifies that a tool-call
// JSON accidentally exposed as the reviewer's final text does not immediately
// block the Loop. The reviewer gets one corrective turn on the same Loop path,
// and a valid decision can still complete the run.
func TestLoopReviewerToolOutputGetsOneProtocolCorrection(t *testing.T) {
	store := newLoopTestStore(t)
	validPass := `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"工具检查后通过","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["检查完成"]}`
	executors[ExecutorReasonix] = &loopStubExecutor{
		outputs: []string{
			"execution output",
			`{"command":"Get-ChildItem -Path C:\\work"}`,
			validPass,
		},
	}

	sess, rev := createTestSession(t, store, LoopConfig{
		Enabled: true, Mode: "review_decides", MaxIterations: 3,
		ReviewNodeID: "s2", Protocol: "loop-review-v1",
	})
	run, err := store.ExecutePipelineV2(context.Background(), sess.ID, rev.ID, "task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		got, ok := store.GetRun(run.ID)
		if ok && got.Status != "running" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	got, _ := store.GetRun(run.ID)
	if got.Status != "complete" {
		t.Fatalf("run.Status = %q, want complete; error: %s", got.Status, got.Error)
	}
	if got.TerminationReason != "review_pass" {
		t.Fatalf("TerminationReason = %q, want review_pass", got.TerminationReason)
	}
	iterations := store.ListIterations(run.ID)
	if len(iterations) != 1 {
		t.Fatalf("iteration count = %d, want 1", len(iterations))
	}
	if iterations[0].Decision != "pass" || iterations[0].ReviewAttemptID == "" {
		t.Fatalf("iteration review = %+v, want pass with review attempt", iterations[0])
	}
	att, ok := store.GetAttempt(iterations[0].ReviewAttemptID)
	if !ok {
		t.Fatalf("review attempt %q not found", iterations[0].ReviewAttemptID)
	}
	if _, err := ParseReviewDecision(att.Output); err != nil {
		t.Fatalf("final reviewer output is not valid review JSON: %v; output=%q", err, att.Output)
	}
}

// TestCodexServeReviewerReusesPersistedThreadAcrossLoopAttempts ensures the
// retained Codex App Server gets the same durable Thread for every reviewer
// iteration. Other reviewer executors may remain fresh, but a Codex serve
// reviewer must not create a new thread for every Loop iteration.
func TestCodexServeReviewerReusesPersistedThreadAcrossLoopAttempts(t *testing.T) {
	store := newLoopTestStore(t)
	var mu sync.Mutex
	var specs []ExecSpec
	old := executors[ExecutorCodex]
	executors[ExecutorCodex] = &loopStubFunc{fn: func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		mu.Lock()
		specs = append(specs, spec)
		mu.Unlock()
		return &ExecResult{
			FinalText:         `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`,
			ExternalSessionID: "codex-review-thread-1",
		}, nil
	}}
	t.Cleanup(func() { executors[ExecutorCodex] = old })

	sess, err := store.CreateOrchSession("codex serve reviewer thread reuse", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "codex serve reviewer",
		Nodes: []AgentNode{{
			ID: "review", Type: NodeReviewer, Label: "Codex 审查者",
			Model: "ccs", Executor: ExecutorCodex, Mode: "serve",
		}},
		LoopConfig: LoopConfig{Enabled: true, Mode: "fixed", FixedIterations: 2, ReviewNodeID: "review", Protocol: "loop-review-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.CreateRun(sess.ID, rev.ID, "review task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	pipe := &Pipeline{ID: rev.ID, Name: rev.Name, Nodes: rev.Nodes, Edges: rev.Edges}
	for number := 1; number <= 2; number++ {
		iter := LoopIteration{
			ID:        fmt.Sprintf("iter_codex_review_%d", number),
			RunID:     run.ID,
			Number:    number,
			Status:    IterationRunning,
			InputTask: "review task",
			StartedAt: time.Now().UTC().Format(time.RFC3339),
		}
		if err := store.CreateIteration(iter); err != nil {
			t.Fatal(err)
		}
		if err := store.executeNodeAttempt(context.Background(), run, pipe, sess.ID, iter.ID, "review", rev.Nodes[0]); err != nil {
			t.Fatalf("iteration %d executeNodeAttempt: %v", number, err)
		}
	}

	mu.Lock()
	got := append([]ExecSpec(nil), specs...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("Codex reviewer calls = %d, want 2", len(got))
	}
	if got[0].ExternalSessionID != "" {
		t.Fatalf("first reviewer ExternalSessionID = %q, want empty", got[0].ExternalSessionID)
	}
	if got[1].ExternalSessionID != "codex-review-thread-1" {
		t.Fatalf("second reviewer ExternalSessionID = %q, want persisted Codex thread", got[1].ExternalSessionID)
	}
	for i, spec := range got {
		if spec.ContextPolicy != "reuse" {
			t.Fatalf("reviewer call %d ContextPolicy = %q, want reuse", i+1, spec.ContextPolicy)
		}
	}
}

// TestCodexServeReviewerInvalidOutputDoesNotStartACorrectionTurn verifies the
// retained App Server contract: one orchestrator node attempt maps to exactly
// one Codex Turn. A malformed reviewer response is handled by the Loop parser,
// not by silently issuing a second provider turn.
func TestCodexServeReviewerInvalidOutputDoesNotStartACorrectionTurn(t *testing.T) {
	store := newLoopTestStore(t)
	var mu sync.Mutex
	calls := 0
	old := executors[ExecutorCodex]
	executors[ExecutorCodex] = &loopStubFunc{fn: func(_ context.Context, _ ExecSpec) (*ExecResult, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return &ExecResult{FinalText: `{"command":"Get-ChildItem"}`, ExternalSessionID: "codex-review-thread-1"}, nil
	}}
	t.Cleanup(func() { executors[ExecutorCodex] = old })

	sess, err := store.CreateOrchSession("codex one turn reviewer", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rev, err := store.CreatePipelineRevision(sess.ID, PipelineRevision{
		Name: "codex one turn reviewer",
		Nodes: []AgentNode{{
			ID: "review", Type: NodeReviewer, Label: "Codex 审查者",
			Model: "ccs", Executor: ExecutorCodex, Mode: "serve",
		}},
		LoopConfig: LoopConfig{Enabled: true, Mode: "fixed", FixedIterations: 1, ReviewNodeID: "review", Protocol: "loop-review-v1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := store.ExecutePipelineV2(context.Background(), sess.ID, rev.ID, "review task", "", ExecutionOptions{Trigger: "manual"})
	if err != nil {
		t.Fatal(err)
	}
	got := waitRunTerminal(t, store, run.ID)
	if got.Status != "blocked" || got.TerminationReason != "blocked" {
		t.Fatalf("run = status %q reason %q error %q, want blocked invalid review", got.Status, got.TerminationReason, got.Error)
	}
	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("Codex reviewer calls = %d, want exactly 1 retained Turn", gotCalls)
	}
}
