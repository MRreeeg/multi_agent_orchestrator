package orchestrator

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

// ── Stage 0: switch ────────────────────────────────────────────────────────

func TestStallMaintenanceEnabledEnvOff(t *testing.T) {
	t.Setenv("REASONIX_STALL_MAINTENANCE", "off")
	if stallMaintenanceEnabled(nil) {
		t.Fatal("env off: want disabled")
	}
	for _, v := range []string{"false", "0", "disable", "disabled", "OFF", "FALSE"} {
		t.Setenv("REASONIX_STALL_MAINTENANCE", v)
		if stallMaintenanceEnabled(nil) {
			t.Fatalf("env %q: want disabled", v)
		}
	}
	t.Setenv("REASONIX_STALL_MAINTENANCE", "on")
	if !stallMaintenanceEnabled(nil) {
		t.Fatal("env on: want enabled")
	}
	t.Setenv("REASONIX_STALL_MAINTENANCE", "")
	if !stallMaintenanceEnabled(nil) {
		t.Fatal("empty env: want enabled (default)")
	}
}

func TestStallMaintenanceEnabledNodeSwitch(t *testing.T) {
	on := true
	off := false
	if !stallMaintenanceEnabled(&on) {
		t.Fatal("switch on: want enabled")
	}
	if stallMaintenanceEnabled(&off) {
		t.Fatal("switch off: want disabled")
	}
	if !stallMaintenanceEnabled(nil) {
		t.Fatal("switch nil: want enabled (default)")
	}
}

// ── Stage 2: maintenance-plan-v1 parsing ───────────────────────────────────

func TestParseMaintenancePlanValid(t *testing.T) {
	output := `先看一下输出。` + "\n" +
		`{"schemaVersion":"maintenance-plan-v1","judgment":"nudge","reason":"空转","nudge":{"message":"直接写代码，不要解释"}}`
	plan, err := ParseMaintenancePlan(output)
	if err != nil {
		t.Fatalf("ParseMaintenancePlan: %v", err)
	}
	if plan.SchemaVersion != "maintenance-plan-v1" || plan.Judgment != "nudge" || plan.Nudge == nil || !strings.Contains(plan.Nudge.Message, "直接写代码") {
		t.Fatalf("unexpected plan: %+v", plan)
	}
	if err := ValidateMaintenancePlan(plan); err != nil {
		t.Fatalf("ValidateMaintenancePlan: %v", err)
	}
}

func TestParseMaintenancePlanDirectAndInvalid(t *testing.T) {
	direct := `{"schemaVersion":"maintenance-plan-v1","judgment":"restart","reason":"死锁","restart":{"correction":"去掉步骤2重试"}}`
	plan, err := ParseMaintenancePlan(direct)
	if err != nil || plan.Judgment != "restart" || plan.Restart == nil {
		t.Fatalf("direct parse: plan=%+v err=%v", plan, err)
	}
	if _, err := ParseMaintenancePlan("只是聊天的回复，没有 JSON"); err == nil {
		t.Fatal("want error for non-JSON output")
	}
	if _, err := ParseMaintenancePlan(""); err == nil {
		t.Fatal("want error for empty output")
	}
}

func TestValidateMaintenancePlanRejectsBadPlans(t *testing.T) {
	cases := []MaintenancePlan{
		{SchemaVersion: "loop-review-v1", Judgment: "nudge", Reason: "r", Nudge: &struct {
			Message string `json:"message"`
		}{Message: "x"}},
		{SchemaVersion: "maintenance-plan-v1", Judgment: "skip", Reason: "r"},
		{SchemaVersion: "maintenance-plan-v1", Judgment: "nudge", Reason: "r"},
		{SchemaVersion: "maintenance-plan-v1", Judgment: "nudge", Reason: "r", Nudge: &struct {
			Message string `json:"message"`
		}{Message: ""}},
		{SchemaVersion: "maintenance-plan-v1", Judgment: "restart", Reason: "r"},
		{SchemaVersion: "maintenance-plan-v1", Judgment: "restart", Reason: "r", Restart: &struct {
			Correction string `json:"correction"`
		}{Correction: ""}},
		{SchemaVersion: "maintenance-plan-v1", Judgment: "noop", Reason: "  "},
	}
	for i, c := range cases {
		if err := ValidateMaintenancePlan(c); err == nil {
			t.Fatalf("case %d: want validation error", i)
		}
	}
	ok := MaintenancePlan{SchemaVersion: "maintenance-plan-v1", Judgment: "noop", Reason: "just slow"}
	if err := ValidateMaintenancePlan(ok); err != nil {
		t.Fatalf("noop plan: %v", err)
	}
}

// ── Stages 3+4: planner + repair loop ──────────────────────────────────────

// blockingExecutor returns ctx.Err() after ctx is cancelled, with partial
// output; every call after the first returns the given output immediately.
type blockingExecutor struct {
	name       string
	partial    string
	mu         sync.Mutex
	blockCalls int
}

func (e *blockingExecutor) Name() string { return e.name }

func (e *blockingExecutor) Execute(ctx context.Context, _ ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if onStart != nil {
		onStart("http://127.0.0.1:9999", 9999)
	}
	e.mu.Lock()
	e.blockCalls++
	call := e.blockCalls
	e.mu.Unlock()
	if call > 1 {
		return &ExecResult{FinalText: "corrective output", RawStderr: "", RuntimeID: "rt-1"}, nil
	}
	<-ctx.Done()
	return &ExecResult{FinalText: e.partial, RawStderr: "", RuntimeID: "rt-1"}, ctx.Err()
}

// plannerExecutor answers maintenance calls from a queue of plan JSONs.
type plannerExecutor struct {
	name  string
	plans []string
	mu    sync.Mutex
}

func (p *plannerExecutor) Name() string { return p.name }

func (p *plannerExecutor) Execute(_ context.Context, spec ExecSpec, _ func(string, int)) (*ExecResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out string
	if len(p.plans) > 0 {
		out = p.plans[0]
		if len(p.plans) > 1 {
			p.plans = p.plans[1:]
		}
	}
	return &ExecResult{FinalText: out}, nil
}

// dualExecutor answers execution prompts through the blocking executor and
// maintenance prompts through the planner executor.
type dualExecutor struct {
	block *blockingExecutor
	plan  *plannerExecutor
}

func (d *dualExecutor) Name() string { return "mimo" }

func (d *dualExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if strings.Contains(spec.Prompt, "维护模式") {
		return d.plan.Execute(ctx, spec, onStart)
	}
	return d.block.Execute(ctx, spec, onStart)
}

func mustPipe(t *testing.T, executorName string) *Pipeline {
	t.Helper()
	return &Pipeline{
		ID: "p1",
		Nodes: []AgentNode{
			{ID: "exec1", Type: NodeExecutor, Label: "执行者", Executor: ExecutorType(executorName), Model: "mimo-v2.5", Mode: "serve"},
			{ID: "rev1", Type: NodeReviewer, Label: "审查者", Executor: ExecutorMimo, Model: "mimo-v2.5", Mode: "serve"},
		},
	}
}

func mustLoopRun(id string) *PipelineRun {
	return &PipelineRun{
		ID:               id,
		SessionID:        "sess-1",
		CurrentIteration: 1,
		LoopConfig: LoopConfig{
			Enabled:       true,
			Mode:          "review_decides",
			MaxIterations: 3,
			ReviewNodeID:  "rev1",
			Protocol:      "loop-review-v1",
		},
	}
}

func mustStoreWithExecutors(t *testing.T, ex map[ExecutorType]PipelineExecutor) *Store {
	t.Helper()
	old := map[ExecutorType]PipelineExecutor{}
	for k, v := range ex {
		old[k] = executors[k]
		executors[k] = v
	}
	t.Cleanup(func() {
		for k := range ex {
			executors[k] = old[k]
		}
	})
	return NewStore()
}

// installStallHooks forces fast stall detection and simulates the runtime
// console; cleared on cleanup.
func installStallHooks(t *testing.T) {
	t.Helper()
	var snapMu sync.Mutex
	busy := false
	stallConsoleSnapshot = func(executor, runtimeID string) (*RuntimeConsoleSnapshot, bool) {
		snapMu.Lock()
		defer snapMu.Unlock()
		if !busy {
			busy = true
			return &RuntimeConsoleSnapshot{
				Runtime:      RuntimeState{RuntimeID: runtimeID, Status: RuntimeBusy},
				CanSend:      false,
				CanInterrupt: true,
				Output:       "partial output",
			}, true
		}
		busy = false
		return &RuntimeConsoleSnapshot{
			Runtime:      RuntimeState{RuntimeID: runtimeID, Status: RuntimeIdle},
			CanSend:      true,
			CanInterrupt: false,
			Output:       "partial output\ncorrective output",
		}, true
	}
	stallWatcherHook = func(w *stallWatcher) {
		w.interval = 5 * time.Millisecond
		w.threshold = 20 * time.Millisecond
	}
	t.Cleanup(func() {
		stallConsoleSnapshot = nil
		stallInterrupt = nil
		stallSendMessage = nil
		stallWatcherHook = nil
	})
}

func TestExecWithStallRepairFastPathWhenDisabled(t *testing.T) {
	off := false
	be := &blockingExecutor{name: "mimo", partial: "p"}
	st := mustStoreWithExecutors(t, map[ExecutorType]PipelineExecutor{ExecutorMimo: be})
	pipe := mustPipe(t, "mimo")
	pipe.Nodes[0].StallMaintenance = &off
	run := mustLoopRun("r1")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		time.Sleep(80 * time.Millisecond)
		cancel()
	}()
	out := st.execWithStallRepair(ctx, run, pipe, "s1", "i1", "exec1", &pipe.Nodes[0],
		"task input", "fresh", "", true, "b1", "ps1", 0)
	// The blocking executor hangs until ctx dies; with maintenance off it
	// must still return through the plain path.
	if out.err == nil {
		t.Fatal("want canceled error after cancel")
	}
	if !strings.Contains(out.output, "p") {
		t.Fatalf("fast path output = %q", out.output)
	}
}

func TestExecWithStallRepairNoopFallsBackToInterrupted(t *testing.T) {
	be := &blockingExecutor{name: "mimo", partial: "partial output"}
	pe := &plannerExecutor{name: "mimo", plans: []string{
		`{"schemaVersion":"maintenance-plan-v1","judgment":"noop","reason":"只是慢"}`,
	}}
	st := mustStoreWithExecutors(t, map[ExecutorType]PipelineExecutor{ExecutorMimo: &dualExecutor{block: be, plan: pe}})
	installStallHooks(t)
	pipe := mustPipe(t, "mimo")
	run := mustLoopRun("r1")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := st.execWithStallRepair(ctx, run, pipe, "s1", "i1", "exec1", &pipe.Nodes[0],
		"task input", "fresh", "", true, "b1", "ps1", 0)
	if out.err == nil {
		t.Fatal("want interrupted error (noop returns the interrupted result)")
	}
	if !strings.Contains(out.output, "partial output") {
		t.Fatalf("noop output = %q", out.output)
	}
	if len(run.MaintenanceEvents) != 1 || run.MaintenanceEvents[0].Action != "noop" || run.MaintenanceEvents[0].Outcome != "applied" {
		t.Fatalf("maintenance events = %+v", run.MaintenanceEvents)
	}
}

func TestExecWithStallRepairNudgePath(t *testing.T) {
	be := &blockingExecutor{name: "mimo", partial: "partial output"}
	pe := &plannerExecutor{name: "mimo", plans: []string{
		`{"schemaVersion":"maintenance-plan-v1","judgment":"nudge","reason":"空转","nudge":{"message":"直接写代码"}}`,
	}}
	st := mustStoreWithExecutors(t, map[ExecutorType]PipelineExecutor{ExecutorMimo: &dualExecutor{block: be, plan: pe}})

	var interruptMu sync.Mutex
	interrupted := []string{}
	stallInterrupt = func(executor, runtimeID string) {
		interruptMu.Lock()
		interrupted = append(interrupted, executor+":"+runtimeID)
		interruptMu.Unlock()
	}
	var msgMu sync.Mutex
	messages := []string{}
	stallSendMessage = func(executor, runtimeID, text string) (string, error) {
		msgMu.Lock()
		messages = append(messages, executor+":"+runtimeID+":"+text)
		msgMu.Unlock()
		return "turn-1", nil
	}
	installStallHooks(t)

	pipe := mustPipe(t, "mimo")
	run := mustLoopRun("r1")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := st.execWithStallRepair(ctx, run, pipe, "s1", "i1", "exec1", &pipe.Nodes[0],
		"task input", "fresh", "", true, "b1", "ps1", 0)
	if out.err != nil {
		t.Logf("events: %+v", run.MaintenanceEvents)
		t.Logf("msgs: %+v", messages)
		t.Logf("interrupted: %+v", interrupted)
		t.Fatalf("nudge path err: %v", out.err)
	}
	if !strings.Contains(out.output, "corrective output") {
		t.Fatalf("nudge output = %q, want corrective output", out.output)
	}
	msgMu.Lock()
	msgs := append([]string{}, messages...)
	msgMu.Unlock()
	if len(msgs) != 1 || !strings.Contains(msgs[0], "直接写代码") {
		t.Fatalf("messages = %v", msgs)
	}
	interruptMu.Lock()
	inter := append([]string{}, interrupted...)
	interruptMu.Unlock()
	if len(inter) != 1 {
		t.Fatalf("interrupts = %v", inter)
	}
	if len(run.MaintenanceEvents) != 1 || run.MaintenanceEvents[0].Action != "nudge" || run.MaintenanceEvents[0].Outcome != "applied" {
		t.Fatalf("maintenance events = %+v", run.MaintenanceEvents)
	}
}

func TestExecWithStallRepairRestartPath(t *testing.T) {
	be := &blockingExecutor{name: "mimo", partial: "partial output"}
	pe := &plannerExecutor{name: "mimo", plans: []string{
		`{"schemaVersion":"maintenance-plan-v1","judgment":"restart","reason":"死锁","restart":{"correction":"去掉步骤2重试"}}`,
	}}
	st := mustStoreWithExecutors(t, map[ExecutorType]PipelineExecutor{ExecutorMimo: &dualExecutor{block: be, plan: pe}})
	installStallHooks(t)

	pipe := mustPipe(t, "mimo")
	run := mustLoopRun("r1")
	st.runs[run.ID] = run
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := st.execWithStallRepair(ctx, run, pipe, "s1", "i1", "exec1", &pipe.Nodes[0],
		"task input", "fresh", "", true, "b1", "ps1", 0)
	if out.err != nil {
		t.Logf("events: %+v", run.MaintenanceEvents)
		t.Fatalf("restart path err: %v", out.err)
	}
	if !strings.Contains(out.output, "corrective output") {
		t.Fatalf("restart output = %q", out.output)
	}
	if out.attemptID == "" {
		t.Fatal("restart must return the new attempt id")
	}
	if len(run.MaintenanceEvents) != 1 || run.MaintenanceEvents[0].Action != "restart" {
		t.Fatalf("maintenance events = %+v", run.MaintenanceEvents)
	}
}

func TestExecWithStallRepairPlannerFailureDegrades(t *testing.T) {
	be := &blockingExecutor{name: "mimo", partial: "partial output"}
	pe := &plannerExecutor{name: "mimo", plans: []string{"garbage", "still garbage"}}
	st := mustStoreWithExecutors(t, map[ExecutorType]PipelineExecutor{ExecutorMimo: &dualExecutor{block: be, plan: pe}})
	installStallHooks(t)

	pipe := mustPipe(t, "mimo")
	run := mustLoopRun("r1")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := st.execWithStallRepair(ctx, run, pipe, "s1", "i1", "exec1", &pipe.Nodes[0],
		"task input", "fresh", "", true, "b1", "ps1", 0)
	if out.err == nil {
		t.Fatal("want interrupted error after planner failure")
	}
	if len(run.MaintenanceEvents) != 1 || run.MaintenanceEvents[0].Action != "planner" || run.MaintenanceEvents[0].Outcome != "failed" {
		t.Fatalf("maintenance events = %+v", run.MaintenanceEvents)
	}
}

func TestExecWithStallRepairBudgetExhausted(t *testing.T) {
	be := &blockingExecutor{name: "mimo", partial: "partial output"}
	pe := &plannerExecutor{name: "mimo", plans: []string{
		`{"schemaVersion":"maintenance-plan-v1","judgment":"restart","reason":"死锁","restart":{"correction":"纠正1"}}`,
		`{"schemaVersion":"maintenance-plan-v1","judgment":"restart","reason":"死锁2","restart":{"correction":"纠正2"}}`,
	}}
	st := mustStoreWithExecutors(t, map[ExecutorType]PipelineExecutor{ExecutorMimo: &dualExecutor{block: be, plan: pe}})
	installStallHooks(t)

	pipe := mustPipe(t, "mimo")
	run := mustLoopRun("r1")
	st.runs[run.ID] = run
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	out := st.execWithStallRepair(ctx, run, pipe, "s1", "i1", "exec1", &pipe.Nodes[0],
		"task input", "fresh", "", true, "b1", "ps1", 0)
	if out.err != nil {
		t.Fatalf("budget path err: %v", out.err)
	}
	if !strings.Contains(out.output, "corrective output") {
		t.Fatalf("budget output = %q", out.output)
	}
	if len(run.MaintenanceEvents) < 1 {
		t.Fatalf("maintenance events = %+v", run.MaintenanceEvents)
	}
}