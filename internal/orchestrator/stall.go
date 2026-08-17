package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"reasonix/internal/event"
)

// Reviewer-driven stall repair: when a loop node makes no progress for a
// while (cheap/free models are the usual culprit), the orchestrator interrupts
// it, wakes the reviewer in "maintenance mode" to diagnose, and applies the
// returned maintenance-plan-v1 instruction: nudge (interrupt + corrective
// message, serve runtimes only), restart (kill + rerun with a correction), or
// noop (judged merely slow). Every layer degrades to the original timeout /
// manual-resume path when anything goes wrong.

const (
	// stallNoProgressTimeout is the no-progress threshold for executors with
	// an observable progress stream (serve-mode runtimes: mimo/opencode/
	// claude/codex).
	stallNoProgressTimeout = 90 * time.Second
	// stallTurnWarnTimeout is the fallback threshold for executors without a
	// progress stream (reasonix serve, run-mode subprocesses, dsh): the turn
	// itself has been running for this long.
	stallTurnWarnTimeout = 3 * time.Minute
	// stallPollInterval is how often the watcher polls the runtime console.
	stallPollInterval = 2 * time.Second
	// stallMaxInterventions caps repair attempts per node per iteration.
	stallMaxInterventions = 2
	// maintenanceTimeout bounds one reviewer maintenance-mode call.
	maintenanceTimeout = 3 * time.Minute
)

// maintenancePlannerMu serializes maintenance calls against the reviewer
// runtime: a serve runtime hosts one turn at a time, and the end-of-round
// review must never be blocked behind a stall diagnosis.
var maintenancePlannerMu sync.Mutex

// Test hooks. nil in production; stall_test.go swaps them in to simulate
// runtime console/transport behavior without spawning provider processes.
var (
	stallConsoleSnapshot func(executor, runtimeID string) (*RuntimeConsoleSnapshot, bool)
	stallInterrupt       func(executor, runtimeID string)
	stallSendMessage     func(executor, runtimeID, text string) (string, error)
	stallWatcherHook     func(w *stallWatcher)
)

// stallObservable reports whether the executor exposes a progress stream the
// watcher can poll (serve-mode runtimes with console snapshots).
func stallObservable(executor, mode string) bool {
	switch executor {
	case "mimo", "opencode", "claude", "codex":
		return strings.EqualFold(strings.TrimSpace(mode), "serve")
	}
	return false
}

// runtimeIDForEndpoint matches a runtime ID by endpoint from the executor's
// live runtime list. Used to bridge the executor onStart callback (endpoint
// only) to the runtime console API (ID based).
func runtimeIDForEndpoint(executor, endpoint string) string {
	var list []*RuntimeState
	switch executor {
	case "mimo":
		list = ListMimoRuntimes()
	case "opencode":
		list = ListOpencodeRuntimes()
	case "claude":
		list = ListClaudeRuntimes()
	case "codex":
		list = ListCodexRuntimes()
	}
	for _, rt := range list {
		if rt != nil && rt.Endpoint != "" && rt.Endpoint == endpoint {
			return rt.RuntimeID
		}
	}
	return ""
}

// stallWatcher observes progress of one node execution and reports when the
// configured no-progress threshold is crossed.
type stallWatcher struct {
	executor  string
	mode      string
	runtimeID string

	interval  time.Duration
	threshold time.Duration
	// progress returns true when the runtime made progress since the last poll.
	progress func() bool
	// lastPoll is the wall-clock moment of the last observed progress.
	lastPoll time.Time
	// outputSnapshot / eventsSnapshot preserve the last observed console
	// state for the maintenance scene.
	outputSnapshot string
	eventsSnapshot []RuntimeConsoleEvent
	lastOutputLen  int
	lastEventsLen  int
	stopped        bool
}

func newStallWatcher(executor, mode, runtimeID string) *stallWatcher {
	w := &stallWatcher{
		executor:  executor,
		mode:      mode,
		runtimeID: runtimeID,
		interval:  stallPollInterval,
		lastPoll:  time.Now(),
	}
	if stallObservable(executor, mode) && runtimeID != "" {
		w.threshold = stallNoProgressTimeout
		w.progress = w.pollConsole
	} else {
		// No progress stream: fall back to "the turn has been running this
		// long" semantics, which the caller implements via threshold-only
		// ticking (progress returns true after the first tick).
		w.threshold = stallTurnWarnTimeout
	}
	return w
}

func (w *stallWatcher) pollConsole() bool {
	if stallConsoleSnapshot != nil {
		snap, ok := stallConsoleSnapshot(w.executor, w.runtimeID)
		if !ok || snap == nil {
			return false
		}
		w.outputSnapshot = snap.Output
		w.eventsSnapshot = snap.Events
		changed := len(snap.Output) != w.lastOutputLen || len(snap.Events) != w.lastEventsLen
		w.lastOutputLen = len(snap.Output)
		w.lastEventsLen = len(snap.Events)
		return changed
	}
	var snap *RuntimeConsoleSnapshot
	var ok bool
	switch w.executor {
	case "mimo":
		snap, ok = GetMimoRuntimeConsole(w.runtimeID)
	case "opencode":
		snap, ok = SnapshotOpencodeRuntime(w.runtimeID)
	case "claude":
		snap, ok = GetClaudeRuntimeConsole(w.runtimeID)
	case "codex":
		snap, ok = GetCodexRuntimeConsole(w.runtimeID)
	}
	if !ok || snap == nil {
		return false
	}
	w.outputSnapshot = snap.Output
	w.eventsSnapshot = snap.Events
	// Progress = the console changed since the last poll.
	changed := len(snap.Output) != w.lastOutputLen || len(snap.Events) != w.lastEventsLen
	w.lastOutputLen = len(snap.Output)
	w.lastEventsLen = len(snap.Events)
	return changed
}

// stalled reports whether the threshold has been crossed without progress.
func (w *stallWatcher) stalled() bool {
	if w.progress == nil {
		// Turn-duration mode: the caller ticks this once per interval and
		// compares elapsed against the threshold directly.
		return false
	}
	if w.progress() {
		w.lastPoll = time.Now()
		return false
	}
	return time.Since(w.lastPoll) >= w.threshold
}

// stallDuration reports how long the watcher has been without progress
// (used by the turn-duration fallback path).
func (w *stallWatcher) stallDuration() time.Duration {
	if w.progress != nil && w.progress() {
		w.lastPoll = time.Now()
		return 0
	}
	return time.Since(w.lastPoll)
}

// interruptRuntime interrupts a busy serve runtime (T1 executors only).
func interruptRuntime(executor, runtimeID string) {
	if runtimeID == "" {
		return
	}
	if stallInterrupt != nil {
		stallInterrupt(executor, runtimeID)
		return
	}
	switch executor {
	case "mimo":
		_ = InterruptMimoRuntime(context.Background(), runtimeID)
	case "opencode":
		_ = InterruptOpencodeRuntime(runtimeID)
	case "claude":
		_ = InterruptClaudeRuntime(context.Background(), runtimeID)
	case "codex":
		_ = InterruptCodexRuntime(context.Background(), runtimeID)
	}
}

// stopNodeRuntime stops the runtime backing a node (kill + unregister).
func stopNodeRuntime(executor, runtimeID string) {
	if runtimeID == "" {
		return
	}
	switch executor {
	case "opencode":
		_ = StopOpencodeRuntime(runtimeID)
	default:
		_ = stopManagedRuntime(ExecutorType(executor), runtimeID)
	}
}

// waitForTurnOutcome waits for the corrective message turn to finish on a
// serve runtime and returns the runtime's current output. The turn is
// considered done when the runtime became busy after the message and is idle
// again (CanSend true).
func waitForTurnOutcome(ctx context.Context, executor, runtimeID string) (string, error) {
	ticker := time.NewTicker(stallPollInterval)
	defer ticker.Stop()
	sawBusy := false
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-ticker.C:
			var snap *RuntimeConsoleSnapshot
			var ok bool
			if stallConsoleSnapshot != nil {
				snap, ok = stallConsoleSnapshot(executor, runtimeID)
			} else {
				switch executor {
				case "mimo":
					snap, ok = GetMimoRuntimeConsole(runtimeID)
				case "opencode":
					snap, ok = SnapshotOpencodeRuntime(runtimeID)
				case "claude":
					snap, ok = GetClaudeRuntimeConsole(runtimeID)
				case "codex":
					snap, ok = GetCodexRuntimeConsole(runtimeID)
				}
			}
			if !ok || snap == nil {
				continue
			}
			if snap.Runtime.Status == RuntimeStopped || snap.Runtime.Status == RuntimeError {
				return snap.Output, fmt.Errorf("runtime %s while waiting for corrective turn", snap.Runtime.Status)
			}
			if snap.CanInterrupt {
				sawBusy = true
			}
			if sawBusy && snap.CanSend {
				return snap.Output, nil
			}
		}
	}
}

// sendRuntimeMessage sends a text message to a serve runtime and returns the
// turn id (best effort).
func sendRuntimeMessage(ctx context.Context, executor, runtimeID, text string) (string, error) {
	if stallSendMessage != nil {
		return stallSendMessage(executor, runtimeID, text)
	}
	switch executor {
	case "mimo":
		return SendMimoRuntimeMessage(ctx, runtimeID, text)
	case "opencode":
		if err := SendOpencodeRuntimeMessage(runtimeID, text); err != nil {
			return "", err
		}
		return "manual", nil
	case "claude":
		return SendClaudeRuntimeMessage(ctx, runtimeID, text)
	case "codex":
		return SendCodexRuntimeMessage(ctx, runtimeID, text)
	}
	return "", fmt.Errorf("executor %q does not support runtime messages", executor)
}

// nodeExecOutcome is the unified result of one node execution pass, plus the
// attempt ID that should record the final result (the original attempt, or a
// restart attempt).
type nodeExecOutcome struct {
	output, stderr string
	usage          *TokenUsage
	runtimeID      string
	endpoint       string
	extSessionID   string
	err            error
	attemptID      string
}

// rawExec is the raw tuple returned by executeNodeWithLoopProtocolAtWorkspace.
type rawExec struct {
	o, st string
	u     *TokenUsage
	rid   string
	ep    string
	es    string
	e     error
}

// execWithStallRepair executes the node under the reviewer-driven stall
// repair loop. When the node stalls, the execution context is cancelled
// first (diagnose after interrupt), then the reviewer is woken in
// maintenance mode and the returned plan is applied:
//
//	nudge    → send corrective message, wait for the turn, return its output
//	restart  → stop the runtime, create a new attempt, rerun with correction
//	noop     → return the interrupted result (manual resume path)
//
// Any failure inside the repair loop (planner error, invalid plan, budget
// exceeded) degrades to the raw result of the interrupted execution, so the
// outer state machine falls back to its normal interrupted/failed handling.
func (s *Store) execWithStallRepair(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID, iterationID, nodeID string, nodeCopy *AgentNode, input, contextPolicy, externalSessionID string, loopReview bool, bindingID, providerSessionID string, interventions int) nodeExecOutcome {
	workspace := runWorkspace(run)
	reviewerNode := findReviewerNode(pipe, run.LoopConfig.ReviewNodeID)
	repairOn := loopReview && reviewerNode != nil &&
		stallMaintenanceEnabled(nodeCopy.StallMaintenance)

	if !repairOn {
		o, st, u, rid, ep, es, e := s.executeNodeWithLoopProtocolAtWorkspace(ctx, nodeCopy, input, contextPolicy, externalSessionID, true, loopReview, workspace)
		return nodeExecOutcome{output: o, stderr: st, usage: u, runtimeID: rid, endpoint: ep, extSessionID: es, err: e, attemptID: ""}
	}

	// ── Repair path ──
	execCtx, cancelExec := context.WithCancel(ctx)
	defer cancelExec()
	resultCh := make(chan rawExec, 1)
	startedCh := make(chan string, 1)
	go func() {
		o, st, u, rid, ep, es, e := s.executeNodeWithLoopProtocolAtWorkspace(execCtx, nodeCopy, input, contextPolicy, externalSessionID, true, loopReview, workspace,
			func(endpoint string, port int) {
				select {
				case startedCh <- endpoint:
				default:
				}
			})
		resultCh <- rawExec{o: o, st: st, u: u, rid: rid, ep: ep, es: es, e: e}
	}()

	watcher := newStallWatcher(string(nodeCopy.Executor), nodeCopy.Mode, "")
	if stallWatcherHook != nil {
		stallWatcherHook(watcher)
	}
	ticker := time.NewTicker(watcher.interval)
	defer ticker.Stop()

	// Turn-duration fallback (no progress stream): trigger once the elapsed
	// time crosses the threshold.
	var lastTick time.Time

	for {
		select {
		case ep := <-startedCh:
			watcher.runtimeID = runtimeIDForEndpoint(string(nodeCopy.Executor), ep)
			if watcher.progress == nil && watcher.runtimeID != "" && stallObservable(string(nodeCopy.Executor), nodeCopy.Mode) {
				// Late discovery: the runtime started before we polled.
				watcher.threshold = stallNoProgressTimeout
				watcher.progress = watcher.pollConsole
				watcher.lastPoll = time.Now()
			}
		case <-ticker.C:
			if interventions >= stallMaxInterventions {
				continue
			}
			stalled := false
			if watcher.progress == nil {
				// Threshold-only mode: elapsed since start.
				if lastTick.IsZero() {
					lastTick = time.Now()
				}
				if time.Since(lastTick) >= watcher.threshold {
					// Re-anchor so repeated ticks don't retrigger instantly.
					lastTick = time.Now()
					stalled = true
				}
			} else {
				// Reset the anchor whenever progress is observed.
				if watcher.stallDuration() >= watcher.threshold {
					// Re-anchor after a stall fires so the next trigger needs a
					// fresh quiet window.
					watcher.lastPoll = time.Now()
					stalled = true
				}
			}
			if !stalled {
				continue
			}
			outcome := s.runStallRepair(ctx, run, pipe, sessionID, iterationID, nodeID, nodeCopy, reviewerNode, input, contextPolicy, externalSessionID, workspace, watcher, interventions, cancelExec, resultCh, bindingID, providerSessionID, loopReview)
			if outcome != nil {
				return *outcome
			}
		case r := <-resultCh:
			// Execution finished on its own (possibly because ctx was
			// cancelled by repair). Return whatever it produced.
			return nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
		case <-ctx.Done():
			cancelExec()
			select {
			case r := <-resultCh:
				return nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
			case <-time.After(10 * time.Second):
				return nodeExecOutcome{err: ctx.Err()}
			}
		}
	}
}

// runStallRepair performs one repair pass: cancel the stuck execution, build
// the scene, wake the reviewer, apply the plan. Returns the final outcome, or
// nil when the caller should keep waiting (no stall actually happened).
// runStallRepair performs one repair pass: cancel the stuck execution, build
// the scene, wake the reviewer, apply the plan. Returns the final outcome, or
// nil when the caller should keep waiting (no stall actually happened).
// ctx is the outer run context — deliberately NOT the cancelled execution
// context, so the planner call and the corrective turn still have a live
// deadline.
func (s *Store) runStallRepair(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID, iterationID, nodeID string, nodeCopy *AgentNode, reviewerNode *AgentNode, input, contextPolicy, externalSessionID, workspace string, watcher *stallWatcher, interventions int, cancelExec context.CancelFunc, resultCh chan rawExec, bindingID, providerSessionID string, loopReview bool) *nodeExecOutcome {
	// Cancel the stuck execution first: the repair works on a frozen scene.
	cancelExec()
	var r rawExec
	select {
	case r = <-resultCh:
	case <-time.After(10 * time.Second):
		r.e = context.DeadlineExceeded
	}
	if r.e == nil {
		// The model finished right before the interrupt: treat as normal
		// completion and abandon the repair.
		return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
	}

	// Interrupt the live runtime (T1 only) so the scene is quiet.
	interruptRuntime(string(nodeCopy.Executor), r.rid)
	watcher.runtimeID = r.rid

	// ── Maintenance scene ──
	scene := maintenanceScene{
		RunID:          run.ID,
		IterationID:    iterationID,
		NodeID:         nodeID,
		NodeLabel:      nodeCopy.Label,
		NodeType:       string(nodeCopy.Type),
		Executor:       string(nodeCopy.Executor),
		Model:          nodeCopy.Model,
		Mode:           nodeCopy.Mode,
		RuntimeID:      r.rid,
		Task:           truncateText(input, 2000),
		OutputSummary:  truncateText(r.o, 2000),
		EventSummary:   summarizeEvents(watcher.eventsSnapshot),
		StallDuration:  time.Since(watcher.lastPoll).Round(time.Second).String(),
		Interventions:  interventions,
		IterationIndex: run.CurrentIteration,
	}

	plan, planErr := s.runMaintenancePlanner(ctx, reviewerNode, scene, workspace)
	if planErr != nil {
		s.appendMaintenanceEvent(run, iterationID, nodeID, "planner failed: "+truncateText(planErr.Error(), 300), "planner", "failed", "")
		return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
	}

	switch plan.Judgment {
	case "nudge":
		if !stallObservable(string(nodeCopy.Executor), nodeCopy.Mode) {
			s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, "nudge", "skipped", "executor has no message channel; falls back to interrupted result")
			return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
		}
		if _, msgErr := sendRuntimeMessage(ctx, string(nodeCopy.Executor), r.rid, plan.Nudge.Message); msgErr != nil {
			s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, "nudge", "failed", msgErr.Error())
			return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
		}
		turnCtx, cancelTurn := context.WithTimeout(ctx, remainingNodeBudget(ctx, stallTurnWarnTimeout))
		defer cancelTurn()
		newOutput, waitErr := waitForTurnOutcome(turnCtx, string(nodeCopy.Executor), r.rid)
		s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, "nudge", "applied", truncateText(plan.Nudge.Message, 300))
		if waitErr != nil {
			return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: waitErr}
		}
		// Corrective turn produced the final output; original partial output
		// is preserved in the attempt history.
		return &nodeExecOutcome{output: newOutput, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: nil}

	case "restart":
		// Stop the old runtime, create a fresh attempt, rerun with the
		// correction injected. The recursive repair keeps monitoring the
		// rerun until the intervention budget runs out.
		stopNodeRuntime(string(nodeCopy.Executor), r.rid)
		attempt, attemptErr := s.CreateAttemptWithIteration(run.ID, nodeID, bindingID, iterationID)
		if attemptErr != nil {
			s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, "restart", "failed", attemptErr.Error())
			return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
		}
		correctedInput := input + "\n\n[维护纠正] " + plan.Restart.Correction
		_ = s.UpdateAttempt(attempt.ID, func(a *NodeAttempt) {
			a.Executor = string(nodeCopy.Executor)
			a.Model = nodeCopy.Model
			a.Mode = nodeCopy.Mode
			a.Agent = nodeCopy.Agent
			a.Skill = nodeCopy.Skill
			a.ProviderSessionID = providerSessionID
			a.Input = correctedInput
		})
		repairCtx := ctx
		out := s.execWithStallRepair(repairCtx, run, pipe, sessionID, iterationID, nodeID, nodeCopy,
			correctedInput, contextPolicy, externalSessionID, loopReview, bindingID, providerSessionID, interventions+1)
		s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, "restart", "applied", truncateText(plan.Restart.Correction, 300))
		// A deeper nested restart records under its own attempt; only adopt
		// this one when the rerun never repaired again.
		if out.attemptID == "" {
			out.attemptID = attempt.ID
		}
		return &out

	case "noop":
		s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, "noop", "applied", "judged merely slow; interrupted result goes to manual resume")
		return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}

	default:
		s.appendMaintenanceEvent(run, iterationID, nodeID, plan.Reason, plan.Judgment, "failed", "unknown judgment")
		return &nodeExecOutcome{output: r.o, stderr: r.st, usage: r.u, runtimeID: r.rid, endpoint: r.ep, extSessionID: r.es, err: r.e}
	}
}

// remainingNodeBudget returns a reasonable wait budget for a corrective turn.
func remainingNodeBudget(ctx context.Context, fallback time.Duration) time.Duration {
	if d, ok := ctx.Deadline(); ok {
		left := time.Until(d)
		if left > 0 {
			if left < fallback {
				return left
			}
		}
	}
	return fallback
}

// maintenanceScene is the diagnosis package handed to the reviewer in
// maintenance mode.
type maintenanceScene struct {
	RunID          string
	IterationID    string
	NodeID          string
	NodeLabel       string
	NodeType       string
	Executor       string
	Model          string
	Mode           string
	RuntimeID      string
	Task           string
	OutputSummary  string
	EventSummary   string
	StallDuration  string
	Interventions  int
	IterationIndex int
}

func truncateText(s string, max int) string {
	s = strings.TrimSpace(s)
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}

func summarizeEvents(events []RuntimeConsoleEvent) string {
	if len(events) == 0 {
		return ""
	}
	n := len(events)
	if n > 10 {
		events = events[n-10:]
	}
	var parts []string
	for _, e := range events {
		txt := ""
		if e.Text != "" {
			txt = e.Text
		} else if e.Method != "" {
			txt = e.Method
		} else if e.Category != "" {
			txt = e.Category
		}
		if txt != "" {
			parts = append(parts, truncateText(txt, 120))
		}
	}
	return strings.Join(parts, "\n")
}

// appendMaintenanceEvent records a repair event on the run (persisted with
// the run JSON) and broadcasts it to live frontends.
func (s *Store) appendMaintenanceEvent(run *PipelineRun, iterationID, nodeID, reason, action, outcome, detail string) {
	if run == nil {
		return
	}
	ev := MaintenanceEvent{
		NodeID:      nodeID,
		IterationID: iterationID,
		At:          time.Now().UTC().Format(time.RFC3339),
		Reason:      reason,
		Action:      action,
		Outcome:     outcome,
		Detail:      detail,
	}
	s.mu.Lock()
	run.MaintenanceEvents = append(run.MaintenanceEvents, ev)
	s.mu.Unlock()
	payload, _ := json.Marshal(struct {
		NodeID      string `json:"nodeId"`
		IterationID string `json:"iterationId,omitempty"`
		Action      string `json:"action"`
		Outcome     string `json:"outcome"`
		Reason      string `json:"reason"`
		Detail      string `json:"detail,omitempty"`
	}{ev.NodeID, ev.IterationID, ev.Action, ev.Outcome, ev.Reason, ev.Detail})
	s.emit(event.Event{Kind: event.PipelineNodeMaintenance, Text: nodeID, Detail: string(payload)})
}

// findReviewerNode returns the node designated as the loop reviewer, or nil.
func findReviewerNode(pipe *Pipeline, reviewNodeID string) *AgentNode {
	if pipe == nil {
		return nil
	}
	for i := range pipe.Nodes {
		if pipe.Nodes[i].ID == reviewNodeID && pipe.Nodes[i].Type == NodeReviewer {
			return &pipe.Nodes[i]
		}
	}
	return nil
}

// runMaintenancePlanner wakes the reviewer in maintenance mode to diagnose a
// stalled node and returns the repair plan. The call is serialized against
// the end-of-round review (one turn at a time on a serve runtime); when the
// reviewer runtime is busy, the caller degrades to the interrupted result.
func (s *Store) runMaintenancePlanner(ctx context.Context, reviewerNode *AgentNode, scene maintenanceScene, workspace string) (MaintenancePlan, error) {
	if !maintenancePlannerMu.TryLock() {
		return MaintenancePlan{}, fmt.Errorf("maintenance planner busy: reviewer runtime is in use")
	}
	defer maintenancePlannerMu.Unlock()

	prompt := maintenancePlannerPrompt(scene)
	callCtx, cancel := context.WithTimeout(ctx, maintenanceTimeout)
	defer cancel()

	runOnce := func(p string) (string, string, error) {
		out, stderr, _, _, _, _, e := s.executeNodeWithLoopProtocolAtWorkspace(callCtx, reviewerNode, p, "fresh", "", false, false, workspace)
		return out, stderr, e
	}

	out, stderr, err := runOnce(prompt)
	if err != nil {
		return MaintenancePlan{}, fmt.Errorf("maintenance planner: %w (stderr: %s)", err, truncateText(stderr, 300))
	}
	plan, perr := ParseMaintenancePlan(out)
	if perr != nil {
		// One systematic format-correction retry, mirroring the loop-review
		// path. Still malformed → abandon maintenance, original path resumes.
		retryPrompt := prompt + "\n\n[系统纠正] 你上一条响应不是合法的 maintenance-plan-v1 计划，可能只输出了工具调用参数或思考。请不要再次调用工具，立即只输出一个纯 JSON 对象，严格包含 schemaVersion、judgment、reason，并根据 judgment 包含 nudge.message（nudge 时）或 restart.correction（restart 时）。judgment 只能是 nudge、restart 或 noop。"
		out2, stderr2, err2 := runOnce(retryPrompt)
		if err2 != nil {
			return MaintenancePlan{}, fmt.Errorf("maintenance planner retry: %w", err2)
		}
		out = out2
		if strings.TrimSpace(stderr2) != "" {
			stderr = stderr2
		}
		plan, perr = ParseMaintenancePlan(out)
		if perr != nil {
			return MaintenancePlan{}, fmt.Errorf("maintenance plan invalid after retry: %w", perr)
		}
	}
	if verr := ValidateMaintenancePlan(plan); verr != nil {
		return MaintenancePlan{}, verr
	}
	return plan, nil
}

// maintenancePlannerPrompt builds the maintenance-mode diagnosis prompt: the
// scene plus the maintenance-plan-v1 protocol. The reviewer must return a
// single JSON object, never a chat reply.
func maintenancePlannerPrompt(scene maintenanceScene) string {
	sceneJSON, _ := json.MarshalIndent(scene, "", "  ")
	return `你是审查者维护模式。执行者节点疑似卡住（长时间无进展），编排器已中断它并冻结现场。请诊断后输出唯一的修复指令。

可用动作：
- nudge: 节点仍在运行但跑偏/空转。给一条简短、明确、可执行的纠正消息，要求它立即继续完成任务。只适合有消息通道的运行时。
- restart: 节点已死锁/损坏，无法通过消息救回。给出 correction（任务纠正/补丁提示），编排器会杀掉运行时并用原任务+纠正重跑。correction 要能独立指导重跑。
- noop: 只是慢，没有明显问题。编排器将放弃维护，把中断结果交给人工恢复路径。

规则：
1. 只输出一个纯 JSON 对象，不要输出任何其他文字、代码块或工具调用。
2. 诊断要依据现场证据（输出摘要/事件摘要/卡顿时长），不要猜测。
3. 这是第 N 次干预（Interventions 字段，从 0 开始）。累计干预已达上限时，优先 noop 让位人工。

输出格式（maintenance-plan-v1）：
{
  "schemaVersion": "maintenance-plan-v1",
  "judgment": "nudge|restart|noop",
  "reason": "诊断结论（简短中文）",
  "nudge": {"message": "纠正消息"},
  "restart": {"correction": "重跑纠正"}
}` + "\n\n## 卡住现场\n\n" + string(sceneJSON)
}
