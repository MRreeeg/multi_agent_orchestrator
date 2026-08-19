package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"reasonix/internal/event"
)

const (
	// A provider/tool call must not be able to leave a pipeline node in running
	// forever. These defaults are deliberately generous for coding tasks while
	// still making a hung serve/runtime recoverable through Resume.
	loopNodeExecutionTimeout = 10 * time.Minute
	// Review is a single bounded judgment. A runaway reviewer must become a
	// resumable interruption instead of consuming the whole iteration budget.
	loopReviewerExecutionTimeout  = 3 * time.Minute
	loopIterationExecutionTimeout = 30 * time.Minute
)

// loopStart defines where the loop begins. Used for both fresh starts and resume.
type loopStart struct {
	IterationNumber     int    // 1 for fresh, N for resume
	InputTask           string // task for the first iteration
	ExistingIterationID string // non-empty to reuse an interrupted iteration
}

// ExecuteLoop runs a pipeline with loop support.
func (s *Store) ExecuteLoop(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID string) error {
	// Validate context policy
	if err := validateContextPolicy(run.ExecOptions.ContextPolicy); err != nil {
		return fmt.Errorf("invalid context policy: %w", err)
	}

	if !run.LoopConfig.Enabled || run.LoopConfig.Mode == "" || run.LoopConfig.Mode == "none" {
		if err := s.executePipelineV2(ctx, run, pipe, sessionID); err != nil {
			return err
		}
		return nil
	}

	// Validate loop config before starting
	if err := s.validateLoopConfig(run, pipe); err != nil {
		s.mu.Lock()
		run.Status = "failed"
		run.Error = fmt.Sprintf("invalid loop config: %s", err)
		run.FinishedAt = time.Now().UTC().Format(time.RFC3339)
		run.UpdatedAt = run.FinishedAt
		s.mu.Unlock()
		if perr := s.persistRun(run, sessionID); perr != nil {
			return fmt.Errorf("loop config invalid: %w; persist: %v", err, perr)
		}
		return err
	}

	return s.executeLoopStateMachine(ctx, run, pipe, sessionID, loopStart{
		IterationNumber: 1,
		InputTask:       run.Task,
	})
}

// ResumeLoop resumes an interrupted loop run from the last interrupted iteration.
func (s *Store) ResumeLoop(ctx context.Context, run *PipelineRun, rev *PipelineRevision, sessionID string) error {
	// Validate context policy
	if err := validateContextPolicy(run.ExecOptions.ContextPolicy); err != nil {
		return fmt.Errorf("invalid context policy: %w", err)
	}

	if run.Status != "interrupted" {
		return fmt.Errorf("cannot resume run %s: status is %q, not interrupted", run.ID, run.Status)
	}

	// Validate revision matches
	if rev.ID != run.PipelineRevisionID {
		return fmt.Errorf("revision mismatch: run expects %s, got %s", run.PipelineRevisionID, rev.ID)
	}
	if rev.SessionID != run.SessionID {
		return fmt.Errorf("session mismatch: run session %s, revision session %s", run.SessionID, rev.SessionID)
	}

	// Find the last interrupted iteration
	var lastIter *LoopIteration
	for _, iterID := range run.IterationIDs {
		if iter, ok := s.GetIteration(iterID); ok && iter.Status == "interrupted" {
			// IterationIDs are normally appended in order, but recovery data can
			// be loaded from older stores. Select by number rather than relying
			// on slice order so resume always starts from the latest interrupted
			// iteration.
			if lastIter == nil || iter.Number > lastIter.Number {
				candidate := iter
				lastIter = &candidate
			}
		}
	}

	if lastIter == nil {
		return fmt.Errorf("no interrupted iteration found for run %s", run.ID)
	}

	// Build pipeline from revision
	pipe := &Pipeline{
		ID:    rev.ID,
		Name:  rev.Name,
		Nodes: rev.Nodes,
		Edges: rev.Edges,
	}

	// Validate loop config
	if err := s.validateLoopConfig(run, pipe); err != nil {
		return fmt.Errorf("loop config invalid on resume: %w", err)
	}

	// Reset run status to running
	s.mu.Lock()
	run.Status = "running"
	run.Error = ""
	run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	s.mu.Unlock()
	if err := s.persistRun(run, sessionID); err != nil {
		return fmt.Errorf("persist run: %w", err)
	}

	return s.executeLoopStateMachine(ctx, run, pipe, sessionID, loopStart{
		IterationNumber:     lastIter.Number,
		InputTask:           lastIter.InputTask,
		ExistingIterationID: lastIter.ID,
	})
}

// executeLoopStateMachine is the single loop state machine used by both ExecuteLoop and ResumeLoop.
// All persistence errors in this function MUST be checked and returned.
func (s *Store) executeLoopStateMachine(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID string, start loopStart) error {
	// Every loop exit, including review parse failure and context timeout, must
	// finalize the provider runtimes created by its attempts. Without this
	// cleanup a finished/blocked run can leave the UI showing agents as running
	// and a canceled serve process can remain borrowable forever.
	defer s.finalizeLoopRuntimes(run)

	loop := run.LoopConfig
	maxIter := loop.MaxIterations
	if loop.Mode == "fixed" {
		maxIter = loop.FixedIterations
	}

	iterationNum := start.IterationNumber - 1 // incremented at loop start
	currentTask := start.InputTask

	for {
		// Check context cancellation at iteration start
		if ctx.Err() != nil {
			if err := s.finishLoopRun(run, sessionID, "canceled", "context canceled", ""); err != nil {
				return fmt.Errorf("context canceled: %w; persist: %v", ctx.Err(), err)
			}
			return ctx.Err()
		}

		iterationNum++

		// Check iteration limit BEFORE creating the iteration
		if iterationNum > maxIter {
			if err := s.finishLoopRun(run, sessionID, "complete", "", "fixed_limit"); err != nil {
				return fmt.Errorf("fixed limit: %w", err)
			}
			return nil
		}

		// Create or reuse iteration
		var iter LoopIteration
		if start.ExistingIterationID != "" && iterationNum == start.IterationNumber {
			// Resume: reuse the interrupted iteration
			existing, ok := s.GetIteration(start.ExistingIterationID)
			if !ok {
				return fmt.Errorf("interrupted iteration %s not found", start.ExistingIterationID)
			}
			iter = existing
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationRunning
				it.Error = ""
				it.InputTask = currentTask
				it.StartedAt = time.Now().UTC().Format(time.RFC3339)
				it.FinishedAt = ""
			}); err != nil {
				return fmt.Errorf("resume iteration: %w", err)
			}
		} else {
			// Fresh: create new iteration
			iter = LoopIteration{
				ID:        s.nextIterationID(),
				RunID:     run.ID,
				Number:    iterationNum,
				Status:    IterationRunning,
				InputTask: currentTask,
				StartedAt: time.Now().UTC().Format(time.RFC3339),
			}
			if err := s.CreateIteration(iter); err != nil {
				return fmt.Errorf("create iteration: %w", err)
			}
		}

		// Update run with current iteration
		s.mu.Lock()
		run.CurrentIteration = iterationNum
		// Only append if not already in the list (resume case)
		alreadyTracked := false
		for _, id := range run.IterationIDs {
			if id == iter.ID {
				alreadyTracked = true
				break
			}
		}
		if !alreadyTracked {
			run.IterationIDs = append(run.IterationIDs, iter.ID)
		}
		run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		s.mu.Unlock()
		s.emit(event.Event{Kind: event.PipelineIteration, Text: run.ID, Detail: fmt.Sprintf(`{"runID":"%s","iteration":%d,"maxIterations":%d}`, run.ID, iterationNum, maxIter)})
		if err := s.persistRun(run, sessionID); err != nil {
			return fmt.Errorf("persist run after iteration create: %w", err)
		}

		// Execute one iteration
		execErr := s.executeOneIteration(ctx, run, pipe, sessionID, &iter, currentTask)
		finishedAt := time.Now().UTC().Format(time.RFC3339)

		// Parent cancellation is an explicit cancel. An internal node/iteration
		// timeout is resumable and therefore becomes interrupted instead.
		if ctx.Err() != nil {
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationCanceled
				it.FinishedAt = finishedAt
			}); err != nil {
				return failLoopPersistence(err, ctx.Err())
			}
			if err := s.finishLoopRun(run, sessionID, "canceled", "context canceled", "canceled"); err != nil {
				return fmt.Errorf("close run: %w; context: %v", err, ctx.Err())
			}
			return ctx.Err()
		}
		if execErr != nil && (errors.Is(execErr, context.DeadlineExceeded) || errors.Is(execErr, context.Canceled)) {
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationInterrupted
				it.Error = execErr.Error()
				it.FinishedAt = finishedAt
			}); err != nil {
				return failLoopPersistence(err, execErr)
			}
			if err := s.finishLoopRun(run, sessionID, "interrupted", execErr.Error(), "interrupted"); err != nil {
				return fmt.Errorf("interrupt run: %w; cause: %v", err, execErr)
			}
			return execErr
		}

		// Execution error
		if execErr != nil {
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationFailed
				it.Error = execErr.Error()
				it.FinishedAt = finishedAt
			}); err != nil {
				return failLoopPersistence(err, execErr)
			}
			if err := s.finishLoopRun(run, sessionID, "failed", fmt.Sprintf("iteration %d failed: %s", iterationNum, execErr), ""); err != nil {
				return fmt.Errorf("iteration failed: %w; persist: %v", execErr, err)
			}
			return execErr
		}

		// Find review output for THIS iteration
		reviewOutput := s.findReviewOutputForIteration(run, iter.ID, loop.ReviewNodeID)
		if reviewOutput == "" {
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationFailed
				it.Error = "no review output found"
				it.FinishedAt = finishedAt
			}); err != nil {
				noReviewErr := fmt.Errorf("no review output from node %s", loop.ReviewNodeID)
				return failLoopPersistence(err, noReviewErr)
			}
			noReviewErr := fmt.Errorf("no review output from node %s", loop.ReviewNodeID)
			if err := s.finishLoopRun(run, sessionID, "failed", noReviewErr.Error(), ""); err != nil {
				return fmt.Errorf("iteration failed: %w; persist: %v", noReviewErr, err)
			}
			return noReviewErr
		}

		// Parse review decision
		decision, err := ParseReviewDecision(reviewOutput)
		if err != nil {
			if err2 := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationBlocked
				it.Error = fmt.Sprintf("review parse failed: %s", err)
				it.FinishedAt = finishedAt
			}); err2 != nil {
				return failLoopPersistence(err2, fmt.Errorf("review output invalid: %w", err))
			}
			parseErr := fmt.Errorf("review output invalid: %w", err)
			if cerr := s.finishLoopRun(run, sessionID, "blocked", parseErr.Error(), "blocked"); cerr != nil {
				return fmt.Errorf("%w; persist: %v", parseErr, cerr)
			}
			return parseErr
		}

		// Validate decision
		if err := ValidateReviewDecision(decision); err != nil {
			if err2 := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationBlocked
				it.Error = fmt.Sprintf("review validation failed: %s", err)
				it.FinishedAt = finishedAt
			}); err2 != nil {
				return failLoopPersistence(err2, fmt.Errorf("review validation failed: %w", err))
			}
			validErr := fmt.Errorf("review validation failed: %w", err)
			if cerr := s.finishLoopRun(run, sessionID, "blocked", validErr.Error(), "blocked"); cerr != nil {
				return fmt.Errorf("%w; persist: %v", validErr, cerr)
			}
			return validErr
		}

		// Update iteration with decision and review attempt ID
		reviewAttID := s.findReviewAttemptIDForIteration(run, iter.ID, loop.ReviewNodeID)
		if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
			it.Decision = decision.Decision
			it.NextTask = decision.NextTask
			it.ReviewAttemptID = reviewAttID
			it.FinishedAt = finishedAt
		}); err != nil {
			return failLoopPersistence(err, fmt.Errorf("iteration %d decision update failed", iterationNum))
		}

		// Handle decision — single exit point for all terminal states
		switch decision.Decision {
		case "blocked":
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationBlocked
			}); err != nil {
				return failLoopPersistence(err, fmt.Errorf("blocked at iteration %d", iterationNum))
			}
			s.mu.Lock()
			run.FinalReview = &decision
			s.mu.Unlock()
			if err := s.finishLoopRun(run, sessionID, "blocked", fmt.Sprintf("blocked at iteration %d: %s", iterationNum, decision.Summary), "blocked"); err != nil {
				return fmt.Errorf("blocked: %w; persist: %v", fmt.Errorf("blocked at iteration %d", iterationNum), err)
			}
			return nil

		case "pass":
			if loop.Mode == "review_decides" {
				if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
					it.Status = IterationPassed
				}); err != nil {
					return failLoopPersistence(err, fmt.Errorf("review_pass"))
				}
				s.mu.Lock()
				run.FinalReview = &decision
				s.mu.Unlock()
				if err := s.finishLoopRun(run, sessionID, "complete", "", "review_pass"); err != nil {
					return fmt.Errorf("review_pass: persist: %w", err)
				}
				return nil
			}

			// fixed mode: pass does NOT terminate, check if we've reached the limit
			if iterationNum >= maxIter {
				if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
					it.Status = IterationPassedByLimit
				}); err != nil {
					return failLoopPersistence(err, fmt.Errorf("fixed_limit"))
				}
				s.mu.Lock()
				run.FinalReview = &decision
				s.mu.Unlock()
				if err := s.finishLoopRun(run, sessionID, "complete", "", "fixed_limit"); err != nil {
					return fmt.Errorf("fixed_limit: persist: %w", err)
				}
				return nil
			}
			// Continue to next iteration — persistence failure terminates the run
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationPassed
			}); err != nil {
				if perr := s.finishLoopRun(run, sessionID, "failed",
					fmt.Sprintf("persistence failure at iteration %d: %s", iterationNum, err), ""); perr != nil {
					return fmt.Errorf("iteration %d persistence failed: %w; close: %v", iterationNum, err, perr)
				}
				return fmt.Errorf("iteration %d persistence failed: %w", iterationNum, err)
			}
			currentTask = decision.NextTask
			if currentTask == "" {
				currentTask = run.Task
			}
			continue

		case "revise":
			if iterationNum >= maxIter {
				if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
					it.Status = IterationCompletedByLimit
				}); err != nil {
					return failLoopPersistence(err, fmt.Errorf("fixed_limit"))
				}
				s.mu.Lock()
				run.FinalReview = &decision
				s.mu.Unlock()
				if err := s.finishLoopRun(run, sessionID, "complete", "", "fixed_limit"); err != nil {
					return fmt.Errorf("fixed_limit: persist: %w", err)
				}
				return nil
			}
			// Continue to next iteration — persistence failure terminates the run
			if err := s.UpdateIteration(iter.ID, func(it *LoopIteration) {
				it.Status = IterationRevising
			}); err != nil {
				if perr := s.finishLoopRun(run, sessionID, "failed",
					fmt.Sprintf("persistence failure at iteration %d: %s", iterationNum, err), ""); perr != nil {
					return fmt.Errorf("iteration %d persistence failed: %w; close: %v", iterationNum, err, perr)
				}
				return fmt.Errorf("iteration %d persistence failed: %w", iterationNum, err)
			}
			currentTask = decision.NextTask
			if currentTask == "" {
				currentTask = buildRevisionTask(decision.RequiredChanges)
			}
			continue
		}
	}
}

// NormalizeLoopConfig applies compatibility migrations and validates fields that
// are independent of a pipeline's node list. It is intentionally shared by
// persistence and execution so a configuration cannot be accepted by one path
// and rejected by the other.
func NormalizeLoopConfig(cfg *LoopConfig) (LoopConfig, error) {
	if cfg == nil {
		return LoopConfig{}, nil
	}
	normalized := *cfg
	if !normalized.Enabled {
		// Disabled is a canonical zero configuration. Do not carry stale mode,
		// reviewer, or iteration fields into a revision after the user turns
		// Loop off; those stale fields otherwise create needless revisions and
		// confuse the frontend on the next reload.
		return LoopConfig{Enabled: false}, nil
	}

	switch normalized.Mode {
	case "review_decides", "fixed":
	default:
		return LoopConfig{}, fmt.Errorf("invalid loop mode %q: must be review_decides or fixed", normalized.Mode)
	}
	if normalized.Protocol != "loop-review-v1" {
		return LoopConfig{}, fmt.Errorf("invalid protocol %q: must be loop-review-v1", normalized.Protocol)
	}
	if normalized.ReviewNodeID == "" {
		return LoopConfig{}, fmt.Errorf("%s mode requires review node ID", normalized.Mode)
	}

	switch normalized.Mode {
	case "review_decides":
		if normalized.MaxIterations < 1 || normalized.MaxIterations > 10 {
			return LoopConfig{}, fmt.Errorf("maxIterations must be between 1 and 10, got %d", normalized.MaxIterations)
		}
	case "fixed":
		// Older persisted/frontend payloads used maxIterations for fixed mode.
		if normalized.FixedIterations == 0 && normalized.MaxIterations > 0 {
			normalized.FixedIterations = normalized.MaxIterations
		}
		if normalized.FixedIterations < 1 || normalized.FixedIterations > 10 {
			return LoopConfig{}, fmt.Errorf("fixedIterations must be between 1 and 10, got %d", normalized.FixedIterations)
		}
	}
	return normalized, nil
}

// ValidateLoopConfig checks that a LoopConfig is valid for persistence and
// execution. Disabled configurations deliberately do not require any other
// fields, while enabled configurations must reference a reviewer node.
func ValidateLoopConfig(cfg *LoopConfig, nodes []AgentNode) error {
	normalized, err := NormalizeLoopConfig(cfg)
	if err != nil {
		return err
	}
	if !normalized.Enabled {
		return nil
	}
	for _, n := range nodes {
		if n.ID != normalized.ReviewNodeID {
			continue
		}
		if n.Type != NodeReviewer {
			return fmt.Errorf("reviewNodeID %q references a %s node, not a reviewer", normalized.ReviewNodeID, n.Type)
		}
		return nil
	}
	return fmt.Errorf("review node %q not found in pipeline nodes", normalized.ReviewNodeID)
}

// validateLoopConfig checks that loop configuration is valid before starting
// and also normalizes legacy fixed-mode data copied from a revision.
func (s *Store) validateLoopConfig(run *PipelineRun, pipe *Pipeline) error {
	// PipelineRun pointers are shared with the executor goroutine and with
	// status readers. Copy the config under the store lock, normalize/validate
	// the copy off-lock, then publish the normalized value under the lock. The
	// previous implementation wrote run.LoopConfig without synchronization,
	// which made a harmless validation step race with GetRun during Loop start.
	s.mu.RLock()
	cfg := run.LoopConfig
	s.mu.RUnlock()

	normalized, err := NormalizeLoopConfig(&cfg)
	if err != nil {
		return err
	}
	if err := ValidateLoopConfig(&normalized, pipe.Nodes); err != nil {
		return err
	}

	s.mu.Lock()
	run.LoopConfig = normalized
	s.mu.Unlock()
	return nil
}

// finishLoopRun is the single exit point for all loop terminal states.
// It sets the run status, error, termination reason, and persists to disk.
func (s *Store) finishLoopRun(run *PipelineRun, sessionID, status, errMsg, terminationReason string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	s.mu.Lock()
	run.Status = status
	run.Error = errMsg
	run.TerminationReason = terminationReason
	run.FinishedAt = now
	run.UpdatedAt = now
	s.mu.Unlock()
	return s.persistRun(run, sessionID)
}

// finalizeLoopRuntimes closes the transient running state for every runtime
// used by this loop. Successful/blocked/failed runs retain healthy serve
// processes as idle for reuse; canceled/interrupted runs stop them so Resume
// never inherits a dead process.
func (s *Store) finalizeLoopRuntimes(run *PipelineRun) {
	s.mu.RLock()
	status := run.Status
	seen := make(map[string]bool)
	type runtimeInfo struct {
		id       string
		nodeID   string
		executor ExecutorType
		endpoint string
		port     int
	}
	var runtimes []runtimeInfo
	for _, attemptID := range run.NodeAttemptIDs {
		att, ok := s.attempts[attemptID]
		if !ok || att.RuntimeID == "" || seen[att.RuntimeID] {
			continue
		}
		seen[att.RuntimeID] = true
		info := runtimeInfo{id: att.RuntimeID, nodeID: att.NodeID, executor: ExecutorType(att.Executor)}
		if rt, ok := s.runtimeStates[att.RuntimeID]; ok {
			info.endpoint = rt.Endpoint
			info.port = rt.Port
			if info.executor == "" {
				info.executor = ExecutorType(rt.Executor)
			}
		}
		runtimes = append(runtimes, info)
	}
	s.mu.RUnlock()

	stop := status == "interrupted" || status == "canceled"
	for _, rt := range runtimes {
		if stop {
			_ = stopManagedRuntime(rt.executor, rt.id)
			s.UnregisterAgent(rt.id)
		} else {
			switch rt.executor {
			case ExecutorMimo:
				_ = mimoRuntimeMgr.Release(rt.id)
			case ExecutorCodex:
				_ = codexRuntimeMgr.Release(rt.id)
			case ExecutorClaude:
				_ = claudeRuntimeMgr.Release(rt.id)
			case ExecutorOpencode:
				_ = opencodeRuntimeMgr.Release(rt.id)
			default:
				_ = reasonixRuntimeMgr.Release(rt.id)
			}
			s.UpdateAgentStatus(rt.id, "idle", "")
		}

		rtStatus := RuntimeIdle
		if stop {
			rtStatus = RuntimeStopped
		}
		_ = s.UpdateRuntimeState(rt.id, func(state *RuntimeState) {
			state.Status = rtStatus
			if stop {
				state.Error = "loop runtime stopped after " + status
			}
		})
		detail := fmt.Sprintf(`{"runtimeID":"%s","endpoint":"%s","port":%d,"status":"%s","nodeID":"%s","executor":"%s"}`, rt.id, rt.endpoint, rt.port, rtStatus, rt.nodeID, rt.executor)
		s.emit(event.Event{Kind: event.PipelineNodeRuntime, Text: rt.nodeID, Detail: detail})
	}
}

// executeOneIteration executes a single iteration of the pipeline DAG.
func (s *Store) executeOneIteration(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID string, iter *LoopIteration, task string) error {
	// Reset node states for this iteration
	s.mu.Lock()
	for _, node := range pipe.Nodes {
		run.NodeStates[node.ID] = RunState{
			Status:     NodePending,
			TokenUsage: TokenUsage{},
		}
	}
	run.Task = task
	s.mu.Unlock()

	// Execute the DAG — return any error from pipeline execution
	if err := s.executePipelineIteration(ctx, run, pipe, sessionID, iter.ID); err != nil {
		return err
	}

	// Check result
	s.mu.Lock()
	status := run.Status
	errMsg := run.Error
	s.mu.Unlock()

	if status == "failed" || status == "canceled" {
		return fmt.Errorf("iteration failed: %s", errMsg)
	}
	return nil
}

// executePipelineIteration runs the DAG for a single iteration.
// Returns error on pipeline cycle or persistence failure during terminal state.
func (s *Store) executePipelineIteration(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID string, iterationID string) error {
	iterationCtx, cancelIteration := context.WithTimeout(ctx, loopIterationExecutionTimeout)
	defer cancelIteration()

	levels := topologicalLevels(pipe)
	if levels == nil {
		// Pipeline has a cycle — must report error, not swallow
		if err := s.finishLoopRun(run, sessionID, "failed", "pipeline contains a cycle", "failed"); err != nil {
			return fmt.Errorf("pipeline cycle: %w; persist: %v", fmt.Errorf("pipeline contains a cycle"), err)
		}
		return fmt.Errorf("pipeline contains a cycle")
	}

	// A Loop reuses the same DAG. Architect nodes establish the plan once;
	// subsequent iterations reuse their last successful output instead of
	// asking the architect to redesign the task on every review cycle.
	iterationNumber := 1
	s.mu.RLock()
	if it, ok := s.iterations[iterationID]; ok && it.Number > 0 {
		iterationNumber = it.Number
	}
	s.mu.RUnlock()

	for _, level := range levels {
		s.mu.Lock()
		if run.Status == "canceled" || run.Status == "failed" {
			s.mu.Unlock()
			return nil
		}
		s.mu.Unlock()

		// A node failure or timeout cancels the remaining parallel nodes in this
		// level so the iteration cannot wait on a sibling forever.
		levelCtx, cancelLevel := context.WithCancel(iterationCtx)
		errCh := make(chan error, len(level))
		var wg sync.WaitGroup
		for _, nodeID := range level {
			nodeID := nodeID
			wg.Add(1)
			go func() {
				defer wg.Done()
				// Check context before starting node
				if levelCtx.Err() != nil {
					return
				}
				s.mu.Lock()
				if run.Status == "canceled" || run.Status == "failed" {
					s.mu.Unlock()
					return
				}
				node := findNode(pipe, nodeID)
				if node == nil {
					s.mu.Unlock()
					return
				}
				nodeCopy := *node
				if iterationNumber > 1 && nodeCopy.Type == NodeArchitect {
					// Do not create a new attempt for the architect. Release the
					// store lock before looking up its prior attempt.
					s.mu.Unlock()
					output := s.latestSuccessfulNodeOutput(run, nodeID, iterationNumber-1)
					if strings.TrimSpace(output) == "" {
						errCh <- fmt.Errorf("architect node %q has no successful output to reuse in iteration %d", nodeCopy.Label, iterationNumber)
						cancelLevel()
						return
					}
					s.mu.Lock()
					state := run.NodeStates[nodeID]
					state.Status = NodeComplete
					state.Output = output
					state.DoneAt = time.Now().UTC().Format(time.RFC3339)
					run.NodeStates[nodeID] = state
					s.mu.Unlock()
					return
				}
				state := run.NodeStates[nodeID]
				state.Status = NodeRunning
				state.StartedAt = time.Now().UTC().Format(time.RFC3339)
				run.NodeStates[nodeID] = state
				run.CurrentNodeID = nodeID
				s.mu.Unlock()

				nodeTimeout := loopNodeExecutionTimeout
				if run.LoopConfig.Enabled && run.LoopConfig.ReviewNodeID == nodeID && nodeCopy.Type == NodeReviewer {
					nodeTimeout = loopReviewerExecutionTimeout
				}
				nodeCtx, cancelNode := context.WithTimeout(levelCtx, nodeTimeout)
				err := s.executeNodeAttempt(nodeCtx, run, pipe, sessionID, iterationID, nodeID, nodeCopy)
				cancelNode()
				if err != nil {
					errCh <- err
					cancelLevel()
				}
			}()
		}
		wg.Wait()
		cancelLevel()
		close(errCh)

		var firstErr error
		var firstNonContextErr error
		for err := range errCh {
			if err == nil {
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
			if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && firstNonContextErr == nil {
				firstNonContextErr = err
			}
		}
		if firstNonContextErr != nil {
			return fmt.Errorf("node execution: %w", firstNonContextErr)
		}
		if firstErr != nil {
			return fmt.Errorf("node execution: %w", firstErr)
		}
	}
	return nil
}

// executeNodeAttempt executes a single node with full binding/provider/attempt tracking.
// Returns error on business failure or persistence failure.
func (s *Store) executeNodeAttempt(ctx context.Context, run *PipelineRun, pipe *Pipeline, sessionID, iterationID, nodeID string, nodeCopy AgentNode) error {
	// Check context before starting
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Resolve an explicit Skill first, then use the task/role catalog router as
	// a backend safety net. The UI may be an older client or a generated step
	// may omit skill; execution must still inject the selected Skill and record
	// it on the attempt.
	loopReview := run.LoopConfig.Enabled && run.LoopConfig.ReviewNodeID == nodeID && nodeCopy.Type == NodeReviewer
	if loopReview {
		// The outer Orchestrator owns the Loop boundary. Keep the Reviewer
		// configured execution mode (run or serve) so the selected executor can
		// actually use its retained runtime, but force one bounded task turn.
		// In particular, do not rewrite serve to run: doing so bypasses Mimo's
		// runtime manager and makes Windows receive the entire prompt inline.
		nodeCopy.ExecutionMode = "task"
	}
	// Always canonicalize the configured value through the shared catalog. This
	// fixes older/generated pipelines that contain aliases such as "review" or
	// an invented skill name: a reviewer must receive the real review-agent
	// contract, and an unknown value must never be injected into execution.
	phase := "execution"
	if loopReview {
		phase = "loop-review"
	}
	taskForSkill := strings.TrimSpace(run.RewrittenTask)
	if taskForSkill == "" {
		taskForSkill = run.Task
	}
	nodeCopy.Skill = SelectSkill(taskForSkill, nodeCopy.Type, phase, nodeCopy.Skill)

	// A Loop Reviewer is a bounded judge, not a conversational worker. Keep its
	// provider-session bookkeeping fresh and disable external session reuse; the
	// retained serve process itself is still managed by the executor/runtime
	// manager when the node is configured with mode=serve.
	contextPolicy := run.ExecOptions.ContextPolicy
	reuseAgentSessions := run.ExecOptions.ReuseAgentSessions
	externalSessionID := ""
	// A retained Codex/Claude reviewer is still orchestrator-controlled, but
	// its provider session must persist across Loop iterations. All other loop
	// reviewers keep the historical fresh-session behavior.
	retainedReviewer := loopReview && (nodeCopy.Executor == ExecutorCodex || nodeCopy.Executor == ExecutorClaude) && strings.EqualFold(nodeCopy.Mode, "serve")
	if !loopReview {
		externalSessionID = "" // populated from the ProviderSession below
	} else if retainedReviewer {
		contextPolicy = "reuse"
		reuseAgentSessions = true
	} else {
		contextPolicy = "fresh"
		reuseAgentSessions = false
	}

	// Atomically find/create binding and ProviderSession (prevents concurrent race)
	binding, ps, err := s.FindOrCreateBindingAndProviderSession(sessionID, nodeID, nodeCopy, string(nodeCopy.Executor), runWorkspace(run), contextPolicy, reuseAgentSessions)
	if err != nil {
		if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("binding/provider session: %w", err), sessionID, iterationID); perr != nil {
			return perr
		}
		return fmt.Errorf("node %q binding/provider session: %w", nodeCopy.Label, err)
	}
	providerSessionID := ps.ID

	// Create attempt with iterationID
	attempt, err := s.CreateAttemptWithIteration(run.ID, nodeID, binding.ID, iterationID)
	if err != nil {
		if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("attempt: %w", err), sessionID, iterationID); perr != nil {
			return perr
		}
		return fmt.Errorf("node %q attempt: %w", nodeCopy.Label, err)
	}

	// Update attempt metadata
	if err := s.UpdateAttempt(attempt.ID, func(a *NodeAttempt) {
		a.Executor = string(nodeCopy.Executor)
		a.Model = nodeCopy.Model
		a.Mode = nodeCopy.Mode
		a.Agent = nodeCopy.Agent
		a.Skill = nodeCopy.Skill
		a.ProviderSessionID = providerSessionID
	}); err != nil {
		if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("attempt update: %w", err), sessionID, iterationID); perr != nil {
			return perr
		}
		return fmt.Errorf("node %q attempt update: %w", nodeCopy.Label, err)
	}

	// Gather input from THIS iteration's upstream attempts only
	input := s.gatherInputForIteration(pipe, run, iterationID, nodeID)
	if err := s.UpdateAttempt(attempt.ID, func(a *NodeAttempt) { a.Input = input }); err != nil {
		if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("attempt input update: %w", err), sessionID, iterationID); perr != nil {
			return perr
		}
		return fmt.Errorf("node %q attempt input: %w", nodeCopy.Label, err)
	}

	// Retained Codex reviewers deliberately reuse their ProviderSession Thread.
	// Other reviewer executors remain fresh to preserve their previous isolation.
	if (!loopReview || retainedReviewer) && providerSessionID != "" {
		if ps, ok := s.GetProviderSession(providerSessionID); ok {
			externalSessionID = ps.ExternalSessionID
		}
	}

	// Execute the node — pass context policy and session ID for Codex resume.
	// Reviewer-driven stall repair (L-53) wraps the call when the loop review
	// is enabled and a reviewer node exists; otherwise it degrades to the
	// plain execution below.
	outcome := s.execWithStallRepair(ctx, run, pipe, sessionID, iterationID, nodeID, &nodeCopy, input, contextPolicy, externalSessionID, loopReview, binding.ID, providerSessionID, 0)
	output, nodeStderr, realUsage, nodeRuntimeID, nodeEndpoint, execExternalSessionID, execErr := outcome.output, outcome.stderr, outcome.usage, outcome.runtimeID, outcome.endpoint, outcome.extSessionID, outcome.err
	// A restart repair records its result under the new attempt, not the
	// original one.
	attemptID := attempt.ID
	if outcome.attemptID != "" {
		attemptID = outcome.attemptID
	}

	// Reviewer gets at most one corrective turn when a provider exposes a tool
	// call as its final text. This preserves compatibility with providers that
	// need one final-format correction, while the hard single-retry cap prevents
	// the unbounded command/evidence replay observed in production.
	// Codex app-server runs exactly one Turn per orchestrator node attempt.
	// Keep the legacy one-shot correction for other reviewer executors, but do
	// not silently start a second retained Codex Turn on malformed output.
	if loopReview && !retainedReviewer && execErr == nil && !validLoopReviewOutput(output) && ctx.Err() == nil {
		retryInput := input + `

[系统纠正] 你上一条响应不是合法的 loop-review-v1 审查结果，可能只输出了工具调用参数。请不要再次调用工具，立即只输出一个纯 JSON 对象，严格包含 schemaVersion、decision、confidence、summary、blockingIssues、requiredChanges、nextTask、evidence。decision 只能是 pass、revise 或 blocked。`
		retryOutput, retryStderr, retryUsage, retryRuntimeID, retryEndpoint, retryExternalSessionID, retryErr := s.executeNodeWithLoopProtocolAtWorkspace(ctx, &nodeCopy, retryInput, contextPolicy, externalSessionID, true, true, runWorkspace(run))
		if retryRuntimeID != "" {
			nodeRuntimeID = retryRuntimeID
		}
		if retryEndpoint != "" {
			nodeEndpoint = retryEndpoint
		}
		if retryExternalSessionID != "" {
			execExternalSessionID = retryExternalSessionID
		}
		if strings.TrimSpace(retryStderr) != "" {
			nodeStderr = strings.TrimSpace(nodeStderr + "\n" + retryStderr)
		}
		if retryUsage != nil {
			realUsage = retryUsage
		}
		output = retryOutput
		execErr = retryErr
	}

	// A canceled/deadline-exceeded serve request must not leave its provider
	// process registered as running. Normal completion keeps the runtime for
	// reuse, including a Loop Reviewer configured with mode=serve. The terminal
	// Loop cleanup decides whether retained runtimes are released or stopped.
	runtimeStopped := false
	if nodeRuntimeID != "" && execErr != nil && (errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded)) {
		runtimeStopped = true
		_ = stopManagedRuntime(nodeCopy.Executor, nodeRuntimeID)
		s.UnregisterAgent(nodeRuntimeID)
	} else if nodeRuntimeID != "" && execErr != nil {
		s.UpdateAgentStatus(nodeRuntimeID, "error", execErr.Error())
	} else if nodeRuntimeID != "" {
		s.UpdateAgentStatus(nodeRuntimeID, "idle", "")
	}

	// Update attempt with results
	doneAt := time.Now().UTC().Format(time.RFC3339)
	if err := s.UpdateAttempt(attemptID, func(a *NodeAttempt) {
		a.Output = output
		a.Stderr = nodeStderr
		a.FinishedAt = doneAt
		if nodeRuntimeID != "" {
			a.RuntimeID = nodeRuntimeID
		}
		if realUsage != nil {
			a.TokenUsage = *realUsage
			a.TokenUsage.DurationMs = time.Since(mustParseTime(a.StartedAt)).Milliseconds()
		}
		if execErr != nil {
			if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
				a.Status = "interrupted"
			} else {
				a.Status = "failed"
			}
			a.Error = execErr.Error()
		} else if strings.TrimSpace(output) == "" {
			a.Status = "failed"
			a.Error = "agent completed without assistant output"
		} else {
			a.Status = "complete"
		}
	}); err != nil {
		if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("attempt result update: %w", err), sessionID, iterationID); perr != nil {
			return perr
		}
		return fmt.Errorf("node %q attempt result: %w", nodeCopy.Label, err)
	}

	// Update ProviderSession with Codex session ID if returned
	if execExternalSessionID != "" && providerSessionID != "" {
		if err := s.UpdateProviderSession(providerSessionID, func(ps *ProviderSession) {
			ps.ExternalSessionID = execExternalSessionID
			ps.Status = "active"
			ps.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
		}); err != nil {
			if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("provider session update: %w", err), sessionID, iterationID); perr != nil {
				return perr
			}
			return fmt.Errorf("node %q provider session update: %w", nodeCopy.Label, err)
		}
	}

	// Create RuntimeState — distinguish success from failure
	if nodeRuntimeID != "" {
		port := portFromEndpoint(nodeEndpoint)
		rtStatus := RuntimeReady
		var rtError string
		if runtimeStopped {
			rtStatus = RuntimeStopped
			if execErr != nil {
				rtError = execErr.Error()
			} else {
				rtError = "reviewer runtime stopped after bounded review"
			}
		} else if execErr != nil {
			rtStatus = RuntimeError
			rtError = execErr.Error()
		} else if strings.TrimSpace(output) == "" {
			rtStatus = RuntimeError
			rtError = "agent completed without assistant output"
		}
		rtState := RuntimeState{
			RuntimeID:      nodeRuntimeID,
			SessionID:      sessionID,
			AgentBindingID: binding.ID,
			NodeID:         nodeID,
			RunID:          run.ID,
			Executor:       string(nodeCopy.Executor),
			Model:          nodeCopy.Model,
			Endpoint:       nodeEndpoint,
			Port:           port,
			Status:         rtStatus,
			Error:          rtError,
			CreatedAt:      time.Now(),
			LastActiveAt:   time.Now(),
			CleanupPolicy:  CleanupRetained,
			AccessMode:     runtimeAccessMode(nodeCopy.Executor, nodeCopy.Mode),
		}
		applyLiveRuntimeState(&rtState)
		if err := s.CreateRuntimeState(rtState); err != nil {
			if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("runtime state: %w", err), sessionID, iterationID); perr != nil {
				return perr
			}
			return fmt.Errorf("node %q runtime state: %w", nodeCopy.Label, err)
		}
	}

	// Update binding and provider session
	if nodeRuntimeID != "" {
		currentRuntimeID := nodeRuntimeID
		if runtimeStopped {
			currentRuntimeID = ""
		}
		if err := s.UpdateBinding(binding.ID, func(b *AgentBinding) {
			b.CurrentRuntimeID = currentRuntimeID
		}); err != nil {
			if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("binding update: %w", err), sessionID, iterationID); perr != nil {
				return perr
			}
			return fmt.Errorf("node %q binding update: %w", nodeCopy.Label, err)
		}
		if providerSessionID != "" {
			if err := s.UpdateProviderSession(providerSessionID, func(ps *ProviderSession) {
				if runtimeStopped {
					ps.LastKnownRuntimeID = ""
					ps.LastKnownEndpoint = ""
				} else {
					ps.LastKnownRuntimeID = nodeRuntimeID
					ps.LastKnownEndpoint = nodeEndpoint
				}
			}); err != nil {
				if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("provider session update: %w", err), sessionID, iterationID); perr != nil {
					return perr
				}
				return fmt.Errorf("node %q provider session update: %w", nodeCopy.Label, err)
			}
		}
	}
	if err := s.UpdateBinding(binding.ID, func(b *AgentBinding) {
		b.LastRunID = run.ID
		b.LastAttemptID = attempt.ID
	}); err != nil {
		if perr := s.failNode(run, nodeID, nodeCopy.Label, fmt.Errorf("binding last run update: %w", err), sessionID, iterationID); perr != nil {
			return perr
		}
		return fmt.Errorf("node %q binding last run: %w", nodeCopy.Label, err)
	}

	// Update run state
	s.mu.Lock()
	runState := run.NodeStates[nodeID]
	runState.DoneAt = doneAt
	runState.Stderr = nodeStderr
	var nodeErr error
	if execErr != nil {
		if errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) {
			runState.Status = NodeInterrupted
			// Keep the low-level pipeline API's historical behavior: a direct
			// canceled execution is observable as failed. ExecuteLoop converts
			// parent cancellation to canceled, and internal timeouts to interrupted.
			if errors.Is(execErr, context.Canceled) && run.Status != "canceled" {
				run.Status = "failed"
				run.FinishedAt = doneAt
			}
		} else {
			runState.Status = NodeFailed
			run.Status = "failed"
			run.FinishedAt = doneAt
		}
		runState.Error = execErr.Error()
		run.Error = fmt.Sprintf("node %s execution stopped: %s", nodeCopy.Label, execErr)
		nodeErr = execErr
	} else if strings.TrimSpace(output) == "" {
		runState.Status = NodeFailed
		runState.Error = "agent completed without assistant output"
		run.Status = "failed"
		run.Error = fmt.Sprintf("node %s: empty output", nodeCopy.Label)
		run.FinishedAt = doneAt
		nodeErr = fmt.Errorf("node %s: empty output", nodeCopy.Label)
	} else {
		runState.Status = NodeComplete
		runState.Output = output
		runState.TokenUsage = TokenUsage{DurationMs: time.Since(mustParseTime(runState.StartedAt)).Milliseconds()}
		if realUsage != nil {
			runState.TokenUsage = *realUsage
		}
	}
	run.NodeStates[nodeID] = runState
	run.UpdatedAt = doneAt
	s.mu.Unlock()

	// Persist run state — strict: failure terminates loop
	if err := s.persistRun(run, s.getSessionID(run)); err != nil {
		return fmt.Errorf("node %q persist failure: %w", nodeCopy.Label, err)
	}
	return nodeErr
}

func validLoopReviewOutput(output string) bool {
	decision, err := ParseReviewDecision(output)
	if err != nil {
		return false
	}
	return ValidateReviewDecision(decision) == nil
}

func stopManagedRuntime(executor ExecutorType, runtimeID string) error {
	if runtimeID == "" {
		return nil
	}
	switch executor {
	case ExecutorMimo:
		return StopMimoRuntime(runtimeID)
	case ExecutorReasonix, "":
		return StopReasonixRuntime(runtimeID)
	case ExecutorCodex:
		return StopCodexRuntime(runtimeID)
	case ExecutorClaude:
		return StopClaudeRuntime(runtimeID)
	case ExecutorOpencode:
		return StopOpencodeRuntime(runtimeID)
	default:
		return nil
	}
}

// findReviewOutputForIteration finds the review node output for a specific iteration.
func (s *Store) findReviewOutputForIteration(run *PipelineRun, iterationID, reviewNodeID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, attID := range run.NodeAttemptIDs {
		if att, ok := s.attempts[attID]; ok &&
			att.IterationID == iterationID &&
			att.NodeID == reviewNodeID &&
			att.Status == "complete" {
			return att.Output
		}
	}

	// Fallback: check NodeStates (only for current iteration)
	if state, ok := run.NodeStates[reviewNodeID]; ok && state.Output != "" {
		return state.Output
	}

	return ""
}

// findReviewAttemptIDForIteration finds the attempt ID for the review node in a specific iteration.
func (s *Store) findReviewAttemptIDForIteration(run *PipelineRun, iterationID, reviewNodeID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, attID := range run.NodeAttemptIDs {
		if att, ok := s.attempts[attID]; ok &&
			att.IterationID == iterationID &&
			att.NodeID == reviewNodeID {
			return att.ID
		}
	}
	return ""
}

// latestSuccessfulNodeOutput returns the most recent successful attempt for a
// node at or before the requested iteration. It is used only for architect
// outputs, which are deliberately reused by later Loop iterations.
func (s *Store) latestSuccessfulNodeOutput(run *PipelineRun, nodeID string, maxIteration int) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	latestIteration := 0
	latestOutput := ""
	for _, attID := range run.NodeAttemptIDs {
		att, ok := s.attempts[attID]
		if !ok || att.NodeID != nodeID || att.Status != "complete" {
			continue
		}
		it, ok := s.iterations[att.IterationID]
		if !ok || it.Number > maxIteration || it.Number < latestIteration {
			continue
		}
		if it.Number == latestIteration && latestOutput != "" {
			continue
		}
		latestIteration = it.Number
		latestOutput = att.Output
	}
	return latestOutput
}

// gatherInputForIteration collects output from upstream nodes in the same iteration.
func (s *Store) gatherInputForIteration(pipe *Pipeline, run *PipelineRun, iterationID, nodeID string) string {
	node := findNode(pipe, nodeID)
	upstream := upstreamEdges(pipe, nodeID)

	// Build the complete hand-off while holding the store lock. The loop is an
	// orchestrator-level cycle, not a canvas cycle: the next iteration must see
	// the previous Review even when the user intentionally drew only
	// Architect -> Executor -> Reviewer. In particular, do not make the Mimo
	// Reviewer/runtime conversation responsible for carrying this state.
	s.mu.Lock()
	defer s.mu.Unlock()

	var parts []string
	if node != nil && node.RoleDesc != "" {
		parts = append(parts, "## 节点职责 / Node duty\n"+node.RoleDesc)
	}
	if hint := assistHint(node); hint != "" {
		parts = append(parts, hint)
	}
	if run.Task != "" {
		parts = append(parts, "## 当前轮任务 / Current round task\n"+run.Task)
	}
	current, hasCurrent := s.iterations[iterationID]
	if hasCurrent {
		parts = append(parts, fmt.Sprintf("## Loop 轮次 / Loop round\n第 %d 轮 (Round %d)", current.Number, current.Number))
	}

	included := make(map[string]bool, len(upstream))
	if len(upstream) > 0 {
		parts = append(parts, "## 上游节点输出 / Upstream output")
	}
	// Read current-iteration attempts first. If an architect was intentionally
	// skipped after iteration 1, fall back to its latest successful output.
	for _, e := range upstream {
		included[e.FromID] = true
		var latestOutput string
		var latestIteration int
		for _, attID := range run.NodeAttemptIDs {
			if att, ok := s.attempts[attID]; ok &&
				att.IterationID == iterationID &&
				att.NodeID == e.FromID &&
				att.Status == "complete" {
				latestOutput = att.Output
			}
		}
		if latestOutput == "" {
			if fromNode := findNode(pipe, e.FromID); fromNode != nil && fromNode.Type == NodeArchitect {
				for _, attID := range run.NodeAttemptIDs {
					att, ok := s.attempts[attID]
					if !ok || att.NodeID != e.FromID || att.Status != "complete" {
						continue
					}
					iter, ok := s.iterations[att.IterationID]
					if ok && iter.Number > latestIteration {
						latestIteration = iter.Number
						latestOutput = att.Output
					}
				}
			}
		}
		if latestOutput != "" {
			label := e.FromID
			if fromNode := findNode(pipe, e.FromID); fromNode != nil && fromNode.Label != "" {
				label = fromNode.Label
			}
			parts = append(parts, fmt.Sprintf("### %s\n%s", label, latestOutput))
		}
	}

	// Explicitly carry the immediately preceding Review to every non-reviewer
	// node in later iterations. `NextTask` is intentionally kept as the current
	// task above, but the full JSON is also required: it contains blockingIssues,
	// requiredChanges, and evidence that an Executor needs to decide what to
	// change. This works for Mimo and Reasonix alike and does not require a
	// reviewer -> executor edge (which would make the persisted canvas a DAG
	// cycle and prevent topological execution).
	if hasCurrent && current.Number > 1 && node != nil && node.Type != NodeReviewer {
		previousNumber := current.Number - 1
		var previous *LoopIteration
		for _, candidateID := range run.IterationIDs {
			candidate, ok := s.iterations[candidateID]
			if !ok || candidate.Number != previousNumber {
				continue
			}
			previous = candidate
			break
		}
		if previous != nil {
			reviewOutput := ""
			if previous.ReviewAttemptID != "" {
				if att, ok := s.attempts[previous.ReviewAttemptID]; ok && att.Status == "complete" {
					reviewOutput = att.Output
				}
			}
			if reviewOutput == "" {
				for _, attID := range run.NodeAttemptIDs {
					att, ok := s.attempts[attID]
					if !ok || att.IterationID != previous.ID || att.NodeID != run.LoopConfig.ReviewNodeID || att.Status != "complete" {
						continue
					}
					reviewOutput = att.Output
					break
				}
			}
			if strings.TrimSpace(reviewOutput) != "" {
				parts = append(parts, "## 上一轮审查结论（必须据此处理）\n"+reviewOutput)
			}
		}
	}

	// A reviewer must receive the actual current-round evidence even when a
	// generated/manual canvas omitted one of the expected edges. This is the
	// loop hand-off boundary: the reviewer never relies on a provider's hidden
	// conversation history or on Mimo's retained runtime to discover what the
	// executor did. Include every completed current-iteration node once.
	if node != nil && node.Type == NodeReviewer {
		var evidence []string
		var architectNode *AgentNode
		for _, candidate := range pipe.Nodes {
			if candidate.ID == nodeID || included[candidate.ID] {
				continue
			}
			// The architect's plan is persisted as a design document and passed
			// to the reviewer as a path reference below, never copied in full
			// (architect output is long; the reviewer should read the file).
			if candidate.Type == NodeArchitect {
				if architectNode == nil {
					architectNode = &candidate
				}
				continue
			}
			for _, attID := range run.NodeAttemptIDs {
				att, ok := s.attempts[attID]
				if !ok || att.IterationID != iterationID || att.NodeID != candidate.ID || att.Status != "complete" || strings.TrimSpace(att.Output) == "" {
					continue
				}
				label := candidate.ID
				if candidate.Label != "" {
					label = candidate.Label
				}
				evidence = append(evidence, fmt.Sprintf("### %s\n%s", label, att.Output))
				break
			}
		}
		if len(evidence) > 0 {
			parts = append(parts, "## 本轮执行结果（供审查）\n"+strings.Join(evidence, "\n\n"))
		}
		// Reference the architect's overall plan so the reviewer decides
		// against the full task plan, not just this iteration's code.
		if architectNode != nil {
			planPath, exists := architectPlanPath(runWorkspace(run), architectNode.ID)
			if exists && planPath != "" {
				parts = append(parts, "## 架构师设计文档（总体计划，必须先阅读再决策）\n"+
					"文件路径："+planPath+"\n"+
					"在作出 pass/revise 决策前，请用只读工具阅读该设计文档，并对照原始任务与总体计划判断本轮执行是否覆盖计划中的任务项；未覆盖或不完整必须 revise 并列出未覆盖项。")
			} else {
				parts = append(parts, "## 架构师设计文档\n未找到总体计划文件（"+planPath+"），请以原始任务为准。")
			}
		}
	}

	if len(parts) == 0 {
		if node != nil && node.RoleDesc != "" {
			return fmt.Sprintf("你是一个%s。你的任务是：%s。请开始工作。", node.Label, node.RoleDesc)
		}
		if node != nil {
			return fmt.Sprintf("请完成你的角色任务。角色：%s", node.Label)
		}
		return "请完成你的角色任务。"
	}
	return strings.Join(parts, "\n\n---\n\n")
}

// buildRevisionTask constructs a task description from required changes.
func buildRevisionTask(changes []string) string {
	if len(changes) == 0 {
		return "请根据审查意见修改"
	}
	var parts []string
	parts = append(parts, "请根据以下审查意见进行修改：")
	for i, c := range changes {
		parts = append(parts, fmt.Sprintf("%d. %s", i+1, c))
	}
	return strings.Join(parts, "\n")
}

// nextIterationID generates a unique iteration ID.
func (s *Store) nextIterationID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	return fmt.Sprintf("iter_%d_%d", time.Now().UnixMilli(), s.nextID)
}

// persistRun saves the run state to disk. Creates a deep copy so concurrent
// readers/writers of the live run pointer are not affected by the serialize duration.
func (s *Store) persistRun(run *PipelineRun, sessionID string) error {
	// Snapshot while holding the store lock. The live run is mutated by the
	// loop goroutine between persistence points, and encoding it without the
	// lock can race on NodeStates/iteration slices even though the JSON write
	// itself happens asynchronously from the executor.
	s.mu.RLock()
	runCopy := clonePipelineRun(run)
	s.mu.RUnlock()

	runDir := filepath.Join(sessionDir(sessionID), "runs")
	return saveSessionJSON(runDir, runCopy.ID+".json", &runCopy)
}

// failNode marks a node as failed and updates the run. Returns persistence error if save fails.
func (s *Store) failNode(run *PipelineRun, nodeID, nodeLabel string, err error, sessionID, iterationID string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	s.mu.Lock()
	st := run.NodeStates[nodeID]
	st.Status = NodeFailed
	st.Error = err.Error()
	st.DoneAt = now
	run.NodeStates[nodeID] = st
	run.Status = "failed"
	run.Error = fmt.Sprintf("node %s failed: %s", nodeLabel, err)
	run.FinishedAt = now
	run.UpdatedAt = now
	s.mu.Unlock()
	if perr := s.persistRun(run, sessionID); perr != nil {
		return fmt.Errorf("node %q failed: %w; persist failed: %v", nodeLabel, err, perr)
	}
	return nil
}

// getSessionID extracts session ID from run.
func (s *Store) getSessionID(run *PipelineRun) string {
	return run.SessionID
}

// failLoopPersistence wraps a persistence error with the business error context.
func failLoopPersistence(persistErr error, businessErr error) error {
	return fmt.Errorf("loop termination: %w; persistence failure: %v", businessErr, persistErr)
}
