package orchestrator

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"
)

// ancestorNodes returns every node that has a directed path into nodeID
// (transitive upstream), excluding nodeID itself.
func ancestorNodes(pipe *Pipeline, nodeID string) map[string]bool {
	incoming := map[string][]string{}
	for _, e := range pipe.Edges {
		incoming[e.ToID] = append(incoming[e.ToID], e.FromID)
	}
	seen := map[string]bool{}
	stack := append([]string(nil), incoming[nodeID]...)
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if seen[n] {
			continue
		}
		seen[n] = true
		stack = append(stack, incoming[n]...)
	}
	return seen
}

// SeedRetryRun copies the completed attempts and node states of the source
// run's upstream nodes into the destination run, so a node-level retry starts
// at the failed node without re-running — or re-paying tokens for — the
// successful pipeline prefix. gatherInputV2 reads upstream outputs from the
// destination run's own attempts, so seeded inputs flow downstream naturally.
//
// Only nodes strictly upstream (transitive) of retryNodeID are seeded; peers
// and downstream nodes always re-execute. Callers must hold no store lock.
func (s *Store) SeedRetryRun(dst, src *PipelineRun, pipe *Pipeline, retryNodeID string) error {
	if findNode(pipe, retryNodeID) == nil {
		return fmt.Errorf("retry node %q not found in pipeline", retryNodeID)
	}
	ancestors := ancestorNodes(pipe, retryNodeID)

	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UTC().Format(time.RFC3339)
	seeded := map[string]bool{}
	for _, attID := range src.NodeAttemptIDs {
		att, ok := s.attempts[attID]
		if !ok || att.Status != "complete" || !ancestors[att.NodeID] {
			continue
		}
		clone := *att
		clone.ID = fmt.Sprintf("att_%d_%d", time.Now().UnixMilli(), s.nextID)
		s.nextID++
		clone.RunID = dst.ID
		clone.IterationID = ""
		clone.StartedAt = now
		clone.FinishedAt = now
		s.attempts[clone.ID] = &clone
		dst.NodeAttemptIDs = append(dst.NodeAttemptIDs, clone.ID)
		if st, ok := src.NodeStates[att.NodeID]; ok {
			copied := st
			copied.Status = NodeComplete
			copied.Error = ""
			dst.NodeStates[att.NodeID] = copied
		} else {
			dst.NodeStates[att.NodeID] = RunState{Status: NodeComplete, Output: att.Output, StartedAt: now, DoneAt: now}
		}
		seeded[att.NodeID] = true
	}
	dst.SeededNodes = seeded
	return nil
}

// FirstBrokenNodeID returns the first failed/interrupted node of the run in
// topological order — the natural retry entry point for a "just retry it"
// button. Empty string when every node completed.
func (s *Store) FirstBrokenNodeID(runID string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[runID]
	if !ok {
		return ""
	}
	rev, ok := s.pipelineRevisions[run.PipelineRevisionID]
	if !ok {
		return ""
	}
	broken := map[string]bool{}
	for id, st := range run.NodeStates {
		if st.Status == NodeFailed || st.Status == "interrupted" {
			broken[id] = true
		}
	}
	for _, level := range topologicalLevels(&Pipeline{Nodes: rev.Nodes, Edges: rev.Edges}) {
		for _, nodeID := range level {
			if broken[nodeID] {
				return nodeID
			}
		}
	}
	return ""
}

// ResetNodeSession detaches every active binding's ProviderSession for one
// node, so the next execution starts a brand-new provider conversation. This
// is the operator's manual escape hatch: memory continuity is the default,
// forgetting is a deliberate act. Returns how many sessions were detached.
func (s *Store) ResetNodeSession(sessionID, nodeID string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return 0, fmt.Errorf("session %q not found", sessionID)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	count := 0
	for _, id := range sess.AgentBindingIDs {
		b, ok := s.bindings[id]
		if !ok || b.NodeID != nodeID || b.Status != "active" || b.ProviderSessionID == "" {
			continue
		}
		if ps, ok := s.providerSessions[b.ProviderSessionID]; ok {
			ps.Status = "reset"
			ps.UpdatedAt = now
			psDir := filepath.Join(sessionDir(sessionID), "provider-sessions")
			if err := saveSessionJSON(psDir, ps.ID+".json", ps); err != nil {
				return count, fmt.Errorf("persist provider session reset: %w", err)
			}
		}
		b.ProviderSessionID = ""
		b.UpdatedAt = now
		bDir := filepath.Join(sessionDir(sessionID), "agents")
		if err := saveSessionJSON(bDir, b.ID+".json", b); err != nil {
			return count, fmt.Errorf("persist binding after reset: %w", err)
		}
		count++
	}
	return count, nil
}

// NodeSessionMemory describes what an operator sees in the node detail view:
// which provider conversation this node will continue and how many completed
// executions already feed it.
type NodeSessionMemory struct {
	BindingID         string `json:"bindingID,omitempty"`
	ProviderSessionID string `json:"providerSessionID,omitempty"`
	ExternalSessionID string `json:"externalSessionID,omitempty"`
	CompletedRuns     int    `json:"completedRuns"`
}

// NodeSessionMemory reports the live conversation anchor for a node. Empty
// ExternalSessionID means the next execution will start a fresh conversation
// (no provider turn has completed yet).
func (s *Store) NodeSessionMemory(sessionID, nodeID string) NodeSessionMemory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := NodeSessionMemory{}
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return out
	}
	for _, id := range sess.AgentBindingIDs {
		b, ok := s.bindings[id]
		if !ok || b.NodeID != nodeID || b.Status != "active" {
			continue
		}
		out.BindingID = b.ID
		if ps, ok := s.providerSessions[b.ProviderSessionID]; ok {
			out.ProviderSessionID = ps.ID
			out.ExternalSessionID = ps.ExternalSessionID
		}
		break
	}
	runIDs := make(map[string]bool, len(sess.RunIDs))
	for _, rid := range sess.RunIDs {
		runIDs[rid] = true
	}
	for _, att := range s.attempts {
		if att.NodeID == nodeID && att.Status == "complete" && runIDs[att.RunID] {
			out.CompletedRuns++
		}
	}
	return out
}

const continuationNoteHeader = "## 人工补充指令（暂停期间由操作者给出）\n"

// AppendContinuationNote records one piece of operator guidance on the run.
// Allowed in any run state: notes captured after a stop/cancel are exactly
// the ones a later continue must carry.
func (s *Store) AppendContinuationNote(runID, text string) error {
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("note text is empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[runID]
	if !ok {
		return fmt.Errorf("run %q not found", runID)
	}
	run.ContinuationNotes = append(run.ContinuationNotes, ContinuationNote{
		Text:      text,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	})
	run.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if err := saveSessionJSON(filepath.Join(sessionDir(run.SessionID), "runs"), run.ID+".json", run); err != nil {
		return fmt.Errorf("persist note: %w", err)
	}
	return nil
}

// takePendingContinuationNotes consumes every pending note and returns the
// joined text for injection into one executor-node input. Must be called with
// s.mu held; persists the consumption marker.
func (s *Store) takePendingContinuationNotesLocked(run *PipelineRun, nodeID string) string {
	var texts []string
	now := time.Now().UTC().Format(time.RFC3339)
	for i := range run.ContinuationNotes {
		n := &run.ContinuationNotes[i]
		if n.Consumed {
			continue
		}
		n.Consumed = true
		n.ConsumedByNodeID = nodeID
		texts = append(texts, n.Text)
	}
	if len(texts) == 0 {
		return ""
	}
	run.UpdatedAt = now
	if err := saveSessionJSON(filepath.Join(sessionDir(run.SessionID), "runs"), run.ID+".json", run); err != nil {
		// Consumption markers are advisory; losing the persist must not lose
		// the injected text itself.
		return strings.Join(texts, "\n\n")
	}
	return strings.Join(texts, "\n\n")
}

// MovePendingContinuationNotes transfers unconsumed notes from the source run
// to the destination (a retry child run), marking them consumed at the source
// so exactly one run ever injects them.
func (s *Store) MovePendingContinuationNotes(src, dst *PipelineRun) int {
	moved := 0
	now := time.Now().UTC().Format(time.RFC3339)
	for i := range src.ContinuationNotes {
		if src.ContinuationNotes[i].Consumed {
			continue
		}
		dst.ContinuationNotes = append(dst.ContinuationNotes, ContinuationNote{
			Text:      src.ContinuationNotes[i].Text,
			CreatedAt: src.ContinuationNotes[i].CreatedAt,
		})
		src.ContinuationNotes[i].Consumed = true
		src.ContinuationNotes[i].ConsumedByNodeID = "moved:" + dst.ID
		moved++
	}
	if moved > 0 {
		src.UpdatedAt = now
		dst.UpdatedAt = now
		if err := saveSessionJSON(filepath.Join(sessionDir(src.SessionID), "runs"), src.ID+".json", src); err != nil {
			// Source persistence failure must not block the retry; the child
			// already carries the notes.
			return moved
		}
		if err := saveSessionJSON(filepath.Join(sessionDir(dst.SessionID), "runs"), dst.ID+".json", dst); err != nil {
			return moved
		}
	}
	return moved
}
