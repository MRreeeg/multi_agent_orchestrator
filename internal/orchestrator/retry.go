package orchestrator

import (
	"fmt"
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
