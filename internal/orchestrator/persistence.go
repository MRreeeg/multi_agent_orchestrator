package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// ── Root directories ──

// orchestratorRoot returns the root directory for orchestrator data. An
// explicit directory makes data survive binary rebuilds and different launch
// shells/users; the default remains the historical location for compatibility.
func orchestratorRoot() string {
	if root := strings.TrimSpace(os.Getenv("REASONIX_ORCHESTRATOR_DATA_DIR")); root != "" {
		return filepath.Clean(root)
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "share", "mimocode", "orchestrator")
}

// DataRoot exposes the resolved persistence directory for startup diagnostics
// and integration tests without exposing the internal layout helpers.
func DataRoot() string { return orchestratorRoot() }

// sessionDir returns the directory for a specific session.
func sessionDir(sessionID string) string {
	return filepath.Join(orchestratorRoot(), "sessions", sessionID)
}

// ── Legacy storeData (for migration) ──

type storeData struct {
	Pipelines map[string]*Pipeline    `json:"pipelines"`
	Runs      map[string]*PipelineRun `json:"runs"`
}

// ── New storeData ──

// orchStoreData is the on-disk format for the orchestrator index.
type orchStoreData struct {
	Sessions map[string]*OrchestrationSession `json:"sessions"`
}

// ── Legacy Save/Load (kept for migration) ──

// Save writes the store to a JSON file (legacy format).
func (s *Store) Save(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	data := storeData{
		Pipelines: s.pipelines,
		Runs:      s.runs,
	}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	return atomicWriteFile(path, b, 0644)
}

// Load reads the store from a JSON file (legacy format).
func (s *Store) Load(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var data storeData
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if data.Pipelines != nil {
		s.pipelines = data.Pipelines
	}
	if data.Runs != nil {
		s.runs = data.Runs
	}
	return nil
}

// ── Legacy SessionState (kept for backward compat) ──

func sessionStateDir() string {
	return filepath.Join(orchestratorRoot(), "sessions")
}

// SaveSessionState saves the orchestrator state for a session (legacy format).
func (s *Store) SaveSessionState(sessionID string, state SessionState) error {
	dir := sessionStateDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	path := filepath.Join(dir, fmt.Sprintf("%s.json", sessionID))
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(path, b, 0644)
}

// LoadSessionState loads the orchestrator state for a session (legacy format).
func (s *Store) LoadSessionState(sessionID string) (*SessionState, error) {
	path := filepath.Join(sessionStateDir(), fmt.Sprintf("%s.json", sessionID))
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state SessionState
	if err := json.Unmarshal(b, &state); err != nil {
		return nil, err
	}
	for i := range state.Nodes {
		if state.Nodes[i].Executor == "" {
			state.Nodes[i].Executor = ExecutorReasonix
		}
		if state.Nodes[i].Mode == "" {
			state.Nodes[i].Mode = "serve"
		}
	}
	return &state, nil
}

// ── New Directory-Based Persistence ──

// saveIndex writes the session index to disk.
func (s *Store) saveIndex() error {
	root := orchestratorRoot()
	if err := os.MkdirAll(root, 0755); err != nil {
		return err
	}
	data := orchStoreData{Sessions: s.orchSessions}
	b, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(root, "index.json"), b, 0644)
}

// loadIndex reads the session index from disk.
func (s *Store) loadIndex() error {
	path := filepath.Join(orchestratorRoot(), "index.json")
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var data orchStoreData
	if err := json.Unmarshal(b, &data); err != nil {
		return err
	}
	if data.Sessions != nil {
		s.orchSessions = data.Sessions
	}
	return nil
}

// saveSessionJSON writes a session entity to its directory.
func saveSessionJSON(dir, filename string, v any) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return atomicWriteFile(filepath.Join(dir, filename), b, 0644)
}

// atomicWriteFile writes beside the destination and renames only after the
// complete JSON document is on disk. This prevents a rebuild/process exit from
// leaving a truncated index or session file that looks like history rollback.
func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".orchestrator-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(perm); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		// Windows can reject rename-over-existing. Remove only the exact target
		// and retry; the temp file is still complete at this point.
		if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
			return err
		}
		return os.Rename(tmpName, path)
	}
	return nil
}

// loadSessionJSON reads a session entity from its directory.
func loadSessionJSON(dir, filename string, v any) error {
	path := filepath.Join(dir, filename)
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}

// listSessionFiles lists JSON files in a directory, returning filenames sorted by name.
func listSessionFiles(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".json") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files
}

// ── OrchestrationSession CRUD ──

// CreateOrchSession creates a new OrchestrationSession.
func (s *Store) CreateOrchSession(title, workspace string) (*OrchestrationSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	nowTS := time.Now().UTC()
	now := nowTS.Format(time.RFC3339)
	id := fmt.Sprintf("orch_%d_%d", nowTS.UnixMilli(), s.nextID)
	s.nextID++

	sess := &OrchestrationSession{
		ID:        id,
		Title:     title,
		Workspace: workspace,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}
	s.orchSessions[id] = sess

	// Persist to disk
	dir := sessionDir(id)
	if err := saveSessionJSON(dir, "session.json", sess); err != nil {
		return nil, err
	}
	if err := s.saveIndex(); err != nil {
		return nil, err
	}
	return sess, nil
}

// GetOrchSession returns an OrchestrationSession by ID.
func (s *Store) GetOrchSession(id string) (*OrchestrationSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.orchSessions[id]
	return sess, ok
}

// FindOrchSessionByNativePath returns the orchestration session bound to a
// native provider transcript. The path is the stable identity across browser
// reloads; localStorage is only a client-side cache.
func (s *Store) FindOrchSessionByNativePath(path string) (*OrchestrationSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, sess := range s.orchSessions {
		if path != "" && sess.NativeSessionPath == path {
			return sess, true
		}
	}
	return nil, false
}

// ListOrchSessionSummaries returns only scalar fields needed by the history
// list. Conversations, revisions and run arrays can be very large and must not
// be serialized for every history refresh.
func (s *Store) ListOrchSessionSummaries() []OrchestrationSessionSummary {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]OrchestrationSessionSummary, 0, len(s.orchSessions))
	for _, sess := range s.orchSessions {
		if sess == nil || strings.EqualFold(strings.TrimSpace(sess.Status), "archived") {
			continue
		}
		out = append(out, OrchestrationSessionSummary{
			ID:                sess.ID,
			Title:             sess.Title,
			Workspace:         sess.Workspace,
			Status:            sess.Status,
			CreatedAt:         sess.CreatedAt,
			UpdatedAt:         sess.UpdatedAt,
			NativeSessionPath: sess.NativeSessionPath,
			NativeSessionName: sess.NativeSessionName,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt > out[j].CreatedAt
		}
		// Timestamps are persisted at second precision. The monotonic ID
		// suffix keeps same-second sessions deterministic and newest-first.
		return out[i].ID > out[j].ID
	})
	return out
}

// ListOrchSessions returns all non-archived sessions, newest first.
//
// Archived sessions remain in the index so that the soft-delete operation is
// durable and recoverable, but they are intentionally hidden from the normal
// history list. Callers that need to inspect an archived session can still use
// GetOrchSession with its ID.
func (s *Store) ListOrchSessions() []OrchestrationSession {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]OrchestrationSession, 0, len(s.orchSessions))
	for _, sess := range s.orchSessions {
		if sess == nil || strings.EqualFold(strings.TrimSpace(sess.Status), "archived") {
			continue
		}
		out = append(out, *sess)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].UpdatedAt != out[j].UpdatedAt {
			return out[i].UpdatedAt > out[j].UpdatedAt
		}
		if out[i].CreatedAt != out[j].CreatedAt {
			return out[i].CreatedAt > out[j].CreatedAt
		}
		return out[i].ID > out[j].ID
	})
	return out
}

// UpdateOrchSession updates a session via a mutation function.
func (s *Store) UpdateOrchSession(id string, fn func(*OrchestrationSession)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.orchSessions[id]
	if !ok {
		return fmt.Errorf("session %q not found", id)
	}
	fn(sess)
	sess.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	dir := sessionDir(id)
	if err := saveSessionJSON(dir, "session.json", sess); err != nil {
		return err
	}
	return s.saveIndex()
}

// DeleteOrchSession archives a session (soft delete).
func (s *Store) DeleteOrchSession(id string) error {
	return s.UpdateOrchSession(id, func(sess *OrchestrationSession) {
		sess.Status = "archived"
	})
}

// ── PipelineRevision CRUD ──

// CreatePipelineRevision creates a new pipeline revision within a session.
func (s *Store) CreatePipelineRevision(sessionID string, rev PipelineRevision) (*PipelineRevision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("pipe_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	rev.ID = id
	rev.SessionID = sessionID
	rev.Version = len(sess.PipelineRevisionIDs) + 1
	rev.CreatedAt = now
	rev.UpdatedAt = now
	if rev.Status == "" {
		rev.Status = "active"
	}
	// Normalize compatible fixed-mode data when a revision is created.
	// Invalid direct Store calls remain readable for legacy callers; the HTTP
	// create/update paths validate before calling the persistence method.
	if rev.LoopConfig.Enabled {
		if normalized, err := NormalizeLoopConfig(&rev.LoopConfig); err == nil {
			rev.LoopConfig = normalized
		}
	}

	s.pipelineRevisions[id] = &rev
	sess.PipelineRevisionIDs = append(sess.PipelineRevisionIDs, id)
	sess.CurrentPipelineID = id
	sess.UpdatedAt = now

	// Persist revision
	dir := filepath.Join(sessionDir(sessionID), "pipelines")
	if err := saveSessionJSON(dir, id+".json", &rev); err != nil {
		return nil, err
	}
	if err := saveSessionJSON(sessionDir(sessionID), "session.json", sess); err != nil {
		return nil, err
	}
	if err := s.saveIndex(); err != nil {
		return nil, err
	}
	return &rev, nil
}

// GetPipelineRevision returns a pipeline revision by ID.
func (s *Store) GetPipelineRevision(id string) (*PipelineRevision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rev, ok := s.pipelineRevisions[id]
	return rev, ok
}

// GetCurrentRevision returns the current pipeline revision for a session.
func (s *Store) GetCurrentRevision(sessionID string) (*PipelineRevision, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.orchSessions[sessionID]
	if !ok || sess.CurrentPipelineID == "" {
		return nil, false
	}
	rev, ok := s.pipelineRevisions[sess.CurrentPipelineID]
	return rev, ok
}

// ListPipelineRevisions returns all revisions for a session, newest first.
func (s *Store) ListPipelineRevisions(sessionID string) []PipelineRevision {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil
	}
	var out []PipelineRevision
	for _, id := range sess.PipelineRevisionIDs {
		if rev, ok := s.pipelineRevisions[id]; ok {
			out = append(out, *rev)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Version > out[j].Version
	})
	return out
}

// UpdatePipelineRevision updates nodes/edges of an existing revision, creating
// a new version if execution-relevant content changed.
func (s *Store) UpdatePipelineRevision(sessionID string, nodes []AgentNode, edges []Edge, source string) (*PipelineRevision, error) {
	// Preserve the existing loop configuration for callers of the legacy
	// nodes/edges-only API. The complete API passes an explicit config.
	var cfg *LoopConfig
	if sess, ok := s.GetOrchSession(sessionID); ok && sess.CurrentPipelineID != "" {
		if rev, ok := s.GetPipelineRevision(sess.CurrentPipelineID); ok {
			copied := rev.LoopConfig
			cfg = &copied
		}
	}
	return s.UpdatePipelineRevisionWithLoopConfig(sessionID, "", nodes, edges, source, cfg)
}

// UpdatePipelineRevisionWithLoopConfig atomically validates and persists a
// pipeline revision together with its LoopConfig. Validation happens before
// any revision or session state is changed, so a rejected request cannot switch
// the session to a partially saved revision.
func (s *Store) UpdatePipelineRevisionWithLoopConfig(sessionID, name string, nodes []AgentNode, edges []Edge, source string, loopConfig *LoopConfig) (*PipelineRevision, error) {
	return s.updatePipelineRevisionWithLoopConfig(sessionID, "", name, nodes, edges, source, loopConfig)
}

// UpdatePipelineRevisionWithLoopConfigIfCurrent is the optimistic-concurrency
// variant used by the browser. expectedRevisionID is checked while holding the
// store lock, so a stale page cannot write its old canvas (and, more
// importantly, its old LoopConfig) over a newer revision.
func (s *Store) UpdatePipelineRevisionWithLoopConfigIfCurrent(sessionID, expectedRevisionID, name string, nodes []AgentNode, edges []Edge, source string, loopConfig *LoopConfig) (*PipelineRevision, error) {
	return s.updatePipelineRevisionWithLoopConfig(sessionID, expectedRevisionID, name, nodes, edges, source, loopConfig)
}

func (s *Store) updatePipelineRevisionWithLoopConfig(sessionID, expectedRevisionID, name string, nodes []AgentNode, edges []Edge, source string, loopConfig *LoopConfig) (*PipelineRevision, error) {
	cfg := LoopConfig{}
	if loopConfig != nil {
		cfg = *loopConfig
	}
	if err := ValidateLoopConfig(&cfg, nodes); err != nil {
		return nil, err
	}
	if err := ValidateLoopTargets(&cfg, nodes); err != nil {
		return nil, err
	}
	if normalized, err := NormalizeLoopConfig(&cfg); err != nil {
		return nil, err
	} else {
		cfg = normalized
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	var oldRev *PipelineRevision
	if sess.CurrentPipelineID != "" {
		oldRev = s.pipelineRevisions[sess.CurrentPipelineID]
		if oldRev != nil && oldRev.SessionID != sessionID {
			return nil, fmt.Errorf("current pipeline revision %q does not belong to session %q", oldRev.ID, sessionID)
		}
	}
	if expectedRevisionID != "" && (oldRev == nil || oldRev.ID != expectedRevisionID) {
		actual := ""
		if oldRev != nil {
			actual = oldRev.ID
		}
		return nil, fmt.Errorf("pipeline revision conflict: expected %q, current is %q", expectedRevisionID, actual)
	}
	contentChanged := oldRev == nil || pipelineContentChanged(oldRev, nodes, edges) || !loopConfigEqual(oldRev.LoopConfig, cfg)

	if !contentChanged {
		now := time.Now().UTC().Format(time.RFC3339)
		oldRev.Nodes = nodes
		oldRev.Edges = edges
		oldRev.LoopConfig = cfg
		if name != "" {
			oldRev.Name = name
		}
		if source != "" {
			oldRev.Source = source
		}
		oldRev.UpdatedAt = now
		dir := filepath.Join(sessionDir(sessionID), "pipelines")
		if err := saveSessionJSON(dir, oldRev.ID+".json", oldRev); err != nil {
			return nil, fmt.Errorf("persist pipeline revision: %w", err)
		}
		return oldRev, nil
	}

	now := time.Now().UTC().Format(time.RFC3339)
	if name == "" {
		name = "Pipeline"
	}
	id := fmt.Sprintf("pipe_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++
	rev := &PipelineRevision{
		ID:                id,
		SessionID:         sessionID,
		Version:           len(sess.PipelineRevisionIDs) + 1,
		Name:              name,
		Nodes:             nodes,
		Edges:             edges,
		Source:            source,
		BasedOnRevisionID: sess.CurrentPipelineID,
		LoopConfig:        cfg,
		CreatedAt:         now,
		UpdatedAt:         now,
		Status:            "active",
	}

	dir := filepath.Join(sessionDir(sessionID), "pipelines")
	// Write the new revision before changing the current pointer. The session
	// pointer is only changed after the complete revision document is durable.
	if err := saveSessionJSON(dir, id+".json", rev); err != nil {
		return nil, err
	}
	if oldRev != nil {
		oldStatus := oldRev.Status
		oldRev.Status = "superseded"
		if err := saveSessionJSON(dir, oldRev.ID+".json", oldRev); err != nil {
			oldRev.Status = oldStatus
			return nil, fmt.Errorf("persist superseded revision: %w", err)
		}
	}

	s.pipelineRevisions[id] = rev
	sess.PipelineRevisionIDs = append(sess.PipelineRevisionIDs, id)
	sess.CurrentPipelineID = id
	sess.UpdatedAt = now
	if err := saveSessionJSON(sessionDir(sessionID), "session.json", sess); err != nil {
		return nil, err
	}
	if err := s.saveIndex(); err != nil {
		return nil, err
	}
	return rev, nil
}

func loopConfigEqual(a, b LoopConfig) bool {
	if len(a.TargetNodeIDs) != len(b.TargetNodeIDs) {
		return false
	}
	for i := range a.TargetNodeIDs {
		if a.TargetNodeIDs[i] != b.TargetNodeIDs[i] {
			return false
		}
	}
	return a.Enabled == b.Enabled && a.Mode == b.Mode && a.MaxIterations == b.MaxIterations &&
		a.FixedIterations == b.FixedIterations && a.ReviewNodeID == b.ReviewNodeID && a.Protocol == b.Protocol
}

// UpdatePipelineRevisionLoopConfig updates the LoopConfig on an existing
// pipeline revision after applying the same validation and compatibility rules
// used by atomic pipeline saves.
func (s *Store) UpdatePipelineRevisionLoopConfig(sessionID, revisionID string, loopConfig *LoopConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	rev, ok := s.pipelineRevisions[revisionID]
	if !ok {
		return fmt.Errorf("revision %q not found", revisionID)
	}
	if rev.SessionID != sessionID {
		return fmt.Errorf("revision %q does not belong to session %q", revisionID, sessionID)
	}
	cfg := LoopConfig{}
	if loopConfig != nil {
		cfg = *loopConfig
	}
	if err := ValidateLoopConfig(&cfg, rev.Nodes); err != nil {
		return err
	}
	if err := ValidateLoopTargets(&cfg, rev.Nodes); err != nil {
		return err
	}
	normalized, err := NormalizeLoopConfig(&cfg)
	if err != nil {
		return err
	}
	rev.LoopConfig = normalized
	rev.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	dir := filepath.Join(sessionDir(sessionID), "pipelines")
	return saveSessionJSON(dir, revisionID+".json", rev)
}

// pipelineContentChanged checks if two revisions have different content.
// Only compares execution-relevant fields, not canvas coordinates (X/Y).
func pipelineContentChanged(old *PipelineRevision, newNodes []AgentNode, newEdges []Edge) bool {
	if len(old.Nodes) != len(newNodes) || len(old.Edges) != len(newEdges) {
		return true
	}
	for i, n := range old.Nodes {
		nn := newNodes[i]
		if n.ID != nn.ID || n.Type != nn.Type || n.Label != nn.Label ||
			n.Model != nn.Model || n.ProviderRoute != nn.ProviderRoute || n.Executor != nn.Executor ||
			n.RoleDesc != nn.RoleDesc || n.Skill != nn.Skill ||
			n.Agent != nn.Agent || n.Mode != nn.Mode ||
			n.ApprovalMode != nn.ApprovalMode || n.ExecutionMode != nn.ExecutionMode {
			return true
		}
	}
	for i, e := range old.Edges {
		if e.FromID != newEdges[i].FromID || e.ToID != newEdges[i].ToID {
			return true
		}
	}
	return false
}

// ── PipelineRun CRUD ──

// CreateRun creates a new pipeline run within a session.
func (s *Store) CreateRun(sessionID, pipelineRevID, task, rewrittenTask, trigger, parentRunID string) (*PipelineRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}
	rev, ok := s.pipelineRevisions[pipelineRevID]
	if !ok {
		return nil, fmt.Errorf("pipeline revision %q not found", pipelineRevID)
	}
	if rev.SessionID != sessionID {
		return nil, fmt.Errorf("pipeline revision %q does not belong to session %q", pipelineRevID, sessionID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	runID := fmt.Sprintf("run_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	// Build node states and copy LoopConfig from the matched pipeline revision.
	nodeStates := make(map[string]RunState)
	for _, n := range rev.Nodes {
		nodeStates[n.ID] = RunState{Status: NodePending, TokenUsage: TokenUsage{}}
	}
	loopConfig := rev.LoopConfig
	if normalized, err := NormalizeLoopConfig(&loopConfig); err == nil {
		loopConfig = normalized
	}

	run := &PipelineRun{
		ID:                 runID,
		PipelineID:         pipelineRevID,
		PipelineRevisionID: pipelineRevID,
		SessionID:          sessionID,
		Task:               task,
		RewrittenTask:      rewrittenTask,
		Status:             "running",
		Trigger:            trigger,
		ParentRunID:        parentRunID,
		ExecOptions:        ExecutionOptions{Trigger: trigger, ParentRunID: parentRunID, Workspace: resolveRunWorkspace("", sess.Workspace)},
		NodeStates:         nodeStates,
		LoopConfig:         loopConfig,
		CreatedAt:          now,
		StartedAt:          now,
		UpdatedAt:          now,
	}

	s.runs[runID] = run
	sess.RunIDs = append(sess.RunIDs, runID)
	sess.CurrentRunID = runID
	if task != "" {
		sess.ActiveTask = task
	}
	if rewrittenTask != "" {
		sess.RewrittenTask = rewrittenTask
	}
	sess.UpdatedAt = now

	// Persist
	dir := filepath.Join(sessionDir(sessionID), "runs")
	if err := saveSessionJSON(dir, runID+".json", run); err != nil {
		return nil, err
	}
	if err := saveSessionJSON(sessionDir(sessionID), "session.json", sess); err != nil {
		return nil, err
	}
	if err := s.saveIndex(); err != nil {
		return nil, err
	}
	return run, nil
}

// SetCurrentRun updates the current run ID for a session.
func (s *Store) SetCurrentRun(sessionID, runID string) error {
	return s.UpdateOrchSession(sessionID, func(sess *OrchestrationSession) {
		sess.CurrentRunID = runID
	})
}

// ListRunsForSession returns all runs for a session, newest first.
func (s *Store) ListRunsForSession(sessionID string) []PipelineRun {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil
	}
	var out []PipelineRun
	for _, id := range sess.RunIDs {
		if r, ok := s.runs[id]; ok {
			out = append(out, clonePipelineRun(r))
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt > out[j].CreatedAt
	})
	return out
}

// ── NodeAttempt CRUD ──

// CreateAttempt creates a new attempt for a node within a run.
func (s *Store) CreateAttempt(runID, nodeID, bindingID string) (*NodeAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	// Count existing attempts for this node in this run
	attemptNum := 0
	for _, a := range s.attempts {
		if a.RunID == runID && a.NodeID == nodeID {
			attemptNum++
		}
	}
	attemptNum++

	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("att_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	attempt := &NodeAttempt{
		ID:             id,
		RunID:          runID,
		NodeID:         nodeID,
		AttemptNumber:  attemptNum,
		Status:         "running",
		AgentBindingID: bindingID,
		StartedAt:      now,
	}

	s.attempts[id] = attempt
	run.NodeAttemptIDs = append(run.NodeAttemptIDs, id)
	run.CurrentNodeID = nodeID
	run.UpdatedAt = now

	// Persist attempt
	dir := filepath.Join(sessionDir(run.SessionID), "attempts")
	if err := saveSessionJSON(dir, id+".json", attempt); err != nil {
		return nil, fmt.Errorf("persist attempt: %w", err)
	}
	// Persist run update
	runDir := filepath.Join(sessionDir(run.SessionID), "runs")
	if err := saveSessionJSON(runDir, runID+".json", run); err != nil {
		return nil, fmt.Errorf("persist run after attempt create: %w", err)
	}

	return attempt, nil
}

// CreateAttemptWithIteration creates a new attempt with an iteration ID.
func (s *Store) CreateAttemptWithIteration(runID, nodeID, bindingID, iterationID string) (*NodeAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	run, ok := s.runs[runID]
	if !ok {
		return nil, fmt.Errorf("run %q not found", runID)
	}

	// Count existing attempts for this node in this run AND iteration
	attemptNum := 0
	for _, a := range s.attempts {
		if a.RunID == runID && a.NodeID == nodeID && a.IterationID == iterationID {
			attemptNum++
		}
	}
	attemptNum++

	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("att_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	attempt := &NodeAttempt{
		ID:             id,
		RunID:          runID,
		NodeID:         nodeID,
		IterationID:    iterationID,
		AttemptNumber:  attemptNum,
		Status:         "running",
		AgentBindingID: bindingID,
		StartedAt:      now,
	}

	s.attempts[id] = attempt
	run.NodeAttemptIDs = append(run.NodeAttemptIDs, id)
	run.CurrentNodeID = nodeID
	run.UpdatedAt = now

	dir := filepath.Join(sessionDir(run.SessionID), "attempts")
	if err := saveSessionJSON(dir, id+".json", attempt); err != nil {
		return nil, fmt.Errorf("persist attempt: %w", err)
	}
	runDir := filepath.Join(sessionDir(run.SessionID), "runs")
	if err := saveSessionJSON(runDir, runID+".json", run); err != nil {
		return nil, fmt.Errorf("persist run after attempt create: %w", err)
	}

	return attempt, nil
}

// GetAttempt returns an attempt by ID (returns copy, not pointer).
func (s *Store) GetAttempt(id string) (NodeAttempt, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	a, ok := s.attempts[id]
	if !ok {
		return NodeAttempt{}, false
	}
	return *a, true
}

// UpdateAttempt updates an attempt via a mutation function.
func (s *Store) UpdateAttempt(id string, fn func(*NodeAttempt)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	attempt, ok := s.attempts[id]
	if !ok {
		return fmt.Errorf("attempt %q not found", id)
	}
	fn(attempt)

	// Find session ID from the run
	if run, ok := s.runs[attempt.RunID]; ok {
		dir := filepath.Join(sessionDir(run.SessionID), "attempts")
		if err := saveSessionJSON(dir, id+".json", attempt); err != nil {
			return fmt.Errorf("persist attempt: %w", err)
		}
	}
	return nil
}

// ListAttempts returns all attempts for a run.
func (s *Store) ListAttempts(runID string) []NodeAttempt {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []NodeAttempt
	for _, a := range s.attempts {
		if a.RunID == runID {
			out = append(out, *a)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AttemptNumber < out[j].AttemptNumber
	})
	return out
}

// ── AgentBinding CRUD ──

// CreateBinding creates a new agent binding for a session node.
func (s *Store) CreateBinding(sessionID, nodeID string, node AgentNode) (*AgentBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("bind_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	binding := &AgentBinding{
		ID:            id,
		SessionID:     sessionID,
		NodeID:        nodeID,
		Executor:      string(node.Executor),
		Model:         node.Model,
		Agent:         node.Agent,
		Skill:         node.Skill,
		Mode:          node.Mode,
		ContextPolicy: "reuse",
		CreatedAt:     now,
		UpdatedAt:     now,
		Status:        "active",
	}

	s.bindings[id] = binding
	sess.AgentBindingIDs = append(sess.AgentBindingIDs, id)
	sess.UpdatedAt = now

	dir := filepath.Join(sessionDir(sessionID), "agents")
	if err := saveSessionJSON(dir, id+".json", binding); err != nil {
		return nil, fmt.Errorf("persist binding: %w", err)
	}
	if err := saveSessionJSON(sessionDir(sessionID), "session.json", sess); err != nil {
		return nil, fmt.Errorf("persist session after binding create: %w", err)
	}
	if err := s.saveIndex(); err != nil {
		return nil, fmt.Errorf("persist index after binding create: %w", err)
	}

	return binding, nil
}

// GetBinding returns an agent binding by ID.
func (s *Store) GetBinding(id string) (*AgentBinding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.bindings[id]
	return b, ok
}

// FindBinding finds an existing binding for a session+node.
func (s *Store) FindBinding(sessionID, nodeID string) (*AgentBinding, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil, false
	}
	for _, id := range sess.AgentBindingIDs {
		if b, ok := s.bindings[id]; ok && b.NodeID == nodeID && b.Status == "active" {
			return b, true
		}
	}
	return nil, false
}

// FindOrCreateBinding finds an existing binding or creates a new one.
func (s *Store) FindOrCreateBinding(sessionID, nodeID string, node AgentNode) (*AgentBinding, error) {
	if b, ok := s.FindBinding(sessionID, nodeID); ok {
		if bindingMatchesNode(b, node) {
			return b, nil
		}
		// Mismatch — need new binding
		if err := s.UpdateBinding(b.ID, func(bind *AgentBinding) {
			bind.Status = "detached"
		}); err != nil {
			return nil, fmt.Errorf("detach old binding: %w", err)
		}
	}
	return s.CreateBinding(sessionID, nodeID, node)
}

// bindingMatchesNode reports whether an existing binding can keep serving a
// node whose config drifted. Matching is graded by memory value:
//
//   - Executor / Agent / Skill changed → false. A different engine or persona
//     cannot meaningfully continue the old conversation.
//   - Only Model / Mode (run↔serve) changed → true. Provider sessions support
//     switching models mid-conversation, so a cost tweak or mode flip no longer
//     costs the agent its entire memory (the historical strict match detached
//     the binding and silently reset every provider session).
//
// Callers must persist the new Model/Mode onto the binding when this returns
// true with changed fields (see findOrCreateBindingLocked).
func bindingMatchesNode(b *AgentBinding, node AgentNode) bool {
	if b.Executor != string(node.Executor) ||
		b.Agent != node.Agent ||
		b.Skill != node.Skill {
		return false
	}
	return true
}

// UpdateBinding updates a binding via a mutation function.
func (s *Store) UpdateBinding(id string, fn func(*AgentBinding)) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	b, ok := s.bindings[id]
	if !ok {
		return fmt.Errorf("binding %q not found", id)
	}
	fn(b)
	b.UpdatedAt = time.Now().UTC().Format(time.RFC3339)

	dir := filepath.Join(sessionDir(b.SessionID), "agents")
	if err := saveSessionJSON(dir, id+".json", b); err != nil {
		return fmt.Errorf("persist binding: %w", err)
	}
	return nil
}

// ForkBinding creates a new binding forked from an existing one.
func (s *Store) ForkBinding(bindingID, reason string) (*AgentBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	old, ok := s.bindings[bindingID]
	if !ok {
		return nil, fmt.Errorf("binding %q not found", bindingID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("bind_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	newBind := &AgentBinding{
		ID:            id,
		SessionID:     old.SessionID,
		NodeID:        old.NodeID,
		Executor:      old.Executor,
		Model:         old.Model,
		Agent:         old.Agent,
		Skill:         old.Skill,
		Mode:          old.Mode,
		ContextPolicy: "fork",
		CreatedAt:     now,
		UpdatedAt:     now,
		Status:        "active",
	}

	s.bindings[id] = newBind
	if sess, ok := s.orchSessions[old.SessionID]; ok {
		sess.AgentBindingIDs = append(sess.AgentBindingIDs, id)
		sess.UpdatedAt = now
		if err := saveSessionJSON(sessionDir(old.SessionID), "session.json", sess); err != nil {
			return nil, fmt.Errorf("persist session after fork: %w", err)
		}
	}

	dir := filepath.Join(sessionDir(old.SessionID), "agents")
	if err := saveSessionJSON(dir, id+".json", newBind); err != nil {
		return nil, fmt.Errorf("persist forked binding: %w", err)
	}
	if err := s.saveIndex(); err != nil {
		return nil, fmt.Errorf("persist index after fork: %w", err)
	}

	return newBind, nil
}

// ListBindings returns all bindings for a session.
func (s *Store) ListBindings(sessionID string) []AgentBinding {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil
	}
	var out []AgentBinding
	for _, id := range sess.AgentBindingIDs {
		if b, ok := s.bindings[id]; ok {
			out = append(out, *b)
		}
	}
	return out
}

// ── ProviderSession CRUD ──

// CreateProviderSession creates a new provider session record.
func (s *Store) CreateProviderSession(bindingID, executor, sessionPath, workspace string) (*ProviderSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	binding, ok := s.bindings[bindingID]
	if !ok {
		return nil, fmt.Errorf("binding %q not found", bindingID)
	}

	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("psess_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	ps := &ProviderSession{
		ID:             id,
		AgentBindingID: bindingID,
		Executor:       executor,
		SessionPath:    sessionPath,
		Workspace:      workspace,
		CreatedAt:      now,
		UpdatedAt:      now,
		Status:         "active",
	}

	s.providerSessions[id] = ps
	binding.ProviderSessionID = id
	binding.ProviderSessionPath = sessionPath
	binding.UpdatedAt = now

	// Persist to separate provider-sessions/ directory
	psDir := filepath.Join(sessionDir(binding.SessionID), "provider-sessions")
	if err := saveSessionJSON(psDir, id+".json", ps); err != nil {
		return nil, fmt.Errorf("persist provider session: %w", err)
	}
	bindDir := filepath.Join(sessionDir(binding.SessionID), "agents")
	if err := saveSessionJSON(bindDir, binding.ID+".json", binding); err != nil {
		return nil, fmt.Errorf("persist binding: %w", err)
	}

	return ps, nil
}

// FindOrCreateBindingAndProviderSession atomically finds or creates both a binding
// and its ProviderSession under a single Store lock. This prevents concurrent
// Pipeline Runs from creating duplicate ProviderSessions for the same binding.
func (s *Store) FindOrCreateBindingAndProviderSession(sessionID, nodeID string, node AgentNode, executor, workspace, policy string, reuseAgentSessions bool) (*AgentBinding, *ProviderSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Find or create binding
	binding, err := s.findOrCreateBindingLocked(sessionID, nodeID, node)
	if err != nil {
		return nil, nil, err
	}

	// Determine whether to reuse or create new ProviderSession
	shouldReuse := reuseAgentSessions && (policy == "" || policy == "reuse")
	var ps *ProviderSession

	if shouldReuse && binding.ProviderSessionID != "" {
		existing, ok := s.providerSessions[binding.ProviderSessionID]
		// A provider session is bound to its working directory. Reusing a
		// session created for another workspace would make a later Run execute
		// with stale provider history or the wrong sandbox directory.
		if ok && strings.TrimSpace(existing.Workspace) == strings.TrimSpace(workspace) {
			ps = existing
		}
	}

	if ps == nil {
		now := time.Now().UTC().Format(time.RFC3339)
		id := fmt.Sprintf("psess_%d_%d", time.Now().UnixMilli(), s.nextID)
		s.nextID++

		ps = &ProviderSession{
			ID:             id,
			AgentBindingID: binding.ID,
			Executor:       executor,
			Workspace:      workspace,
			CreatedAt:      now,
			UpdatedAt:      now,
			Status:         "active",
		}

		s.providerSessions[id] = ps
		binding.ProviderSessionID = id
		binding.UpdatedAt = now

		psDir := filepath.Join(sessionDir(binding.SessionID), "provider-sessions")
		if err := saveSessionJSON(psDir, id+".json", ps); err != nil {
			return nil, nil, fmt.Errorf("persist provider session: %w", err)
		}
	}

	// Persist binding
	bindDir := filepath.Join(sessionDir(binding.SessionID), "agents")
	if err := saveSessionJSON(bindDir, binding.ID+".json", binding); err != nil {
		return nil, nil, fmt.Errorf("persist binding: %w", err)
	}

	return binding, ps, nil
}

// findOrCreateBindingLocked finds or creates a binding. Must be called with s.mu held.
func (s *Store) findOrCreateBindingLocked(sessionID, nodeID string, node AgentNode) (*AgentBinding, error) {
	sess, ok := s.orchSessions[sessionID]
	if !ok {
		return nil, fmt.Errorf("session %q not found", sessionID)
	}

	for _, id := range sess.AgentBindingIDs {
		if b, ok := s.bindings[id]; ok && b.NodeID == nodeID && b.Status == "active" {
			if bindingMatchesNode(b, node) {
				// Graded match: keep the binding (and its ProviderSession /
				// conversation memory) even when Model or Mode drifted, but
				// record the new values so the UI and future matches see them.
				if b.Model != node.Model || b.Mode != node.Mode {
					b.Model = node.Model
					b.Mode = node.Mode
					b.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
					bindingDir := filepath.Join(sessionDir(sessionID), "agents")
					if err := saveSessionJSON(bindingDir, b.ID+".json", b); err != nil {
						return nil, fmt.Errorf("persist binding model/mode update: %w", err)
					}
				}
				return b, nil
			}
			// Mismatch — detach old binding
			b.Status = "detached"
			b.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
			break
		}
	}

	// Create new binding
	now := time.Now().UTC().Format(time.RFC3339)
	id := fmt.Sprintf("bind_%d_%d", time.Now().UnixMilli(), s.nextID)
	s.nextID++

	binding := &AgentBinding{
		ID:            id,
		SessionID:     sessionID,
		NodeID:        nodeID,
		Executor:      string(node.Executor),
		Model:         node.Model,
		Agent:         node.Agent,
		Skill:         node.Skill,
		Mode:          node.Mode,
		ContextPolicy: "reuse",
		CreatedAt:     now,
		UpdatedAt:     now,
		Status:        "active",
	}

	s.bindings[id] = binding
	sess.AgentBindingIDs = append(sess.AgentBindingIDs, id)
	sess.UpdatedAt = now

	return binding, nil
}

// GetProviderSession returns a provider session by ID (returns copy, not pointer).
func (s *Store) GetProviderSession(id string) (ProviderSession, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ps, ok := s.providerSessions[id]
	if !ok {
		return ProviderSession{}, false
	}
	return *ps, true
}

// ── RuntimeState CRUD ──

// CreateRuntimeState creates a new runtime state record.
func (s *Store) CreateRuntimeState(rt RuntimeState) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.runtimeStates[rt.RuntimeID] = &rt

	// Persist to runtimes/ directory
	if rt.SessionID != "" {
		rtDir := filepath.Join(sessionDir(rt.SessionID), "runtimes")
		if err := saveSessionJSON(rtDir, rt.RuntimeID+".json", &rt); err != nil {
			return fmt.Errorf("persist runtime state: %w", err)
		}
		// Append to session's RuntimeIDs
		if sess, ok := s.orchSessions[rt.SessionID]; ok {
			found := false
			for _, id := range sess.RuntimeIDs {
				if id == rt.RuntimeID {
					found = true
					break
				}
			}
			if !found {
				sess.RuntimeIDs = append(sess.RuntimeIDs, rt.RuntimeID)
				sess.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
				if err := saveSessionJSON(sessionDir(rt.SessionID), "session.json", sess); err != nil {
					return fmt.Errorf("persist session after runtime create: %w", err)
				}
				if err := s.saveIndex(); err != nil {
					return fmt.Errorf("persist index after runtime create: %w", err)
				}
			}
		}
	}
	return nil
}

// GetRuntimeState returns a runtime state by ID (returns copy).
func (s *Store) GetRuntimeState(id string) (RuntimeState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rt, ok := s.runtimeStates[id]
	if !ok {
		return RuntimeState{}, false
	}
	return *rt, true
}

// UpdateRuntimeState updates a runtime state via a mutation function.
func (s *Store) UpdateRuntimeState(id string, fn func(*RuntimeState)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rt, ok := s.runtimeStates[id]
	if !ok {
		return fmt.Errorf("runtime state %q not found", id)
	}
	fn(rt)
	rt.LastActiveAt = time.Now()
	if rt.SessionID != "" {
		rtDir := filepath.Join(sessionDir(rt.SessionID), "runtimes")
		if err := saveSessionJSON(rtDir, id+".json", rt); err != nil {
			return fmt.Errorf("persist runtime state: %w", err)
		}
	}
	return nil
}

// ListRuntimeStates returns all runtime states for a session.
func (s *Store) ListRuntimeStates(sessionID string) []RuntimeState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []RuntimeState
	for _, rt := range s.runtimeStates {
		if rt.SessionID == sessionID {
			out = append(out, *rt)
		}
	}
	return out
}

// ── LoopIteration CRUD ──

// CreateIteration creates a new loop iteration.
func (s *Store) CreateIteration(iter LoopIteration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.iterations[iter.ID] = &iter

	// Persist to iterations/ directory
	if iter.RunID != "" {
		if run, ok := s.runs[iter.RunID]; ok && run.SessionID != "" {
			iterDir := filepath.Join(sessionDir(run.SessionID), "iterations")
			if err := saveSessionJSON(iterDir, iter.ID+".json", &iter); err != nil {
				return fmt.Errorf("persist iteration: %w", err)
			}
		}
	}
	return nil
}

// GetIteration returns an iteration by ID.
func (s *Store) GetIteration(id string) (LoopIteration, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	iter, ok := s.iterations[id]
	if !ok {
		return LoopIteration{}, false
	}
	return *iter, true
}

// UpdateIteration updates an iteration via a mutation function.
func (s *Store) UpdateIteration(id string, fn func(*LoopIteration)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	iter, ok := s.iterations[id]
	if !ok {
		return fmt.Errorf("iteration %q not found", id)
	}
	fn(iter)
	if iter.RunID != "" {
		if run, ok := s.runs[iter.RunID]; ok && run.SessionID != "" {
			iterDir := filepath.Join(sessionDir(run.SessionID), "iterations")
			if err := saveSessionJSON(iterDir, id+".json", iter); err != nil {
				return fmt.Errorf("persist iteration: %w", err)
			}
		}
	}
	return nil
}

// ListIterations returns all iterations for a run.
func (s *Store) ListIterations(runID string) []LoopIteration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []LoopIteration
	for _, iter := range s.iterations {
		if iter.RunID == runID {
			out = append(out, *iter)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Number < out[j].Number
	})
	return out
}

// UpdateProviderSession updates a provider session via a mutation function.
func (s *Store) UpdateProviderSession(id string, fn func(*ProviderSession)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ps, ok := s.providerSessions[id]
	if !ok {
		return fmt.Errorf("provider session %q not found", id)
	}
	fn(ps)
	ps.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	if binding, ok := s.bindings[ps.AgentBindingID]; ok {
		psDir := filepath.Join(sessionDir(binding.SessionID), "provider-sessions")
		if err := saveSessionJSON(psDir, id+".json", ps); err != nil {
			return fmt.Errorf("persist provider session: %w", err)
		}
	}
	return nil
}

// ── Migration ──

// MigrateLegacyData migrates old store.json and sessions/*.json to the new directory structure.
func (s *Store) MigrateLegacyData(legacyStorePath string) error {
	marker := filepath.Join(orchestratorRoot(), ".migrated")
	if _, err := os.Stat(marker); err == nil {
		return nil // already migrated
	}

	slog := fmt.Sprintf("orchestrator: migrating legacy data from %s", legacyStorePath)

	// Load legacy store.json
	if err := s.Load(legacyStorePath); err != nil {
		fmt.Println(slog, "failed to load legacy store:", err)
		return nil // not fatal
	}

	// Try to find legacy session state files
	stateDir := sessionStateDir()
	entries, _ := os.ReadDir(stateDir)

	if len(s.pipelines) == 0 && len(entries) == 0 {
		// No legacy data, mark as migrated
		_ = os.MkdirAll(orchestratorRoot(), 0755)
		_ = atomicWriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339)), 0644)
		return nil
	}

	// Create a migration session
	now := time.Now().UTC().Format(time.RFC3339)
	migrationSessionID := "orch_migrated_main"
	migrationSession := &OrchestrationSession{
		ID:        migrationSessionID,
		Title:     "迁移的历史数据",
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}

	// Migrate pipelines as revisions
	revCount := 0
	for _, pipe := range s.pipelines {
		rev := PipelineRevision{
			ID:        pipe.ID,
			SessionID: migrationSessionID,
			Version:   revCount + 1,
			Name:      pipe.Name,
			Nodes:     pipe.Nodes,
			Edges:     pipe.Edges,
			Source:    "migration",
			CreatedAt: pipe.CreatedAt,
			UpdatedAt: pipe.UpdatedAt,
			Status:    "active",
		}
		s.pipelineRevisions[pipe.ID] = &rev
		migrationSession.PipelineRevisionIDs = append(migrationSession.PipelineRevisionIDs, pipe.ID)
		if revCount == 0 {
			migrationSession.CurrentPipelineID = pipe.ID
		}
		revCount++
	}

	// Migrate runs
	for _, run := range s.runs {
		run.SessionID = migrationSessionID
		s.runs[run.ID] = run
		migrationSession.RunIDs = append(migrationSession.RunIDs, run.ID)
	}
	if len(migrationSession.RunIDs) > 0 {
		migrationSession.CurrentRunID = migrationSession.RunIDs[len(migrationSession.RunIDs)-1]
	}

	// Migrate legacy session states (requirementMessages, chatMessages, etc.)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		stateID := strings.TrimSuffix(entry.Name(), ".json")
		state, err := s.LoadSessionState(stateID)
		if err != nil {
			continue
		}
		// Merge requirement messages from chatMessages
		for _, msg := range state.ChatMessages {
			migrationSession.RequirementMessages = append(migrationSession.RequirementMessages, msg)
		}
		if state.RewrittenTask != "" {
			migrationSession.RewrittenTask = state.RewrittenTask
		}
	}

	s.orchSessions[migrationSessionID] = migrationSession

	// Persist everything
	dir := sessionDir(migrationSessionID)
	_ = saveSessionJSON(dir, "session.json", migrationSession)

	pipelinesDir := filepath.Join(dir, "pipelines")
	for _, rev := range s.pipelineRevisions {
		_ = saveSessionJSON(pipelinesDir, rev.ID+".json", rev)
	}

	runsDir := filepath.Join(dir, "runs")
	for _, run := range s.runs {
		_ = saveSessionJSON(runsDir, run.ID+".json", run)
	}

	_ = s.saveIndex()
	_ = os.MkdirAll(orchestratorRoot(), 0755)
	_ = atomicWriteFile(marker, []byte(time.Now().UTC().Format(time.RFC3339)), 0644)

	fmt.Println(slog, "completed:", len(s.pipelines), "pipelines,", len(s.runs), "runs")
	return nil
}

// reconcileSessionFiles recovers session records that were durably written
// before index.json was updated. The index is intentionally treated as a cache
// of session pointers rather than the only source of truth.
func (s *Store) reconcileSessionFiles() error {
	root := filepath.Join(orchestratorRoot(), "sessions")
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if s.orchSessions == nil {
		s.orchSessions = make(map[string]*OrchestrationSession)
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		var diskSession OrchestrationSession
		if err := loadSessionJSON(filepath.Join(root, entry.Name()), "session.json", &diskSession); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("load session %s: %w", entry.Name(), err)
		}
		if diskSession.ID == "" {
			diskSession.ID = entry.Name()
		}
		current, exists := s.orchSessions[diskSession.ID]
		// RFC3339 UTC timestamps sort lexicographically. Prefer the disk
		// document when it is at least as new as the index entry.
		if !exists || diskSession.UpdatedAt >= current.UpdatedAt {
			sess := diskSession
			s.orchSessions[diskSession.ID] = &sess
		}
	}
	return nil
}

func containsID(ids []string, id string) bool {
	for _, existing := range ids {
		if existing == id {
			return true
		}
	}
	return false
}

// rebuildSessionReferences reconstructs session lists from entity documents.
// This closes the crash window between writing a run/revision and updating the
// session index, which otherwise made old history vanish after a rebuild.
func (s *Store) rebuildSessionReferences() {
	for sessionID, sess := range s.orchSessions {
		var revisions []PipelineRevision
		for _, rev := range s.pipelineRevisions {
			if rev.SessionID != sessionID {
				continue
			}
			if !containsID(sess.PipelineRevisionIDs, rev.ID) {
				sess.PipelineRevisionIDs = append(sess.PipelineRevisionIDs, rev.ID)
			}
			revisions = append(revisions, *rev)
		}
		sort.Slice(revisions, func(i, j int) bool { return revisions[i].Version < revisions[j].Version })
		if len(revisions) > 0 {
			sess.PipelineRevisionIDs = sess.PipelineRevisionIDs[:0]
			for _, rev := range revisions {
				sess.PipelineRevisionIDs = append(sess.PipelineRevisionIDs, rev.ID)
			}
			if _, ok := s.pipelineRevisions[sess.CurrentPipelineID]; !ok {
				sess.CurrentPipelineID = revisions[len(revisions)-1].ID
			}
		}

		var runs []PipelineRun
		for _, run := range s.runs {
			if run.SessionID != sessionID {
				continue
			}
			runs = append(runs, *run)
		}
		sort.Slice(runs, func(i, j int) bool { return runs[i].CreatedAt < runs[j].CreatedAt })
		if len(runs) > 0 {
			sess.RunIDs = sess.RunIDs[:0]
			for _, run := range runs {
				sess.RunIDs = append(sess.RunIDs, run.ID)
			}
			if _, ok := s.runs[sess.CurrentRunID]; !ok {
				sess.CurrentRunID = runs[len(runs)-1].ID
			}
		}
	}
}

// LoadIndexData loads only the session index for HTTP startup. The index is
// the durable, atomically-written list used by the history UI; scanning all
// 6,000+ session directories here made the first /presets request block long
// enough for the browser's startup timeout to fire. Child entities are still
// loaded on demand by LoadSessionData, and LoadAllData retains the full
// reconciliation path for explicit full-store recovery.
func (s *Store) LoadIndexData() error {
	return s.loadIndex()
}

// LoadSessionData loads the entities belonging to one orchestration session.
// Keeping this operation session-scoped avoids making the first browser request
// scan thousands of old sessions on Windows. The caller may safely invoke it
// repeatedly; later loads replace the same entity IDs in memory.
func (s *Store) LoadSessionData(sessionID string) error {
	if strings.TrimSpace(sessionID) == "" {
		return fmt.Errorf("session ID is required")
	}
	sDir := sessionDir(sessionID)

	revisions := make(map[string]*PipelineRevision)
	for _, f := range listSessionFiles(filepath.Join(sDir, "pipelines")) {
		var rev PipelineRevision
		if err := loadSessionJSON(filepath.Join(sDir, "pipelines"), f, &rev); err != nil {
			continue
		}
		if rev.LoopConfig.Enabled {
			if normalized, err := NormalizeLoopConfig(&rev.LoopConfig); err == nil {
				rev.LoopConfig = normalized
			}
		}
		revisions[rev.ID] = &rev
	}

	runs := make(map[string]*PipelineRun)
	for _, f := range listSessionFiles(filepath.Join(sDir, "runs")) {
		var run PipelineRun
		if err := loadSessionJSON(filepath.Join(sDir, "runs"), f, &run); err != nil {
			continue
		}
		if run.LoopConfig.Enabled {
			if normalized, err := NormalizeLoopConfig(&run.LoopConfig); err == nil {
				run.LoopConfig = normalized
			}
		}
		runs[run.ID] = &run
	}

	attempts := make(map[string]*NodeAttempt)
	for _, f := range listSessionFiles(filepath.Join(sDir, "attempts")) {
		var attempt NodeAttempt
		if err := loadSessionJSON(filepath.Join(sDir, "attempts"), f, &attempt); err == nil {
			attempts[attempt.ID] = &attempt
		}
	}

	bindings := make(map[string]*AgentBinding)
	for _, f := range listSessionFiles(filepath.Join(sDir, "agents")) {
		var binding AgentBinding
		if err := loadSessionJSON(filepath.Join(sDir, "agents"), f, &binding); err == nil {
			bindings[binding.ID] = &binding
		}
	}

	providerSessions := make(map[string]*ProviderSession)
	for _, f := range listSessionFiles(filepath.Join(sDir, "provider-sessions")) {
		var ps ProviderSession
		if err := loadSessionJSON(filepath.Join(sDir, "provider-sessions"), f, &ps); err == nil {
			providerSessions[ps.ID] = &ps
		}
	}

	runtimeStates := make(map[string]*RuntimeState)
	for _, f := range listSessionFiles(filepath.Join(sDir, "runtimes")) {
		var runtime RuntimeState
		if err := loadSessionJSON(filepath.Join(sDir, "runtimes"), f, &runtime); err == nil {
			runtimeStates[runtime.RuntimeID] = &runtime
		}
	}

	iterations := make(map[string]*LoopIteration)
	for _, f := range listSessionFiles(filepath.Join(sDir, "iterations")) {
		var iteration LoopIteration
		if err := loadSessionJSON(filepath.Join(sDir, "iterations"), f, &iteration); err == nil {
			iterations[iteration.ID] = &iteration
		}
	}

	s.mu.Lock()
	for id, rev := range revisions {
		s.pipelineRevisions[id] = rev
	}
	for id, run := range runs {
		s.runs[id] = run
	}
	for id, attempt := range attempts {
		s.attempts[id] = attempt
	}
	for id, binding := range bindings {
		s.bindings[id] = binding
	}
	for id, ps := range providerSessions {
		s.providerSessions[id] = ps
	}
	for id, runtime := range runtimeStates {
		s.runtimeStates[id] = runtime
	}
	for id, iteration := range iterations {
		s.iterations[id] = iteration
	}
	s.mu.Unlock()

	// A process restart cannot leave an old run looking active forever. This is
	// intentionally done only for the session just loaded, not for every session
	// in the history index.
	return s.markInterrupted()
}

// LoadAllData loads all data from disk into memory.
func (s *Store) LoadAllData() error {
	// Load index first, then reconcile it with the per-session documents. The
	// index is only a directory of pointers; session.json is the authoritative
	// record for RunIDs/PipelineRevisionIDs. If a process stopped after writing a
	// session file but before updating index.json, rebuilding must not make the
	// history disappear from the UI.
	if err := s.loadIndex(); err != nil {
		return err
	}
	if err := s.reconcileSessionFiles(); err != nil {
		return err
	}

	// Load each session's sub-entities
	for sessID := range s.orchSessions {
		sDir := sessionDir(sessID)

		// Load pipeline revisions
		revFiles := listSessionFiles(filepath.Join(sDir, "pipelines"))
		for _, f := range revFiles {
			var rev PipelineRevision
			if err := loadSessionJSON(filepath.Join(sDir, "pipelines"), f, &rev); err == nil {
				if rev.LoopConfig.Enabled {
					if normalized, normErr := NormalizeLoopConfig(&rev.LoopConfig); normErr == nil {
						rev.LoopConfig = normalized
						// Keep the compatibility migration in memory during startup.
						// The normalized value is persisted by the next regular mutation;
						// loading history must not synchronously fsync every revision.
					}
				}
				s.pipelineRevisions[rev.ID] = &rev
			}
		}

		// Load runs
		runFiles := listSessionFiles(filepath.Join(sDir, "runs"))
		for _, f := range runFiles {
			var run PipelineRun
			if err := loadSessionJSON(filepath.Join(sDir, "runs"), f, &run); err == nil {
				if run.LoopConfig.Enabled {
					if normalized, normErr := NormalizeLoopConfig(&run.LoopConfig); normErr == nil {
						run.LoopConfig = normalized
						// As above, defer the disk rewrite until a normal run mutation.
					}
				}
				s.runs[run.ID] = &run
			}
		}

		// Load attempts
		attFiles := listSessionFiles(filepath.Join(sDir, "attempts"))
		for _, f := range attFiles {
			var att NodeAttempt
			if err := loadSessionJSON(filepath.Join(sDir, "attempts"), f, &att); err == nil {
				s.attempts[att.ID] = &att
			}
		}

		// Load bindings (agents/ directory only)
		bindFiles := listSessionFiles(filepath.Join(sDir, "agents"))
		for _, f := range bindFiles {
			var bind AgentBinding
			if err := loadSessionJSON(filepath.Join(sDir, "agents"), f, &bind); err == nil {
				s.bindings[bind.ID] = &bind
			}
		}

		// Load provider sessions (provider-sessions/ directory)
		psFiles := listSessionFiles(filepath.Join(sDir, "provider-sessions"))
		for _, f := range psFiles {
			var ps ProviderSession
			if err := loadSessionJSON(filepath.Join(sDir, "provider-sessions"), f, &ps); err == nil {
				s.providerSessions[ps.ID] = &ps
			}
		}

		// Load runtime states (runtimes/ directory)
		rtFiles := listSessionFiles(filepath.Join(sDir, "runtimes"))
		for _, f := range rtFiles {
			var rt RuntimeState
			if err := loadSessionJSON(filepath.Join(sDir, "runtimes"), f, &rt); err == nil {
				s.runtimeStates[rt.RuntimeID] = &rt
			}
		}

		// Load loop iterations (iterations/ directory)
		iterFiles := listSessionFiles(filepath.Join(sDir, "iterations"))
		for _, f := range iterFiles {
			var iter LoopIteration
			if err := loadSessionJSON(filepath.Join(sDir, "iterations"), f, &iter); err == nil {
				s.iterations[iter.ID] = &iter
			}
		}
	}

	// Entity files are authoritative for the session's child lists. Rebuild
	// references before recovery so a run that was written just before a
	// restart is still visible and resumable. Do not rewrite every session file
	// during startup: on a large history this turns a fast read into hundreds of
	// synchronous Windows FlushFileBuffers calls. The repaired references are
	// persisted by the next normal session mutation.
	s.rebuildSessionReferences()

	// Mark running Runs/Iterations/Attempts as interrupted on startup
	if err := s.markInterrupted(); err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	return nil
}

// markInterrupted marks all running entities as interrupted after restart.
// Returns the first persistence error encountered — caller must handle.
func (s *Store) markInterrupted() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now().UTC().Format(time.RFC3339)

	// Mark running runs as interrupted
	for _, run := range s.runs {
		if run.Status == "running" {
			run.Status = "interrupted"
			run.Error = "interrupted by restart"
			run.UpdatedAt = now
			runDir := filepath.Join(sessionDir(run.SessionID), "runs")
			if err := saveSessionJSON(runDir, run.ID+".json", run); err != nil {
				return fmt.Errorf("mark run %s interrupted: %w", run.ID, err)
			}
		}
	}

	// Mark running iterations as interrupted
	for _, iter := range s.iterations {
		if iter.Status == "running" {
			iter.Status = "interrupted"
			iter.Error = "interrupted by restart"
			iter.FinishedAt = now
			if run, ok := s.runs[iter.RunID]; ok {
				iterDir := filepath.Join(sessionDir(run.SessionID), "iterations")
				if err := saveSessionJSON(iterDir, iter.ID+".json", iter); err != nil {
					return fmt.Errorf("mark iteration %s interrupted: %w", iter.ID, err)
				}
			}
		}
	}

	// A persisted serve runtime only belongs to the process that launched it.
	// After an orchestrator restart every child process is gone, regardless of
	// executor (reasonix serve included) — never show a historical runtime as
	// alive. Preserve the provider ThreadID/session for a later resume, but
	// clear the process identity (endpoint/port/PID) so the UI cannot click a
	// dead endpoint.
	for _, rt := range s.runtimeStates {
		if rt.Status == RuntimeStopped || rt.Status == RuntimeError {
			continue
		}
		rt.Status = RuntimeStopped
		rt.Error = "orchestrator restarted; serve runtime is no longer connected"
		rt.Endpoint = ""
		rt.Port = 0
		rt.PID = 0
		rt.TurnID = ""
		rt.LastActiveAt = time.Now()
		if rt.SessionID != "" {
			rtDir := filepath.Join(sessionDir(rt.SessionID), "runtimes")
			if err := saveSessionJSON(rtDir, rt.RuntimeID+".json", rt); err != nil {
				return fmt.Errorf("mark retained runtime %s stopped: %w", rt.RuntimeID, err)
			}
		}
	}

	// Mark running attempts as interrupted
	for _, att := range s.attempts {
		if att.Status == "running" {
			att.Status = "interrupted"
			att.Error = "interrupted by restart"
			att.FinishedAt = now
			if run, ok := s.runs[att.RunID]; ok {
				attDir := filepath.Join(sessionDir(run.SessionID), "attempts")
				if err := saveSessionJSON(attDir, att.ID+".json", att); err != nil {
					return fmt.Errorf("mark attempt %s interrupted: %w", att.ID, err)
				}
			}
		}
	}

	return nil
}
