package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"reasonix/internal/event"
	"reasonix/internal/orchestrator"
)

// orchestratorHandler routes /orchestrator/api/* requests to the store.
type orchestratorHandler struct {
	store *orchestrator.Store

	// Loading the complete directory tree synchronously made the first browser
	// request wait for tens of thousands of JSON files. Keep the small index
	// available immediately and load one session's entities only when an API
	// actually needs them.
	loadMu         sync.Mutex
	loadedSessions map[string]bool
}

// legacyPersistPath resolves the legacy JSON file at use time. Resolving it
// dynamically keeps tests and embedded callers that set
// REASONIX_ORCHESTRATOR_DATA_DIR before constructing a handler isolated from
// the developer's real history.
func legacyPersistPath() string {
	return filepath.Join(orchestrator.DataRoot(), "store.json")
}

func newOrchestratorHandler(emitter event.Sink) *orchestratorHandler {
	h := &orchestratorHandler{
		store:          orchestrator.NewStore(),
		loadedSessions: make(map[string]bool),
	}
	h.store.SetEmitter(emitter)
	slog.Info("orchestrator: persistence root", "path", orchestrator.DataRoot(), "legacyPath", legacyPersistPath())
	// Load persisted data from disk.
	if err := h.store.Load(legacyPersistPath()); err != nil {
		slog.Warn("orchestrator: failed to load persisted data", "err", err)
	}
	// Migrate legacy data and load new directory-based data.
	if err := h.store.MigrateLegacyData(legacyPersistPath()); err != nil {
		slog.Warn("orchestrator: migration failed", "err", err)
	}
	// Loading every entity here blocks /presets, /nodes/types, and the history
	// list when the user has accumulated thousands of sessions. Load only the
	// index and session documents during construction; entity files are loaded
	// by ensureSessionLoaded for the selected session.
	if err := h.store.LoadIndexData(); err != nil {
		slog.Warn("orchestrator: failed to load session index", "err", err)
	}
	return h
}

func lazySessionID(path string) string {
	if !strings.HasPrefix(path, "/orch-sessions/") {
		return ""
	}
	rest := strings.TrimPrefix(path, "/orch-sessions/")
	parts := strings.Split(rest, "/")
	if len(parts) == 0 || parts[0] == "" {
		return ""
	}
	if len(parts) == 1 {
		return parts[0]
	}
	switch parts[1] {
	case "pipelines", "runs", "agents", "runtimes":
		return parts[0]
	default:
		return ""
	}
}

func (h *orchestratorHandler) markSessionLoaded(sessionID string) {
	h.loadMu.Lock()
	defer h.loadMu.Unlock()
	if h.loadedSessions == nil {
		h.loadedSessions = make(map[string]bool)
	}
	h.loadedSessions[sessionID] = true
}

func (h *orchestratorHandler) ensureSessionLoaded(sessionID string) error {
	if sessionID == "" {
		return nil
	}
	h.loadMu.Lock()
	defer h.loadMu.Unlock()
	if h.loadedSessions == nil {
		h.loadedSessions = make(map[string]bool)
	}
	if h.loadedSessions[sessionID] {
		return nil
	}
	if err := h.store.LoadSessionData(sessionID); err != nil {
		return err
	}
	h.loadedSessions[sessionID] = true
	return nil
}

// save persists the store to disk (best-effort, non-blocking).
func (h *orchestratorHandler) save() {
	if err := h.store.Save(legacyPersistPath()); err != nil {
		slog.Warn("orchestrator: failed to persist data", "err", err)
	}
}

// ServeHTTP dispatches to the correct method handler based on path.
func (h *orchestratorHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/orchestrator/api")
	path = strings.TrimSuffix(path, "/")

	slog.Debug("orchestrator api", "method", r.Method, "path", path)

	if sessionID := lazySessionID(path); sessionID != "" {
		if err := h.ensureSessionLoaded(sessionID); err != nil {
			writeErr(w, fmt.Sprintf("load session %q: %v", sessionID, err), http.StatusInternalServerError)
			return
		}
	}

	switch {
	// ── New Orchestration Session API ──
	case path == "/orch-sessions" && r.Method == http.MethodPost:
		h.createOrchSession(w, r)
	case path == "/orch-sessions" && r.Method == http.MethodGet:
		h.listOrchSessions(w, r)
	case path == "/orch-sessions/migrate" && r.Method == http.MethodPost:
		h.migrateOrchSession(w, r)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/orch-sessions/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/orch-sessions/")
		h.getOrchSession(w, r, id)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/orch-sessions/") && r.Method == http.MethodPut:
		id := strings.TrimPrefix(path, "/orch-sessions/")
		h.updateOrchSession(w, r, id)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/orch-sessions/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(path, "/orch-sessions/")
		h.deleteOrchSession(w, r, id)
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/conversation") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/conversation")
		h.getOrchConversation(w, r, sessionID)
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/conversation") && r.Method == http.MethodPut:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/conversation")
		h.putOrchConversation(w, r, sessionID)

	// Pipeline revisions within orchestration session
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/pipelines") && r.Method == http.MethodPost:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/pipelines")
		h.createPipelineRevision(w, r, sessionID)
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/pipelines") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/pipelines")
		h.listPipelineRevisions(w, r, sessionID)
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/pipelines/current") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/pipelines/current")
		h.getCurrentRevision(w, r, sessionID)
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/pipelines/current") && r.Method == http.MethodPut:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/pipelines/current")
		h.updateCurrentRevision(w, r, sessionID)

	// Runs within orchestration session
	case strings.HasPrefix(path, "/orch-sessions/") && strings.Contains(path, "/runs/") && strings.HasSuffix(path, "/iterations") && r.Method == http.MethodGet:
		parts := strings.Split(strings.TrimPrefix(path, "/orch-sessions/"), "/")
		if len(parts) != 4 || parts[1] != "runs" || parts[3] != "iterations" {
			writeErr(w, "bad iterations path", http.StatusBadRequest)
		} else {
			h.listOrchIterations(w, r, parts[0], parts[2])
		}
	case strings.Contains(path, "/runs/") && strings.HasSuffix(path, "/cancel") && r.Method == http.MethodPost:
		parts := strings.Split(strings.TrimPrefix(path, "/orch-sessions/"), "/")
		// /orch-sessions/{sessionID}/runs/{runID}/cancel
		if len(parts) != 4 || parts[1] != "runs" || parts[2] == "" || parts[3] != "cancel" {
			writeErr(w, "bad cancel path", http.StatusBadRequest)
		} else {
			h.cancelOrchRun(w, r, parts[0], parts[2])
		}
	case strings.HasSuffix(path, "/runs") && r.Method == http.MethodPost:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/runs")
		h.createOrchRun(w, r, sessionID)
	case strings.HasSuffix(path, "/runs") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/runs")
		h.listOrchRuns(w, r, sessionID)
	case strings.HasSuffix(path, "/runs/current") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/runs/current")
		h.getCurrentRun(w, r, sessionID)

	// Agent bindings within orchestration session
	case strings.HasSuffix(path, "/agents") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/agents")
		h.listOrchBindings(w, r, sessionID)
	case strings.Contains(path, "/agents/") && strings.HasSuffix(path, "/fork") && r.Method == http.MethodPost:
		// /orch-sessions/{sid}/agents/{bid}/fork
		parts := strings.Split(strings.TrimPrefix(path, "/orch-sessions/"), "/")
		if len(parts) == 4 {
			h.forkBinding(w, r, parts[0], parts[2])
		} else {
			http.Error(w, "bad path", http.StatusBadRequest)
		}

	// Runtimes within orchestration session
	case strings.HasSuffix(path, "/runtimes") && r.Method == http.MethodGet:
		sessionID := strings.TrimSuffix(strings.TrimPrefix(path, "/orch-sessions/"), "/runtimes")
		h.listOrchRuntimes(w, r, sessionID)

	// ── Legacy API (kept for backward compat) ──
	case path == "/pipelines" && r.Method == http.MethodGet:
		h.listPipelines(w, r)
	case path == "/pipelines" && r.Method == http.MethodPost:
		h.savePipeline(w, r)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/pipelines/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/pipelines/")
		h.getPipeline(w, r, id)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/pipelines/") && r.Method == http.MethodDelete:
		id := strings.TrimPrefix(path, "/pipelines/")
		h.deletePipeline(w, r, id)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/pipelines/") && (r.Method == http.MethodPut || r.Method == http.MethodPost):
		id := strings.TrimPrefix(path, "/pipelines/")
		h.updatePipeline(w, r, id)
	case strings.HasSuffix(path, "/execute") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/pipelines/"), "/execute")
		h.executePipeline(w, r, id)
	case path == "/runs" && r.Method == http.MethodGet:
		h.listRuns(w, r)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/runs/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/runs/")
		h.getRun(w, r, id)
	case strings.HasSuffix(path, "/cancel") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runs/"), "/cancel")
		h.cancelRun(w, r, id)
	case path == "/agents" && r.Method == http.MethodGet:
		h.listAgents(w, r)
	case path == "/skills" && r.Method == http.MethodGet:
		h.skills(w, r)
	case path == "/nodes/types" && r.Method == http.MethodGet:
		h.nodeTypes(w, r)
	case path == "/presets" && r.Method == http.MethodGet:
		h.presets(w, r)
	case path == "/stats" && r.Method == http.MethodGet:
		h.stats(w, r)
	case path == "/requirements/expand" && r.Method == http.MethodPost:
		h.expandRequirement(w, r)
	case path == "/requirements/understand" && r.Method == http.MethodPost:
		h.understandRequirement(w, r)
	case path == "/requirements/analyze" && r.Method == http.MethodPost:
		h.analyzeRequirement(w, r)
	case path == "/sessions" && r.Method == http.MethodGet:
		h.listSessions(w, r)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/sessions/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/sessions/")
		h.getSession(w, r, id)
	case strings.HasSuffix(path, "/state") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/sessions/"), "/state")
		h.saveSessionState(w, r, id)
	case strings.HasSuffix(path, "/state") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/sessions/"), "/state")
		h.loadSessionState(w, r, id)
	case path == "/runtimes" && r.Method == http.MethodGet:
		h.listRuntimes(w, r)
	case strings.HasSuffix(path, "/console") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/console")
		h.getRuntimeConsole(w, r, id)
	case strings.HasSuffix(path, "/message") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/message")
		h.sendRuntimeMessage(w, r, id)
	case strings.HasSuffix(path, "/interrupt") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/interrupt")
		h.interruptRuntime(w, r, id)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/runtimes/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/runtimes/")
		h.getRuntime(w, r, id)
	case strings.HasSuffix(path, "/stop") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/stop")
		h.stopRuntime(w, r, id)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// writeErr writes a text error response.
func writeErr(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// cleanJSON fixes corrupted UTF-8 bytes and broken \uXXXX escape sequences
// that models sometimes produce in Chinese text.
func cleanJSON(s string) string {
	// Replace invalid UTF-8 bytes with the Unicode replacement character
	s = strings.ToValidUTF8(s, "\uFFFD")
	// Fix broken \u sequences: \u followed by non-hex chars
	// Replace with the replacement character
	var buf strings.Builder
	buf.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] == '\\' && i+1 < len(s) && s[i+1] == 'u' {
			// Check if next 4 chars are valid hex
			if i+5 < len(s) {
				valid := true
				for j := i + 2; j <= i+5; j++ {
					c := s[j]
					if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
						valid = false
						break
					}
				}
				if valid {
					buf.WriteString(s[i : i+6])
					i += 5
					continue
				}
			}
			// Invalid \u sequence, skip it
			buf.WriteRune('\uFFFD')
			i++ // skip the 'u'
		} else {
			buf.WriteByte(s[i])
		}
	}
	return buf.String()
}

// listSkills returns the names from the shared version-aware Skill catalog.
func listSkills() []string {
	infos := orchestrator.ListAvailableSkills()
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		out = append(out, info.Name)
	}
	if len(out) == 0 {
		out = []string{"brainstorming", "executing-plans", "review-agent", "harness-eval", "loop"}
	}
	return out
}

func (h *orchestratorHandler) skills(w http.ResponseWriter, _ *http.Request) {
	infos := orchestrator.ListAvailableSkills()
	if infos == nil {
		infos = []orchestrator.SkillInfo{}
	}
	writeJSON(w, infos)
}

func (h *orchestratorHandler) listPipelines(w http.ResponseWriter, _ *http.Request) {
	pipelines := h.store.ListPipelines()
	if pipelines == nil {
		pipelines = []orchestrator.Pipeline{}
	}
	writeJSON(w, pipelines)
}

func (h *orchestratorHandler) savePipeline(w http.ResponseWriter, r *http.Request) {
	var payload orchestrator.PipelinePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeErr(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	pipe, err := h.store.SavePipeline(payload)
	if err != nil {
		writeErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.save()
	writeJSON(w, pipe)
}

func (h *orchestratorHandler) getPipeline(w http.ResponseWriter, _ *http.Request, id string) {
	pipe, ok := h.store.GetPipeline(id)
	if !ok {
		writeErr(w, fmt.Sprintf("pipeline %q not found", id), http.StatusNotFound)
		return
	}
	writeJSON(w, pipe)
}

func (h *orchestratorHandler) deletePipeline(w http.ResponseWriter, _ *http.Request, id string) {
	if err := h.store.DeletePipeline(id); err != nil {
		writeErr(w, err.Error(), http.StatusNotFound)
		return
	}
	h.save()
	w.WriteHeader(http.StatusNoContent)
}

func (h *orchestratorHandler) updatePipeline(w http.ResponseWriter, r *http.Request, id string) {
	var payload orchestrator.PipelinePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		writeErr(w, fmt.Sprintf("invalid request body: %v", err), http.StatusBadRequest)
		return
	}
	pipe, err := h.store.UpdatePipeline(id, payload)
	if err != nil {
		writeErr(w, err.Error(), http.StatusNotFound)
		return
	}
	h.save()
	writeJSON(w, pipe)
}

func (h *orchestratorHandler) executePipeline(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		Task string `json:"task"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	// Use background context for pipeline execution (request context cancels when handler returns)
	run, err := h.store.ExecutePipeline(context.Background(), id, body.Task)
	if err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.save()
	// ExecutePipeline returns a live run that is updated asynchronously. Read a
	// detached snapshot before encoding it so the response cannot race with the
	// executor mutating NodeStates.
	if snapshot, ok := h.store.GetRun(run.ID); ok {
		writeJSON(w, snapshot)
	} else {
		writeJSON(w, *run)
	}
}

func (h *orchestratorHandler) listRuns(w http.ResponseWriter, _ *http.Request) {
	runs := h.store.ListRuns()
	if runs == nil {
		runs = []orchestrator.PipelineRun{}
	}
	writeJSON(w, runs)
}

func (h *orchestratorHandler) getRun(w http.ResponseWriter, _ *http.Request, id string) {
	run, ok := h.store.GetRun(id)
	if !ok {
		writeErr(w, fmt.Sprintf("run %q not found", id), http.StatusNotFound)
		return
	}
	writeJSON(w, run)
}

func (h *orchestratorHandler) cancelRun(w http.ResponseWriter, _ *http.Request, id string) {
	if err := h.store.CancelRun(id); err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.save()
	w.WriteHeader(http.StatusNoContent)
}

func (h *orchestratorHandler) listAgents(w http.ResponseWriter, _ *http.Request) {
	agents := h.store.ListAgents()
	if agents == nil {
		agents = []orchestrator.AgentInstance{}
	}
	writeJSON(w, agents)
}

func (h *orchestratorHandler) nodeTypes(w http.ResponseWriter, _ *http.Request) {
	types := []orchestrator.NodeTypeInfo{
		{
			Type:   orchestrator.NodeArchitect,
			Label:  "架构师",
			Models: []string{"deepseek-pro", "deepseek-flash", "deepseek", "mimo-v2.5-pro", "mimo-v2.5", "xiaomi/mimo-v2.5", "ccs", "o3", "codex-default"},
			ModelsByExecutor: map[orchestrator.ExecutorType][]string{
				orchestrator.ExecutorReasonix: {"deepseek-pro", "deepseek-flash", "deepseek"},
				orchestrator.ExecutorMimo:     {"mimo-v2.5-pro", "mimo-v2.5", "xiaomi/mimo-v2.5"},
				orchestrator.ExecutorCodex:    {"ccs", "o3", "codex-default"},
				orchestrator.ExecutorClaude:   {"ccs", "opus", "sonnet", "haiku", "claude-fable-5"},
			},
			Skills:    listSkills(),
			Executors: []orchestrator.ExecutorType{orchestrator.ExecutorReasonix, orchestrator.ExecutorMimo, orchestrator.ExecutorCodex, orchestrator.ExecutorClaude},
		},
		{
			Type:   orchestrator.NodeReviewer,
			Label:  "审查者",
			Models: []string{"deepseek-flash", "deepseek", "xiaomi/mimo-v2.5"},
			ModelsByExecutor: map[orchestrator.ExecutorType][]string{
				orchestrator.ExecutorReasonix: {"deepseek-flash", "deepseek"},
				orchestrator.ExecutorMimo:     {"xiaomi/mimo-v2.5"},
				orchestrator.ExecutorCodex:    {"ccs", "o3", "codex-default"},
				orchestrator.ExecutorClaude:   {"ccs", "opus", "sonnet", "haiku", "claude-fable-5"},
			},
			Skills:    listSkills(),
			Executors: []orchestrator.ExecutorType{orchestrator.ExecutorReasonix, orchestrator.ExecutorMimo, orchestrator.ExecutorCodex, orchestrator.ExecutorClaude},
		},
		{
			Type:   orchestrator.NodeExecutor,
			Label:  "执行者",
			Models: []string{"deepseek-flash", "deepseek-pro", "xiaomi/mimo-v2.5", "xiaomi/mimo-v2.5-pro", "o3", "codex-default"},
			ModelsByExecutor: map[orchestrator.ExecutorType][]string{
				orchestrator.ExecutorReasonix: {"deepseek-flash", "deepseek-pro"},
				orchestrator.ExecutorMimo:     {"xiaomi/mimo-v2.5", "xiaomi/mimo-v2.5-pro"},
				orchestrator.ExecutorCodex:    {"ccs", "o3", "codex-default"},
				orchestrator.ExecutorClaude:   {"ccs", "opus", "sonnet", "haiku", "claude-fable-5"},
			},
			Skills:    listSkills(),
			Executors: []orchestrator.ExecutorType{orchestrator.ExecutorReasonix, orchestrator.ExecutorMimo, orchestrator.ExecutorCodex, orchestrator.ExecutorClaude},
		},
	}
	writeJSON(w, types)
}

func (h *orchestratorHandler) presets(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, orchestrator.Presets())
}

func (h *orchestratorHandler) stats(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, h.store.GetStats())
}

func (h *orchestratorHandler) listSessions(w http.ResponseWriter, _ *http.Request) {
	sessions := h.store.ListSessions()
	if sessions == nil {
		sessions = []orchestrator.Session{}
	}
	writeJSON(w, sessions)
}

func (h *orchestratorHandler) getSession(w http.ResponseWriter, _ *http.Request, id string) {
	sess, runs, ok := h.store.GetSession(id)
	if !ok {
		writeErr(w, fmt.Sprintf("session %q not found", id), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{"session": sess, "runs": runs})
}

func (h *orchestratorHandler) saveSessionState(w http.ResponseWriter, r *http.Request, id string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("saveSessionState: read body failed", "err", err)
		writeErr(w, "failed to read body", http.StatusBadRequest)
		return
	}
	slog.Info("saveSessionState", "id", id, "bodyLen", len(body), "bodySnippet", truncate(string(body), 200))
	var state orchestrator.SessionState
	if err := json.Unmarshal(body, &state); err != nil {
		slog.Error("saveSessionState: decode failed", "err", err, "bodyLen", len(body), "bodyFirst500", string(body[:min(len(body), 500)]))
		writeErr(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	for i := range state.Nodes {
		if state.Nodes[i].Executor == "" {
			state.Nodes[i].Executor = orchestrator.ExecutorReasonix
		}
		if state.Nodes[i].Mode == "" {
			state.Nodes[i].Mode = "serve"
		}
	}
	for i := range state.Edges {
		if state.Edges[i].ID == "" {
			state.Edges[i].ID = fmt.Sprintf("e-%s-%s-%d", state.Edges[i].FromID, state.Edges[i].ToID, i)
		}
	}
	slog.Info("saveSessionState: decoded OK", "nodes", len(state.Nodes), "edges", len(state.Edges), "chatMsgs", len(state.ChatMessages))
	if err := h.store.SaveSessionState(id, state); err != nil {
		writeErr(w, "failed to save: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *orchestratorHandler) loadSessionState(w http.ResponseWriter, _ *http.Request, id string) {
	state, err := h.store.LoadSessionState(id)
	if err != nil {
		writeErr(w, "not found", http.StatusNotFound)
		return
	}
	writeJSON(w, state)
}

func (h *orchestratorHandler) listRuntimes(w http.ResponseWriter, _ *http.Request) {
	var all []*orchestrator.RuntimeState
	all = append(all, orchestrator.ListMimoRuntimes()...)
	all = append(all, orchestrator.ListReasonixRuntimes()...)
	all = append(all, orchestrator.ListCodexRuntimes()...)
	all = append(all, orchestrator.ListClaudeRuntimes()...)
	writeJSON(w, all)
}

func (h *orchestratorHandler) getRuntime(w http.ResponseWriter, _ *http.Request, id string) {
	if rt, ok := orchestrator.GetMimoRuntime(id); ok {
		writeJSON(w, rt)
		return
	}
	if rt, ok := orchestrator.GetReasonixRuntime(id); ok {
		writeJSON(w, rt)
		return
	}
	if rt, ok := orchestrator.GetCodexRuntime(id); ok {
		writeJSON(w, rt)
		return
	}
	if rt, ok := orchestrator.GetClaudeRuntime(id); ok {
		writeJSON(w, rt)
		return
	}
	writeErr(w, "not found", http.StatusNotFound)
}

func (h *orchestratorHandler) stopRuntime(w http.ResponseWriter, _ *http.Request, id string) {
	if err := orchestrator.StopMimoRuntime(id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := orchestrator.StopReasonixRuntime(id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := orchestrator.StopCodexRuntime(id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := orchestrator.StopClaudeRuntime(id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeErr(w, "runtime not found", http.StatusNotFound)
}

// getRuntimeConsole returns the server-proxied Codex WebSocket console state.
// Only Codex retained runtimes currently expose this endpoint; other providers
// keep their existing browser/local-history integrations.
func (h *orchestratorHandler) getRuntimeConsole(w http.ResponseWriter, _ *http.Request, id string) {
	if snapshot, ok := orchestrator.GetMimoRuntimeConsole(id); ok {
		writeJSON(w, snapshot)
		return
	}
	if snapshot, ok := orchestrator.GetClaudeRuntimeConsole(id); ok {
		writeJSON(w, snapshot)
		return
	}
	snapshot, ok := orchestrator.GetCodexRuntimeConsole(id)
	if !ok {
		writeErr(w, "runtime console not found", http.StatusNotFound)
		return
	}
	writeJSON(w, snapshot)
}

func (h *orchestratorHandler) sendRuntimeMessage(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Text) == "" {
		writeErr(w, "text is required", http.StatusBadRequest)
		return
	}
	turnID, err := orchestrator.SendMimoRuntimeMessage(r.Context(), id, body.Text)
	if err == nil {
		writeJSON(w, map[string]string{"turnID": turnID})
		return
	}
	turnID, err = orchestrator.SendCodexRuntimeMessage(r.Context(), id, body.Text)
	if err == nil {
		writeJSON(w, map[string]string{"turnID": turnID})
		return
	}
	turnID, err = orchestrator.SendClaudeRuntimeMessage(r.Context(), id, body.Text)
	if err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"turnID": turnID})
}

func (h *orchestratorHandler) interruptRuntime(w http.ResponseWriter, r *http.Request, id string) {
	if err := orchestrator.InterruptMimoRuntime(r.Context(), id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := orchestrator.InterruptCodexRuntime(r.Context(), id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := orchestrator.InterruptClaudeRuntime(r.Context(), id); err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func enforceGeneratedRoleBoundary(agent, roleDesc string) string {
	role := strings.ToLower(strings.TrimSpace(agent))
	boundary := ""
	switch {
	case strings.Contains(role, "架构师") || strings.Contains(role, "architect"):
		boundary = "本节点只负责当前调用的方案设计和交接：禁止写代码、修改文件或执行实现命令；禁止自行设定新的 Goal，禁止在节点内部循环/迭代/执行三轮，禁止自己审查自己。输出当前轮方案后立即交给下游执行者；Loop 由 Orchestrator 在节点外部控制。"
	case strings.Contains(role, "执行者") || strings.Contains(role, "executor") || strings.Contains(role, "developer") || strings.Contains(role, "implementer"):
		boundary = "本节点只执行当前轮收到的上游方案：禁止自行设置 Goal，禁止创建内部 Loop 或自行执行三轮，禁止代替审查者作结论；完成当前轮后立即输出结果交给审查者，下一轮由 Orchestrator 控制。"
	case strings.Contains(role, "审查者") || strings.Contains(role, "审查") || strings.Contains(role, "reviewer") || strings.Contains(role, "review"):
		boundary = "本节点只审查当前轮：禁止修改、创建、删除文件，禁止执行写入命令，禁止自行设置 Goal 或内部循环；只输出当前轮的 pass/revise/blocked 结论，下一轮由 Orchestrator 控制。"
	}
	roleDesc = strings.TrimSpace(roleDesc)
	if boundary == "" {
		return roleDesc
	}
	if roleDesc == "" {
		return "【固定职责边界】\n" + boundary
	}
	return roleDesc + "\n\n【固定职责边界（不得覆盖）】\n" + boundary
}

// analyzeRequirement analyzes a user requirement and suggests pipeline configuration.
// Returns structured requirement + suggested node roles.
func (h *orchestratorHandler) analyzeRequirement(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text    string `json:"text"`
		History []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"history"`
		PipelineInfo string `json:"pipelineInfo"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Text == "" {
		writeErr(w, "text is required", http.StatusBadRequest)
		return
	}

	// Build conversation context
	historyText := ""
	for _, msg := range body.History {
		role := "用户"
		if msg.Role == "assistant" {
			role = "Flash"
		}
		historyText += fmt.Sprintf("[%s]: %s\n", role, msg.Content)
	}
	historyText += fmt.Sprintf("[用户]: %s", body.Text)
	if body.PipelineInfo != "" {
		historyText += body.PipelineInfo
	}

	skillCatalog := orchestrator.SkillCatalogSummary()
	prompt := `你是一个需求分析助手。你需要：
1. 阅读完整对话历史，理解用户的需求和上下文
2. 如果用户在后续消息中修改了需求，要结合之前的讨论来理解
3. 改写需求为结构化文档（rewritten字段）
4. 分解为可执行的步骤
5. 设计步骤之间的依赖关系（串行/并行/汇聚）
6. 为每个步骤分配合适的Agent角色
7. 如果需求提到“循环、迭代、反复执行、审查后修改、直到通过”等 Loop 语义，必须把它设计成“运行时循环”，不能把每一轮展开成多组节点
8. 每个 roleDesc 只描述该节点一次调用的职责，不得让 Agent 自己设定 Goal、自己循环三轮、自己执行完整流水线或执行完后才交接；这些都由 Orchestrator 负责
9. Skill 只能从下面的真实目录中选择，不能编造名称；每个节点最多一个 Skill，没有合适的就留空。Reviewer 只能选择 review-agent/code-review 类审查 Skill，绝对不能选择 loop，因为 loop 是定时调度器而不是审查器。

可用 Skill 目录：
` + skillCatalog + `

完整对话历史：
` + historyText + `

请严格输出以下JSON格式（不要加markdown代码块标记，直接输出JSON）：
{
  "analysis": "一句话总结用户的需求",
  "rewritten": "改写后的结构化需求文档（包含背景、目标、约束、验收标准）",
  "steps": [
    {"id": "s1", "step": "步骤描述", "agent": "架构师/执行者/审查者", "model": "模型名", "executor": "reasonix或mimo", "mode": "serve或run", "skill": "可选skill名称，无则留空", "roleDesc": "这个Agent的具体职责，要详细，包括输入输出格式"}
  ],
  "edges": [
    {"from": "s1", "to": "s2"},
    {"from": "s1", "to": "s3"},
    {"from": "s2", "to": "s4"},
    {"from": "s3", "to": "s4"}
  ],
  "suggestion": "对用户的建议（可选）",
  "loopConfig": {
    "enabled": false,
    "mode": "review_decides",
    "maxIterations": 3,
    "fixedIterations": 0,
    "reviewNodeID": "",
    "protocol": "loop-review-v1"
  }
}

模型选择原则（按成本优先，能用便宜的就不用贵的）：
- 架构师：deepseek-pro（需要强推理能力时才用）
- 执行者：xiaomi/mimo-v2.5（性价比最高）或 deepseek-flash（轻量任务）
- 审查者：deepseek-flash（最便宜，审查足够）
- 只有真正需要强推理的任务才用 deepseek-pro，不要默认全用pro

执行器选择原则：
- deepseek 模型（deepseek-pro, deepseek-flash, deepseek）→ executor: "reasonix"
- xiaomi 模型（xiaomi/mimo-v2.5, xiaomi/mimo-v2.5-pro）→ executor: "mimo"

运行模式选择原则：
- 默认优先使用 mode: "serve"
- 只有一次性、无状态、明确不需要复用上下文时，才使用 mode: "run"
- 当前多节点流水线、可恢复编排、需要查看运行地址时，都应该优先给出 mode: "serve"

边的类型说明（省略type默认为serial）：
- 无type或serial: A完成后B才开始
- parallel: A和B独立执行，无依赖
- converge: A和B都完成后C才开始

设计原则：
- 独立的任务应该并行（如同时调研两个方向）
- 有依赖的任务必须串行（如先设计后实现）
- 汇聚点用于合并多个并行任务的输出

节点任务设计原则（非常重要的约束）：
- 每个节点的任务必须有明确的、有价值的输出目标
- 不要让节点重复已有信息（如已经读过的文件不需要再输出一遍完整内容）
- 节点之间应该有清晰的数据流：上游输出 → 下游输入
- 架构师节点只产出方案和实施清单，不写代码、不执行实现、不在内部循环；输出后立即交给执行者
- 执行者节点应该产出新内容（代码、文档、分析），而不是重复读取
- 执行者只处理当前轮，不得自己设置 Goal 或模拟下一轮
- 审查者节点应该基于上游输出做判断，不需要重新读取原始文件
- 如果一个节点的任务只是"读取文件"，那它应该同时做分析或处理，不能只读不产出
- 审查者应该在所有实现完成后汇聚审查

Loop 设计规则（必须遵守）：
- Loop 是运行时对同一份 Pipeline DAG 重复执行，不是把多轮复制到 Canvas。
- Canvas 只画一轮基础 DAG；不要添加从审查者回到执行者/架构师的回边，也不要画重复节点。
- 第一轮允许架构师产出一次方案；第二轮及以后 Orchestrator 会复用该方案，不能再生成或执行新的架构师节点。
- 只生成一组基础节点。例如“架构师 → 执行者 → 审查者，最多 3 轮”只能生成 3 个节点和 2 条边；绝对不能生成 9 个节点、3 组相同角色或把“第1轮/第2轮/第3轮”写进节点。
- review_decides：审查者输出 pass/revise/blocked；revise 由 Orchestrator 重新调度同一组基础节点开始下一轮，pass 提前结束，blocked 终止。审查者不能自己调用下一轮。
- fixed：同一组基础节点精确执行 N 轮；每轮仍然执行审查者，pass 也不能提前结束。
- loopConfig.enabled=true 时，steps 中必须恰好有一个 type=reviewer 的审查节点，并在 loopConfig.reviewNodeID 中填写该节点的 step id；如果需求没有明确要求 Loop，loopConfig.enabled=false。
- 不要为了表达 Loop 增加回边；Canvas 只表示一轮基础 DAG，循环由运行时状态机执行。
- loopConfig 的轮数必须是 1 到 10 的整数；review_decides 填 maxIterations，fixed 填 fixedIterations。

Agent角色:
- 架构师(pro)：分析需求、设计方案、定义接口、评估质量
- 执行者(mimo)：读写文件、执行命令、实现功能
- 审查者(flash)：审查代码、发现问题、给出建议

每个节点的id用s1, s2, s3...格式，edges引用这些id。
再次强调：如果是 Loop，返回的 steps/edges 只能描述一轮，不能展开多轮。`

	bin := "reasonix"
	workDir := "."
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Join(filepath.Dir(exe), "reasonix.exe")
		if _, err := os.Stat(bin); err != nil {
			bin = "reasonix"
		}
		workDir = filepath.Dir(exe)
	}
	// Requirement analysis is backed by a model subprocess.  It can take
	// longer than the 15s CRUD/API deadline, especially on the first request
	// after startup.  Keep a finite server-side deadline, but do not let the
	// generic frontend timeout cancel the process before it has a chance to
	// return structured JSON.
	const analysisTimeout = 4 * time.Minute
	ctx, cancel := context.WithTimeout(r.Context(), analysisTimeout)
	defer cancel()
	// Log input dimensions for debugging second-call failures
	slog.Info("analyze: starting", "history_count", len(body.History), "prompt_len", len(prompt), "text_len", len(body.Text))
	// Pass prompt via stdin to avoid Windows 32K command-line length limit.
	cmd := exec.CommandContext(ctx, bin, "run", "--model", "deepseek-flash")
	cmd.Dir = workDir
	cmd.Stdin = strings.NewReader(prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = err.Error()
		}
		isTimeout := ctx.Err() == context.DeadlineExceeded
		slog.Error("analyze failed", "err", errMsg, "timeout", isTimeout, "bin", bin, "stdout", truncate(stdout.String(), 200))
		writeErr(w, "analysis failed: "+errMsg, http.StatusInternalServerError)
		return
	}
	slog.Info("analyze: subprocess completed", "stdout_len", stdout.Len())

	// Parse the JSON response
	// reasonix run outputs thinking text BEFORE the JSON and token stats AFTER it.
	// Find the LAST complete JSON object containing "analysis" key to avoid
	// matching stray { from tool dispatch lines.
	raw := stdout.String()
	var jsonStr string
	// Scan for all top-level JSON objects and pick the last one with "analysis"
	for i := 0; i < len(raw); i++ {
		if raw[i] != '{' {
			continue
		}
		// Find matching closing brace
		depth := 0
		end := -1
		for j := i; j < len(raw); j++ {
			if raw[j] == '{' {
				depth++
			} else if raw[j] == '}' {
				depth--
				if depth == 0 {
					end = j
					break
				}
			}
		}
		if end < 0 {
			continue
		}
		candidate := raw[i : end+1]
		if strings.Contains(candidate, `"analysis"`) {
			jsonStr = candidate
		}
		i = end // skip past this object
	}
	if jsonStr == "" {
		slog.Error("analyze: no JSON found", "raw", truncate(raw, 500))
		writeErr(w, "no JSON in analysis response", http.StatusInternalServerError)
		return
	}
	// Fix corrupted UTF-8 and escape sequences from model output
	raw = cleanJSON(jsonStr)

	var result struct {
		Analysis  string `json:"analysis"`
		Rewritten string `json:"rewritten"`
		Steps     []struct {
			ID       string `json:"id"`
			Step     string `json:"step"`
			Agent    string `json:"agent"`
			Model    string `json:"model"`
			Skill    string `json:"skill"`
			RoleDesc string `json:"roleDesc"`
		} `json:"steps"`
		Edges []struct {
			From string `json:"from"`
			To   string `json:"to"`
			Type string `json:"type"`
		} `json:"edges"`
		Suggestion string                   `json:"suggestion"`
		LoopConfig *orchestrator.LoopConfig `json:"loopConfig"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		slog.Error("analyze: failed to parse Flash response", "err", err, "raw", raw)
		writeErr(w, "failed to parse analysis", http.StatusInternalServerError)
		return
	}
	for i := range result.Steps {
		result.Steps[i].RoleDesc = enforceGeneratedRoleBoundary(result.Steps[i].Agent, result.Steps[i].RoleDesc)
	}

	writeJSON(w, result)
}

func (h *orchestratorHandler) expandRequirement(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text string `json:"text"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Text == "" {
		writeErr(w, "text is required", http.StatusBadRequest)
		return
	}

	prompt := `请将以下口语化需求整理为结构化需求文档。保持简洁，输出纯文本。

格式：
## 背景
[为什么要做这个]

## 目标
[要实现什么]

## 约束
[技术限制、时间限制等]

## 验收标准
[怎么算完成]

原始需求：
` + body.Text

	// Find the reasonix binary.
	bin := "reasonix"
	workDir := filepath.Dir(os.Args[0])
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Join(filepath.Dir(exe), "reasonix.exe")
		if _, err := os.Stat(bin); err != nil {
			bin = "reasonix"
		}
	}

	cmd := exec.Command(bin, "run", "--model", "deepseek-flash", prompt)
	cmd.Dir = workDir
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = err.Error()
		}
		slog.Error("expand requirement failed", "err", errMsg)
		writeJSON(w, map[string]string{"expanded": body.Text}) // fallback to original
		return
	}

	expanded := strings.TrimSpace(stdout.String())
	if expanded == "" {
		expanded = body.Text
	}
	writeJSON(w, map[string]string{"expanded": expanded})
}

func (h *orchestratorHandler) understandRequirement(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text  string `json:"text"`
		Nodes []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
			Type string `json:"type"`
			Role string `json:"role"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Text == "" {
		writeErr(w, "text is required", http.StatusBadRequest)
		return
	}

	// Build a prompt for Flash to understand the requirement and optimize node roles.
	nodesDesc := ""
	for _, n := range body.Nodes {
		nodesDesc += fmt.Sprintf("- %s (%s): 当前角色描述: %s\n", n.Name, n.Type, n.Role)
	}

	prompt := `你是一个需求理解助手。请分析以下需求，并为每个协作节点优化其角色描述。

需求：
` + body.Text + `

当前节点配置：
` + nodesDesc + `

请输出JSON格式（不要加markdown代码块标记）：
{
  "expanded": "结构化的需求描述（背景、目标、约束、验收标准）",
  "nodeRoles": [
    {"id": "节点ID", "roleDesc": "优化后的角色描述，要具体、可执行"}
  ]
}

要求：
1. 角色描述要具体，告诉每个节点该做什么、输出什么格式
2. 不要笼统地说"实现功能"，要说出具体步骤
3. 架构师只负责设计，不写代码
4. 执行者负责写代码，输出完整代码
5. 审查者只审查不修改`

	bin := "reasonix"
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Join(filepath.Dir(exe), "reasonix.exe")
		if _, err := os.Stat(bin); err != nil {
			bin = "reasonix"
		}
	}
	cmd := exec.Command(bin, "run", "--model", "deepseek-flash", prompt)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		errMsg := stderr.String()
		if errMsg == "" {
			errMsg = err.Error()
		}
		slog.Error("understand requirement failed", "err", errMsg)
		// Fallback: return original text and roles
		roles := make([]map[string]string, len(body.Nodes))
		for i, n := range body.Nodes {
			roles[i] = map[string]string{"id": n.ID, "roleDesc": n.Role}
		}
		writeJSON(w, map[string]interface{}{"expanded": body.Text, "nodeRoles": roles})
		return
	}

	// Parse the JSON response from Flash
	raw := strings.TrimSpace(stdout.String())
	// Remove markdown code block if present
	raw = strings.TrimPrefix(raw, "```json")
	raw = strings.TrimPrefix(raw, "```")
	raw = strings.TrimSuffix(raw, "```")
	raw = strings.TrimSpace(raw)

	var result struct {
		Expanded  string `json:"expanded"`
		NodeRoles []struct {
			ID       string `json:"id"`
			RoleDesc string `json:"roleDesc"`
		} `json:"nodeRoles"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil {
		slog.Error("understand: failed to parse Flash response", "err", err, "raw", raw)
		// Fallback
		roles := make([]map[string]string, len(body.Nodes))
		for i, n := range body.Nodes {
			roles[i] = map[string]string{"id": n.ID, "roleDesc": n.Role}
		}
		writeJSON(w, map[string]interface{}{"expanded": body.Text, "nodeRoles": roles})
		return
	}

	writeJSON(w, map[string]interface{}{
		"expanded":  result.Expanded,
		"nodeRoles": result.NodeRoles,
	})
}

// ── New Orchestration Session API Handlers ──

func (h *orchestratorHandler) createOrchSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Title             string `json:"title"`
		Workspace         string `json:"workspace"`
		NativeSessionPath string `json:"nativeSessionPath"`
		NativeSessionName string `json:"nativeSessionName"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if body.Title == "" {
		body.Title = "新会话"
	}
	if body.NativeSessionPath != "" {
		if existing, ok := h.store.FindOrchSessionByNativePath(body.NativeSessionPath); ok {
			writeJSON(w, existing)
			return
		}
	}
	sess, err := h.store.CreateOrchSession(body.Title, body.Workspace)
	if err != nil {
		writeErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// The session was created in this live Store, so its entities are already
	// authoritative in memory. Do not immediately reload it from disk: the
	// recovery path intentionally marks running entities as interrupted, and
	// that would interrupt a run created moments later in the same process.
	h.markSessionLoaded(sess.ID)
	if body.NativeSessionPath != "" || body.NativeSessionName != "" {
		_ = h.store.UpdateOrchSession(sess.ID, func(s *orchestrator.OrchestrationSession) {
			s.NativeSessionPath = body.NativeSessionPath
			s.NativeSessionName = body.NativeSessionName
		})
		if updated, ok := h.store.GetOrchSession(sess.ID); ok {
			sess = updated
		}
	}
	writeJSON(w, sess)
}

func (h *orchestratorHandler) migrateOrchSession(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NativeSessionPath string `json:"nativeSessionPath"`
		NativeSessionName string `json:"nativeSessionName"`
		Workspace         string `json:"workspace"`
		Title             string `json:"title"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.NativeSessionPath == "" {
		writeErr(w, "nativeSessionPath is required", http.StatusBadRequest)
		return
	}
	if existing, ok := h.store.FindOrchSessionByNativePath(body.NativeSessionPath); ok {
		writeJSON(w, existing)
		return
	}
	if body.Title == "" {
		body.Title = body.NativeSessionName
	}
	if body.Title == "" {
		body.Title = "迁移会话"
	}
	sess, err := h.store.CreateOrchSession(body.Title, body.Workspace)
	if err != nil {
		writeErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	h.markSessionLoaded(sess.ID)
	if err := h.store.UpdateOrchSession(sess.ID, func(s *orchestrator.OrchestrationSession) {
		s.NativeSessionPath = body.NativeSessionPath
		s.NativeSessionName = body.NativeSessionName
	}); err != nil {
		writeErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sess, _ = h.store.GetOrchSession(sess.ID)
	writeJSON(w, sess)
}

func (h *orchestratorHandler) getOrchConversation(w http.ResponseWriter, _ *http.Request, id string) {
	sess, ok := h.store.GetOrchSession(id)
	if !ok {
		writeErr(w, "session not found", http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]interface{}{
		"requirementMessages": sess.RequirementMessages,
		"chatMessages":        sess.ChatMessages,
		"pipelineMessages":    sess.PipelineMessages,
	})
}

func (h *orchestratorHandler) putOrchConversation(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		RequirementMessages []orchestrator.ChatMsg `json:"requirementMessages"`
		ChatMessages        []orchestrator.ChatMsg `json:"chatMessages"`
		PipelineMessages    []orchestrator.ChatMsg `json:"pipelineMessages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, "invalid request body", http.StatusBadRequest)
		return
	}
	if err := h.store.UpdateOrchSession(id, func(sess *orchestrator.OrchestrationSession) {
		sess.RequirementMessages = body.RequirementMessages
		sess.ChatMessages = body.ChatMessages
		sess.PipelineMessages = body.PipelineMessages
	}); err != nil {
		writeErr(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *orchestratorHandler) listOrchSessions(w http.ResponseWriter, r *http.Request) {
	// History refresh is a hot path. Return only summary fields; full session
	// conversations and run arrays are loaded by GET /orch-sessions/{id} when a
	// user opens one card.
	sessions := h.store.ListOrchSessionSummaries()
	if limitText := r.URL.Query().Get("limit"); limitText != "" {
		if limit, err := strconv.Atoi(limitText); err == nil && limit > 0 && limit < len(sessions) {
			sessions = sessions[:limit]
		}
	}
	if sessions == nil {
		sessions = []orchestrator.OrchestrationSessionSummary{}
	}
	writeJSON(w, sessions)
}

func (h *orchestratorHandler) getOrchSession(w http.ResponseWriter, _ *http.Request, id string) {
	sess, ok := h.store.GetOrchSession(id)
	if !ok {
		writeErr(w, fmt.Sprintf("session %q not found", id), http.StatusNotFound)
		return
	}
	// Include current pipeline and run for convenience
	result := map[string]interface{}{
		"session": sess,
	}
	if rev, ok := h.store.GetCurrentRevision(id); ok {
		result["currentPipeline"] = rev
	}
	if sess.CurrentRunID != "" {
		if run, ok := h.store.GetRun(sess.CurrentRunID); ok {
			result["currentRun"] = run
		}
	}
	result["pipelines"] = h.store.ListPipelineRevisions(id)
	result["runs"] = h.store.ListRunsForSession(id)
	result["bindings"] = h.store.ListBindings(id)
	// RuntimeState is persisted independently from a run so a retained Codex
	// app-server can be reopened in the Runtime Console after a page refresh.
	// Return an ID-keyed object because the frontend backfills runtimes by nodeID.
	runtimes := make(map[string]orchestrator.RuntimeState)
	for _, runtime := range h.store.ListRuntimeStates(id) {
		runtimes[runtime.RuntimeID] = runtime
	}
	result["runtimeStates"] = runtimes
	writeJSON(w, result)
}

func (h *orchestratorHandler) updateOrchSession(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		Title         string  `json:"title,omitempty"`
		Status        string  `json:"status,omitempty"`
		Task          string  `json:"task,omitempty"`
		RewrittenTask string  `json:"rewrittenTask,omitempty"`
		Workspace     *string `json:"workspace,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, "invalid request body", http.StatusBadRequest)
		return
	}
	err := h.store.UpdateOrchSession(id, func(sess *orchestrator.OrchestrationSession) {
		if body.Title != "" {
			sess.Title = body.Title
		}
		if body.Status != "" {
			sess.Status = body.Status
		}
		if body.Task != "" {
			sess.ActiveTask = body.Task
		}
		if body.RewrittenTask != "" {
			sess.RewrittenTask = body.RewrittenTask
		}
		if body.Workspace != nil {
			sess.Workspace = strings.TrimSpace(*body.Workspace)
		}
	})
	if err != nil {
		writeErr(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *orchestratorHandler) deleteOrchSession(w http.ResponseWriter, _ *http.Request, id string) {
	if err := h.store.DeleteOrchSession(id); err != nil {
		writeErr(w, err.Error(), http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *orchestratorHandler) createPipelineRevision(w http.ResponseWriter, r *http.Request, sessionID string) {
	var body struct {
		Name       string                   `json:"name"`
		Nodes      []orchestrator.AgentNode `json:"nodes"`
		Edges      []orchestrator.Edge      `json:"edges"`
		Source     string                   `json:"source"`
		LoopConfig *orchestrator.LoopConfig `json:"loopConfig,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, "invalid request body", http.StatusBadRequest)
		return
	}
	cfg := body.LoopConfig
	if cfg == nil {
		cfg = &orchestrator.LoopConfig{}
	}
	if err := orchestrator.ValidateLoopConfig(cfg, body.Nodes); err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	normalized, err := orchestrator.NormalizeLoopConfig(cfg)
	if err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	rev, err := h.store.CreatePipelineRevision(sessionID, orchestrator.PipelineRevision{
		Name:       body.Name,
		Nodes:      body.Nodes,
		Edges:      body.Edges,
		Source:     body.Source,
		LoopConfig: normalized,
	})
	if err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, rev)
}

func (h *orchestratorHandler) listPipelineRevisions(w http.ResponseWriter, _ *http.Request, sessionID string) {
	revs := h.store.ListPipelineRevisions(sessionID)
	if revs == nil {
		revs = []orchestrator.PipelineRevision{}
	}
	writeJSON(w, revs)
}

func (h *orchestratorHandler) getCurrentRevision(w http.ResponseWriter, _ *http.Request, sessionID string) {
	rev, ok := h.store.GetCurrentRevision(sessionID)
	if !ok {
		writeErr(w, "no current pipeline", http.StatusNotFound)
		return
	}
	writeJSON(w, rev)
}

func (h *orchestratorHandler) updateCurrentRevision(w http.ResponseWriter, r *http.Request, sessionID string) {
	if _, ok := h.store.GetOrchSession(sessionID); !ok {
		writeErr(w, fmt.Sprintf("session %q not found", sessionID), http.StatusNotFound)
		return
	}
	var body struct {
		Name           string                   `json:"name,omitempty"`
		Nodes          []orchestrator.AgentNode `json:"nodes"`
		Edges          []orchestrator.Edge      `json:"edges"`
		Source         string                   `json:"source"`
		BaseRevisionID string                   `json:"baseRevisionID,omitempty"`
		LoopConfig     *orchestrator.LoopConfig `json:"loopConfig,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, "invalid request body", http.StatusBadRequest)
		return
	}
	// New clients always send loopConfig. Older clients that only send nodes and
	// edges must not accidentally disable a persisted Loop, so preserve the
	// current revision's config when the field is omitted.
	cfg := body.LoopConfig
	if cfg == nil {
		if current, ok := h.store.GetCurrentRevision(sessionID); ok {
			copied := current.LoopConfig
			cfg = &copied
		} else {
			cfg = &orchestrator.LoopConfig{}
		}
	}
	rev, err := h.store.UpdatePipelineRevisionWithLoopConfigIfCurrent(sessionID, body.BaseRevisionID, body.Name, body.Nodes, body.Edges, body.Source, cfg)
	if err != nil {
		status := http.StatusBadRequest
		if strings.Contains(err.Error(), "pipeline revision conflict") {
			status = http.StatusConflict
		}
		writeErr(w, err.Error(), status)
		return
	}
	writeJSON(w, rev)
}

func (h *orchestratorHandler) createOrchRun(w http.ResponseWriter, r *http.Request, sessionID string) {
	var body struct {
		Task               string `json:"task"`
		RewrittenTask      string `json:"rewrittenTask,omitempty"`
		Trigger            string `json:"trigger"`
		ParentRunID        string `json:"parentRunID,omitempty"`
		ReuseAgentSessions *bool  `json:"reuseAgentSessions,omitempty"`
		ResumeRunID        string `json:"resumeRunID,omitempty"`
		ContextPolicy      string `json:"contextPolicy,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Resume deliberately ignores task/configuration fields from the request;
	// ExecutePipelineV2 loads the original run and its revision as the source of
	// truth. It only needs the target session and run ID.
	if body.ResumeRunID != "" {
		run, err := h.store.ExecutePipelineV2(context.Background(), sessionID, "", "", "", orchestrator.ExecutionOptions{
			ResumeRunID: body.ResumeRunID,
			Trigger:     "resume",
		})
		if err != nil {
			writeErr(w, err.Error(), http.StatusBadRequest)
			return
		}
		if snapshot, ok := h.store.GetRun(run.ID); ok {
			writeJSON(w, snapshot)
		} else {
			writeJSON(w, *run)
		}
		return
	}

	// The browser normally sends both fields, but a resumed page or an older
	// client may omit them. Use the session's latest persisted requirement so a
	// run cannot silently execute stale or empty content.
	if sess, ok := h.store.GetOrchSession(sessionID); ok {
		if body.Task == "" {
			body.Task = sess.ActiveTask
		}
		if body.RewrittenTask == "" {
			body.RewrittenTask = sess.RewrittenTask
		}
	} else {
		writeErr(w, fmt.Sprintf("session %q not found", sessionID), http.StatusNotFound)
		return
	}
	if strings.TrimSpace(body.Task) == "" && strings.TrimSpace(body.RewrittenTask) == "" {
		writeErr(w, "task is required", http.StatusBadRequest)
		return
	}
	rev, ok := h.store.GetCurrentRevision(sessionID)
	if !ok {
		writeErr(w, "no pipeline revision found; create one first", http.StatusBadRequest)
		return
	}
	if body.Trigger == "" {
		body.Trigger = "manual"
	}
	opts := orchestrator.ExecutionOptions{
		Trigger:       body.Trigger,
		ParentRunID:   body.ParentRunID,
		ContextPolicy: body.ContextPolicy,
	}
	if body.ReuseAgentSessions != nil {
		opts.ReuseAgentSessions = *body.ReuseAgentSessions
	} else {
		opts.ReuseAgentSessions = true
	}
	run, err := h.store.ExecutePipelineV2(context.Background(), sessionID, rev.ID, body.Task, body.RewrittenTask, opts)
	if err != nil {
		writeErr(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if snapshot, ok := h.store.GetRun(run.ID); ok {
		writeJSON(w, snapshot)
	} else {
		writeJSON(w, *run)
	}
}

func (h *orchestratorHandler) listOrchIterations(w http.ResponseWriter, _ *http.Request, sessionID, runID string) {
	if _, ok := h.store.GetOrchSession(sessionID); !ok {
		writeErr(w, fmt.Sprintf("session %q not found", sessionID), http.StatusNotFound)
		return
	}
	run, ok := h.store.GetRun(runID)
	if !ok {
		writeErr(w, fmt.Sprintf("run %q not found", runID), http.StatusNotFound)
		return
	}
	if run.SessionID != sessionID {
		writeErr(w, fmt.Sprintf("run %q does not belong to session %q", runID, sessionID), http.StatusNotFound)
		return
	}
	iterations := h.store.ListIterations(runID)
	if iterations == nil {
		iterations = []orchestrator.LoopIteration{}
	}
	writeJSON(w, iterations)
}

func (h *orchestratorHandler) cancelOrchRun(w http.ResponseWriter, _ *http.Request, sessionID, runID string) {
	if _, ok := h.store.GetOrchSession(sessionID); !ok {
		writeErr(w, fmt.Sprintf("session %q not found", sessionID), http.StatusNotFound)
		return
	}
	run, ok := h.store.GetRun(runID)
	if !ok || run.SessionID != sessionID {
		writeErr(w, fmt.Sprintf("run %q not found in session %q", runID, sessionID), http.StatusNotFound)
		return
	}
	if err := h.store.CancelRun(runID); err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.save()
	writeJSON(w, map[string]any{"id": runID, "status": "canceling"})
}

func (h *orchestratorHandler) listOrchRuns(w http.ResponseWriter, _ *http.Request, sessionID string) {
	runs := h.store.ListRunsForSession(sessionID)
	if runs == nil {
		runs = []orchestrator.PipelineRun{}
	}
	writeJSON(w, runs)
}

func (h *orchestratorHandler) getCurrentRun(w http.ResponseWriter, _ *http.Request, sessionID string) {
	sess, ok := h.store.GetOrchSession(sessionID)
	if !ok {
		writeErr(w, "session not found", http.StatusNotFound)
		return
	}
	if sess.CurrentRunID == "" {
		writeErr(w, "no current run", http.StatusNotFound)
		return
	}
	run, ok := h.store.GetRun(sess.CurrentRunID)
	if !ok {
		writeErr(w, "run not found", http.StatusNotFound)
		return
	}
	writeJSON(w, run)
}

func (h *orchestratorHandler) listOrchBindings(w http.ResponseWriter, _ *http.Request, sessionID string) {
	bindings := h.store.ListBindings(sessionID)
	if bindings == nil {
		bindings = []orchestrator.AgentBinding{}
	}
	writeJSON(w, bindings)
}

func (h *orchestratorHandler) forkBinding(w http.ResponseWriter, r *http.Request, sessionID, bindingID string) {
	var body struct {
		Reason string `json:"reason"`
	}
	// body is optional
	_ = json.NewDecoder(r.Body).Decode(&body)

	binding, err := h.store.ForkBinding(bindingID, body.Reason)
	if err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, binding)
}

func (h *orchestratorHandler) listOrchRuntimes(w http.ResponseWriter, _ *http.Request, sessionID string) {
	if _, ok := h.store.GetOrchSession(sessionID); !ok {
		writeErr(w, "session not found", http.StatusNotFound)
		return
	}

	// Runtime identity (session/node/model/access mode) belongs to the persisted
	// orchestration record. The provider managers only own volatile lifecycle
	// fields, so returning their raw List() output used to erase nodeID/model and
	// made a refreshed canvas lose the Runtime Console button.
	byID := make(map[string]orchestrator.RuntimeState)
	for _, runtime := range h.store.ListRuntimeStates(sessionID) {
		byID[runtime.RuntimeID] = runtime
	}
	live := make([]*orchestrator.RuntimeState, 0)
	live = append(live, orchestrator.ListMimoRuntimes()...)
	live = append(live, orchestrator.ListReasonixRuntimes()...)
	live = append(live, orchestrator.ListCodexRuntimes()...)
	live = append(live, orchestrator.ListClaudeRuntimes()...)
	for _, runtime := range live {
		persisted, ok := byID[runtime.RuntimeID]
		if !ok {
			// A provider process without a persisted record is not safely attributable
			// to this session, therefore it must not leak into this session's UI.
			continue
		}
		if runtime.Endpoint != "" {
			persisted.Endpoint = runtime.Endpoint
		}
		if runtime.Port != 0 {
			persisted.Port = runtime.Port
		}
		if runtime.PID != 0 {
			persisted.PID = runtime.PID
		}
		if runtime.Status != "" {
			persisted.Status = runtime.Status
		}
		persisted.Error = runtime.Error
		if !runtime.LastActiveAt.IsZero() {
			persisted.LastActiveAt = runtime.LastActiveAt
		}
		if runtime.ThreadID != "" {
			persisted.ThreadID = runtime.ThreadID
		}
		persisted.TurnID = runtime.TurnID
		if runtime.AccessMode != "" {
			persisted.AccessMode = runtime.AccessMode
		}
		byID[runtime.RuntimeID] = persisted
	}

	all := make([]orchestrator.RuntimeState, 0, len(byID))
	for _, runtime := range byID {
		all = append(all, runtime)
	}
	writeJSON(w, all)
}
