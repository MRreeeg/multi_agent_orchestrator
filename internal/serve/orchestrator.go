package serve

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"reasonix/internal/config"
	"reasonix/internal/event"
	dshclient "reasonix/internal/executor/dsh"
	"reasonix/internal/orchestrator"
	"reasonix/internal/proc"
	"reasonix/internal/skill"
)

// orchestratorHandler routes /orchestrator/api/* requests to the store.
type orchestratorHandler struct {
	store *orchestrator.Store
	// emitter forwards progress/status events (e.g. analyze_progress) to the
	// SSE frontend. May be nil in tests.
	emitter event.Sink

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
		emitter:        emitter,
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
	case strings.HasPrefix(path, "/orch-sessions/") && strings.HasSuffix(path, "/runtimes") && r.Method == http.MethodGet:
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
	case strings.HasSuffix(path, "/detail") && strings.Count(path, "/") == 3 && strings.HasPrefix(path, "/runs/") && r.Method == http.MethodGet:
		h.getRunDetail(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "/runs/"), "/detail"))
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/runs/") && r.Method == http.MethodGet:
		h.getRun(w, r, strings.TrimPrefix(path, "/runs/"))
	case strings.HasSuffix(path, "/cancel") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runs/"), "/cancel")
		h.cancelRun(w, r, id)
	case strings.HasSuffix(path, "/analysis") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runs/"), "/analysis")
		h.analyzeRunProgress(w, r, id)
	case path == "/analysis/options" && r.Method == http.MethodGet:
		h.analysisOptions(w, r)
	case path == "/agents" && r.Method == http.MethodGet:
		h.listAgents(w, r)
	case path == "/skills" && r.Method == http.MethodGet:
		h.skills(w, r)
	case path == "/nodes/types" && r.Method == http.MethodGet:
		h.nodeTypes(w, r)
	case path == "/dsh-presets" && r.Method == http.MethodGet:
		h.dshPresets(w, r)
	case path == "/dsh-presets/install" && r.Method == http.MethodPost:
		h.dshPresetsInstall(w, r)
	case path == "/selfcheck" && r.Method == http.MethodGet:
		h.selfcheck(w, r)
	case path == "/presets" && r.Method == http.MethodGet:
		h.presets(w, r)
	case path == "/stats" && r.Method == http.MethodGet:
		h.stats(w, r)
	case path == "/requirements/expand" && r.Method == http.MethodPost:
		h.expandRequirement(w, r)
	case path == "/requirements/understand" && r.Method == http.MethodPost:
		h.understandRequirement(w, r)
	case path == "/upload-image" && r.Method == http.MethodPost:
		h.uploadImage(w, r)
	case strings.HasPrefix(path, "/images/") && r.Method == http.MethodGet:
		h.serveImage(w, r)
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
	case path == "/runtimes/cleanup" && r.Method == http.MethodPost:
		h.cleanupRuntimes(w, r)
	case strings.HasSuffix(path, "/console") && r.Method == http.MethodGet:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/console")
		h.getRuntimeConsole(w, r, id)
	case strings.HasSuffix(path, "/message") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/message")
		h.sendRuntimeMessage(w, r, id)
	case strings.HasSuffix(path, "/permission") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/permission")
		h.answerRuntimePermission(w, r, id)
	case strings.HasSuffix(path, "/interrupt") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/interrupt")
		h.interruptRuntime(w, r, id)
	case strings.Count(path, "/") == 2 && strings.HasPrefix(path, "/runtimes/") && r.Method == http.MethodGet:
		id := strings.TrimPrefix(path, "/runtimes/")
		h.getRuntime(w, r, id)
	case strings.HasSuffix(path, "/stop") && r.Method == http.MethodPost:
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/runtimes/"), "/stop")
		h.stopRuntime(w, r, id)
	case path == "/runtime/open" && r.Method == http.MethodPost:
		h.openRuntimeBrowser(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// emitAnalyzeProgress forwards one analyze progress event to the SSE frontend
// (no-op when the handler has no emitter, e.g. in tests).
func (h *orchestratorHandler) emitAnalyzeProgress(stage string, elapsedSec, attempt int, line string) {
	if h.emitter == nil {
		return
	}
	lineJSON, _ := json.Marshal(line)
	h.emitter.Emit(event.Event{
		Kind:   event.AnalyzeProgress,
		Text:   stage,
		Detail: fmt.Sprintf(`{"elapsed":%d,"attempt":%d,"line":%s}`, elapsedSec, attempt, lineJSON),
	})
}

// writeErr writes a text error response.
func writeErr(w http.ResponseWriter, msg string, code int) {
	http.Error(w, msg, code)
}

// ── Image attachment upload/serve (L-54) ──

const maxImageUploadBytes = 10 * 1024 * 1024

func imageAttachmentDir() string {
	return filepath.Join(orchestrator.DataRoot(), "attachments")
}

func allowedImageType(ct string) bool {
	switch ct {
	case "image/png", "image/jpeg", "image/gif", "image/webp", "image/bmp":
		return true
	}
	return false
}

// uploadImage persists a base64 image attachment and returns its id + readback URL.
func (h *orchestratorHandler) uploadImage(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Data string `json:"data"`
		Name string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Data == "" {
		writeErr(w, "data is required", http.StatusBadRequest)
		return
	}
	raw, err := base64.StdEncoding.DecodeString(body.Data)
	if err != nil {
		writeErr(w, "invalid base64", http.StatusBadRequest)
		return
	}
	if len(raw) > maxImageUploadBytes {
		writeErr(w, "image too large (max 10MB)", http.StatusBadRequest)
		return
	}
	ct := http.DetectContentType(raw)
	if !allowedImageType(ct) {
		writeErr(w, "unsupported image type: "+ct, http.StatusBadRequest)
		return
	}
	ext := ".png"
	if m, err := mime.ExtensionsByType(ct); err == nil && len(m) > 0 {
		ext = m[0]
	}
	var rnd [4]byte
	rand.Read(rnd[:])
	id := fmt.Sprintf("%d_%s", time.Now().UnixMilli(), hex.EncodeToString(rnd[:]))
	dir := imageAttachmentDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		writeErr(w, "cannot create attachment dir", http.StatusInternalServerError)
		return
	}
	if err := os.WriteFile(filepath.Join(dir, id+ext), raw, 0644); err != nil {
		writeErr(w, "cannot save image", http.StatusInternalServerError)
		return
	}
	writeJSON(w, map[string]string{
		"id":   id,
		"name": body.Name,
		"url":  "/orchestrator/api/images/" + id,
	})
}

// imageIDRe matches the ids produced by uploadImage (UnixMilli "_" hex4).
// Strict validation also rejects glob metacharacters, which would otherwise be
// interpolated into filepath.Glob and leak stored attachments.
var imageIDRe = regexp.MustCompile(`^[0-9]{13}_[0-9a-f]{8}$`)

// serveImage reads back an uploaded image by id.
func (h *orchestratorHandler) serveImage(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/orchestrator/api/images/")
	if !imageIDRe.MatchString(id) {
		writeErr(w, "bad image id", http.StatusBadRequest)
		return
	}
	matches, err := filepath.Glob(filepath.Join(imageAttachmentDir(), id+".*"))
	if err != nil || len(matches) == 0 {
		writeErr(w, "image not found", http.StatusNotFound)
		return
	}
	ct := mime.TypeByExtension(filepath.Ext(matches[0]))
	if ct == "" {
		ct = "application/octet-stream"
	}
	raw, err := os.ReadFile(matches[0])
	if err != nil {
		writeErr(w, "image not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", ct)
	w.Write(raw)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// runAnalysisExec dispatches one analysis prompt to the chosen executor.
// reasonix (default) keeps the stdin subprocess path: it streams thinking
// lines to onProgress and avoids the Windows 32K argv limit via stdin. The
// other executors go through the unified orchestrator executor registry in
// one-shot run mode; their final text is the analysis answer. The
// responsibility contract (the JSON schema) is always built server-side and
// passed as the prompt, so any executor/model combination stays compatible
// with the downstream parsers.
func runAnalysisExec(ctx context.Context, bin, workDir, prompt, executor, model, agent string, onProgress func(line string)) (string, string, error) {
	exType := strings.ToLower(strings.TrimSpace(executor))
	if exType == "" || exType == "reasonix" {
		return spawnReasonixAnalysis(ctx, bin, workDir, prompt, model, agent, onProgress)
	}
	exec := orchestrator.GetExecutor(orchestrator.ExecutorType(exType))
	if exec == nil {
		return "", "", fmt.Errorf("unknown executor %q", exType)
	}
	spec := orchestrator.ExecSpec{
		Workspace: workDir,
		Prompt:    prompt,
		ModelRef:  model,
		Mode:      "run",
		Executor:  exType,
		NodeID:    "analysis",
		MaxSteps:  25,
		// Headless has no interactive approval surface: the analysis subprocess
		// must never sit on a pending "ask". Trusted nodes map to
		// DSH_PERMISSION_MODE=danger-full-access (approval never).
		Trust: true,
	}
	// For dsh the analysis "agent persona" is a locally authored DSH preset id
	// (e.g. frontend-analyst); pass it through so the analysis runs under the
	// customized agent's persona and tool catalog.
	if exType == "dsh" {
		spec.DshPreset = strings.TrimSpace(agent)
	}
	res, err := exec.Execute(ctx, spec, nil)
	if err != nil && res == nil {
		return "", "", err
	}
	if res == nil {
		return "", "", fmt.Errorf("executor %q returned no result", exType)
	}
	out := strings.TrimSpace(res.FinalText)
	if out == "" {
		out = strings.TrimSpace(res.RawStdout)
	}
	return out, strings.TrimSpace(res.RawStderr), err
}

// spawnReasonixAnalysis runs a `reasonix run --model deepseek-flash` subprocess
// (or `reasonix subagent run <agent> --model <model>` when an analysis agent
// persona is selected) with the prompt on stdin. Every spawn hides the console
// window so the desktop app (GUI subsystem) does not flash a black window
// while analyzing. onProgress, when non-nil, is called with each non-empty
// stdout line while the subprocess runs (reasonix run streams thinking text to
// stdout before the final JSON), letting the frontend show live progress.
func spawnReasonixAnalysis(ctx context.Context, bin, workDir, prompt, model, agent string, onProgress func(line string)) (string, string, error) {
	if model == "" {
		model = "deepseek-flash"
	}
	var args []string
	if agent != "" {
		// A subagent profile supplies its own persona/tool preferences; the
		// analysis responsibility contract (the JSON schema below) is passed
		// as the task via stdin, so the two stay decoupled.
		args = []string{"subagent", "run", agent, "--model", model}
	} else {
		args = []string{"run", "--model", model}
	}
	cmd := exec.CommandContext(ctx, bin, args...)
	proc.HideWindow(cmd)
	cmd.Dir = workDir
	cmd.Stdin = strings.NewReader(prompt)
	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		return "", "", err
	}
	stderrPipe, err := cmd.StderrPipe()
	if err != nil {
		return "", "", err
	}
	if err := cmd.Start(); err != nil {
		return "", "", err
	}
	var stdout bytes.Buffer
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(stdoutPipe)
		sc.Buffer(make([]byte, 0, 1<<20), 4<<20)
		for sc.Scan() {
			line := sc.Text()
			stdout.WriteString(line + "\n")
			if onProgress != nil && strings.TrimSpace(line) != "" {
				onProgress(line)
			}
		}
	}()
	stderrBytes, _ := io.ReadAll(stderrPipe)
	<-done
	errMsg := string(stderrBytes)
	if err := cmd.Wait(); err != nil {
		if errMsg == "" {
			errMsg = err.Error()
		}
		return stdout.String(), errMsg, fmt.Errorf("%s", errMsg)
	}
	return stdout.String(), errMsg, nil
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

// getRunDetail returns one run plus the pipeline revision nodes it executed,
// so the frontend can render a clean per-agent breakdown (type/executor/model/
// label) alongside the run-level summary. The plain getRun endpoint keeps its
// old shape for compatibility.
func (h *orchestratorHandler) getRunDetail(w http.ResponseWriter, _ *http.Request, id string) {
	run, ok := h.store.GetRun(id)
	if !ok {
		writeErr(w, fmt.Sprintf("run %q not found", id), http.StatusNotFound)
		return
	}
	var nodes []orchestrator.AgentNode
	if run.PipelineRevisionID != "" {
		if rev, revOK := h.store.GetPipelineRevision(run.PipelineRevisionID); revOK {
			nodes = rev.Nodes
		}
	}
	writeJSON(w, map[string]any{"run": run, "nodes": nodes})
}

func (h *orchestratorHandler) cancelRun(w http.ResponseWriter, _ *http.Request, id string) {
	if err := h.store.CancelRun(id); err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	h.save()
	w.WriteHeader(http.StatusNoContent)
}

// analyzeRunProgress returns an AI summary of the current Loop execution:
// what each node produced, where it stands, blockers and suggested next steps.
// It reuses the same model subprocess mechanism as requirement analysis.
func (h *orchestratorHandler) analyzeRunProgress(w http.ResponseWriter, r *http.Request, id string) {
	run, ok := h.store.GetRun(id)
	if !ok {
		writeErr(w, fmt.Sprintf("run %q not found", id), http.StatusNotFound)
		return
	}
	var body struct {
		Executor string `json:"executor"`
		Model    string `json:"model"`
		Agent    string `json:"agent"`
	}
	json.NewDecoder(r.Body).Decode(&body) // optional; empty body keeps defaults
	attempts := h.store.ListAttempts(id)
	iterations := h.store.ListIterations(id)

	typeLbl := map[string]string{"architect": "架构师", "executor": "执行者", "reviewer": "审查者"}
	var nodesText []string
	order := 0
	for _, att := range attempts {
		if att.NodeID == "" {
			continue
		}
		order++
		label := typeLbl[att.NodeID]
		if label == "" {
			label = att.NodeID
		}
		output := strings.TrimSpace(att.Output)
		if len(output) > 600 {
			output = output[:600] + "…"
		}
		nodesText = append(nodesText, fmt.Sprintf("%d. 节点ID=%s 类型=%s 状态=%s\n输出摘要: %s", order, att.NodeID, label, att.Status, output))
	}
	iterText := "暂无迭代记录"
	if len(iterations) > 0 {
		var parts []string
		for _, it := range iterations {
			parts = append(parts, fmt.Sprintf("第%d轮 状态=%s", it.Number, it.Status))
		}
		iterText = strings.Join(parts, "; ")
	}
	reviewText := "暂无"
	if run.FinalReview != nil {
		rv := *run.FinalReview
		reviewText = fmt.Sprintf("decision=%s confidence=%v summary=%s", rv.Decision, rv.Confidence, rv.Summary)
	}

	prompt := fmt.Sprintf(`你是多Agent编排控制台的运行状态分析助手。以下是当前 Loop 运行的信息，请用中文分析执行进展，并给出卡点与下一步建议。

Run ID: %s
任务: %s
运行状态: %s
迭代进度: %s
审查决策: %s

节点执行情况:
%s

请只输出一个 JSON 对象（不要 Markdown 代码围栏、不要解释），字段：
{"summary":"一句话进展","progress":"较详细的进展描述","blocking":["卡点1","卡点2"],"suggestions":["建议1","建议2"]}
若没有卡点，blocking 给空数组。`, id, run.Task, run.Status, iterText, reviewText, strings.Join(nodesText, "\n"))

	bin := "reasonix"
	workDir := "."
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Join(filepath.Dir(exe), "reasonix.exe")
		if _, statErr := os.Stat(bin); statErr != nil {
			// Absolute path via PATH: a bare name with a cmd.Dir is rejected
			// by Go ("cannot run executable found relative to current
			// directory") inside the desktop app.
			if lp, lpErr := exec.LookPath("reasonix"); lpErr == nil {
				bin = lp
			}
		}
		workDir = filepath.Dir(exe)
	}
	const analysisTimeout = 4 * time.Minute
	ctx, cancel := context.WithTimeout(r.Context(), analysisTimeout)
	defer cancel()
	slog.Info("analyzeRunProgress: starting", "run", id, "attempts", len(attempts), "prompt_len", len(prompt))
	h.emitAnalyzeProgress("spawning", 0, 1, "")
	start := time.Now()
	stdout, errMsg, err := runAnalysisExec(ctx, bin, workDir, prompt, body.Executor, body.Model, body.Agent, func(line string) {
		h.emitAnalyzeProgress("thinking", int(time.Since(start).Seconds()), 1, line)
	})
	if err != nil {
		h.emitAnalyzeProgress("failed", int(time.Since(start).Seconds()), 1, errMsg)
		slog.Error("analyzeRunProgress failed", "err", errMsg, "run", id)
		writeErr(w, "analysis failed: "+errMsg, http.StatusInternalServerError)
		return
	}
	h.emitAnalyzeProgress("done", int(time.Since(start).Seconds()), 1, "")

	var result map[string]any
	scanner := bufio.NewScanner(strings.NewReader(stdout))
	var lastJSON string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "{") && json.Valid([]byte(line)) {
			lastJSON = line
		}
	}
	if lastJSON == "" {
		writeErr(w, "analysis produced no JSON", http.StatusInternalServerError)
		return
	}
	if err := json.Unmarshal([]byte(lastJSON), &result); err != nil {
		writeErr(w, "analysis JSON invalid: "+err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, result)
}

func (h *orchestratorHandler) listAgents(w http.ResponseWriter, _ *http.Request) {
	agents := h.store.ListAgents()
	if agents == nil {
		agents = []orchestrator.AgentInstance{}
	}
	writeJSON(w, agents)
}

// analysisOptions returns the executor picklist with per-executor model lists
// for the requirement/progress analysis entry points, mirroring the node-type
// catalog, plus the locally authored DSH agent presets (available as analysis
// personas when the executor is dsh). Selecting an executor only swaps the
// engine behind the analysis — the responsibility contract (the JSON schema)
// is always built server-side and passed as the prompt, so it stays universal.
func (h *orchestratorHandler) analysisOptions(w http.ResponseWriter, r *http.Request) {
	catalog := orchestrator.NodeTypeCatalogWithProbes(r.Context())
	executors := []string{}
	modelsByExecutor := map[string][]string{}
	if len(catalog) > 0 {
		for _, ex := range catalog[0].Executors {
			executors = append(executors, string(ex))
		}
		for k, v := range catalog[0].ModelsByExecutor {
			modelsByExecutor[string(k)] = append([]string{}, v...)
		}
	}
	if len(executors) == 0 {
		executors = []string{string(orchestrator.ExecutorReasonix)}
	}
	agents := analysisAgentProfiles()
	if agents == nil {
		agents = []analysisAgent{}
	}
	dshPresets := dshclient.ListAgentPresets()
	if dshPresets == nil {
		dshPresets = []dshclient.AgentPresetInfo{}
	}
	writeJSON(w, map[string]any{
		"executors":        executors,
		"modelsByExecutor": modelsByExecutor,
		"agents":           agents,
		"dshPresets":       dshPresets,
	})
}

type analysisAgent struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

// analysisAgentCache caches the subagent profile enumeration: scanning skill
// directories (maxDepth 3) can take seconds on cold start, and the frontend
// would otherwise abort the options request and fall back to a single default.
var analysisAgentCache struct {
	mu      sync.Mutex
	agents  []analysisAgent
	fetched time.Time
}

// analysisAgentProfiles enumerates reasonix subagent profiles visible from the
// executable's working directory, mirroring `reasonix subagent list`. Builtin
// subagent skills are included, same as the CLI. Failures degrade to an empty
// list so the analysis entry points stay usable. Results are cached for 30s.
func analysisAgentProfiles() (out []analysisAgent) {
	// A malformed skill directory (bad frontmatter, unexpected file layout)
	// must never take the whole /analysis/options endpoint down: degrade to an
	// empty list, exactly like the other failure paths.
	defer func() {
		if recover() != nil {
			out = nil
		}
	}()
	analysisAgentCache.mu.Lock()
	defer analysisAgentCache.mu.Unlock()
	if analysisAgentCache.agents != nil && time.Since(analysisAgentCache.fetched) < 30*time.Second {
		return analysisAgentCache.agents
	}
	workDir := "."
	if exe, err := os.Executable(); err == nil {
		workDir = filepath.Dir(exe)
	}
	opts := skill.Options{ProjectRoot: workDir, MaxDepth: 3, Stderr: io.Discard}
	if cfg, err := config.Load(); err == nil {
		opts.CustomPaths = cfg.SkillCustomPaths()
		opts.ExcludedPaths = cfg.SkillExcludedPaths()
		opts.MaxDepth = cfg.SkillMaxDepth()
	}
	for _, sk := range skill.New(opts).List() {
		if sk.RunAs == skill.RunSubagent {
			out = append(out, analysisAgent{Name: sk.Name, Description: sk.Description})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	analysisAgentCache.agents = out
	analysisAgentCache.fetched = time.Now()
	return out
}

func (h *orchestratorHandler) nodeTypes(w http.ResponseWriter, r *http.Request) {
	// 程序化探测优先：前端下拉反映本机真实可用的模型（每台电脑可能不同），
	// 探测失败时回落静态目录。
	writeJSON(w, orchestrator.NodeTypeCatalogWithProbes(r.Context()))
}

// dshPresets returns the locally authored DSH agent presets
// ($DSH_HOME/.agent-presets) so the config panel can offer them on dsh nodes.
// Read-only: importing the bundled pack is an explicit user action via
// POST /dsh-presets/install (the "一键导入" button).
func (h *orchestratorHandler) dshPresets(w http.ResponseWriter, _ *http.Request) {
	presets := dshclient.ListAgentPresets()
	if presets == nil {
		presets = []dshclient.AgentPresetInfo{}
	}
	writeJSON(w, presets)
}

// dshPresetsInstall runs the bundled dsh-agent-pack installer on demand
// (button-triggered). Idempotent: presets already present are skipped, so a
// repeated click only fills the gaps. Returns the install report so the
// frontend can show what was imported.
func (h *orchestratorHandler) dshPresetsInstall(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, orchestrator.InstallAgentPack())
}

// selfcheck returns the one-click self-check report: agent catalog, live
// runtime states, skill catalog, per-executor binary availability, and the
// locally imported DSH agent presets.
func (h *orchestratorHandler) selfcheck(w http.ResponseWriter, r *http.Request) {
	report := orchestrator.SelfCheckSnapshot(r.Context())
	running := h.store.ListAgents()
	if running == nil {
		running = []orchestrator.AgentInstance{}
	}
	writeJSON(w, map[string]interface{}{
		"checkedAt":   report.CheckedAt,
		"agents":      report.Agents,
		"running":     running,
		"runtimes":    report.Runtimes,
		"skills":      report.Skills,
		"skillRoots":  report.SkillRoots,
		"health":      report.Health,
		"dshPresets":  report.DshPresets,
		"probes":      report.Probes,
		"packInstall": report.PackInstall,
	})
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
	all = append(all, orchestrator.ListOpencodeRuntimes()...)
	writeJSON(w, all)
}

// cleanupRuntimes deletes stale per-runtime working directories under
// bin/orchestrator-runtimes (each can hold a 50+ MB reasonix.exe copy), keeping
// the `keep` most recent ones. Returns how many were removed and the freed size.
func (h *orchestratorHandler) cleanupRuntimes(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Keep int `json:"keep"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Keep < 1 {
		body.Keep = 2
	}

	root := filepath.Join(filepath.Dir(os.Args[0]), "orchestrator-runtimes")
	if exe, err := os.Executable(); err == nil {
		root = filepath.Join(filepath.Dir(exe), "orchestrator-runtimes")
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		writeJSON(w, map[string]any{"deleted": 0, "freedMB": 0, "kept": []string{}, "error": err.Error()})
		return
	}
	var dirs []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), "runtime-") {
			dirs = append(dirs, e.Name())
		}
	}
	sort.Strings(dirs) // names embed timestamps: ascending = oldest first
	var kept []string
	deleted := 0
	var freed int64
	for i, name := range dirs {
		if i >= len(dirs)-body.Keep {
			kept = append(kept, name)
			continue
		}
		target := filepath.Join(root, name)
		dirSize, _ := dirSizeBytes(target)
		if rmErr := os.RemoveAll(target); rmErr == nil {
			deleted++
			freed += dirSize
		}
	}
	slog.Info("runtimes cleanup", "root", root, "deleted", deleted, "freed_mb", freed>>20, "kept", kept)
	writeJSON(w, map[string]any{"deleted": deleted, "freedMB": int(freed >> 20), "kept": kept})
}

func dirSizeBytes(path string) (int64, error) {
	var total int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip unreadable entries; count what we can
		}
		if !d.IsDir() {
			if info, e := d.Info(); e == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total, err
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
	if rt, ok := orchestrator.GetOpencodeRuntime(id); ok {
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
	if err := orchestrator.StopOpencodeRuntime(id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeErr(w, "runtime not found", http.StatusNotFound)
}

// openRuntimeBrowser opens a runtime's HTTP endpoint in the system default
// browser. Only loopback endpoints are accepted so the API cannot be abused
// to launch arbitrary URLs.
func (h *orchestratorHandler) openRuntimeBrowser(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Endpoint string `json:"endpoint"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || strings.TrimSpace(body.Endpoint) == "" {
		writeErr(w, "endpoint is required", http.StatusBadRequest)
		return
	}
	u, err := url.Parse(strings.TrimSpace(body.Endpoint))
	if err != nil {
		writeErr(w, fmt.Sprintf("invalid endpoint: %v", err), http.StatusBadRequest)
		return
	}
	host := u.Hostname()
	if u.Scheme != "http" && u.Scheme != "https" {
		writeErr(w, "only http(s) endpoints are allowed", http.StatusBadRequest)
		return
	}
	if host != "127.0.0.1" && host != "localhost" && host != "::1" {
		writeErr(w, "only loopback endpoints are allowed", http.StatusBadRequest)
		return
	}
	if err := openSystemBrowser(u.String()); err != nil {
		writeErr(w, fmt.Sprintf("failed to open browser: %v", err), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// openSystemBrowser launches the OS default browser for a URL in a detached
// process, mirroring the pattern used by the MCP manager.
func openSystemBrowser(target string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", target)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", target)
	default:
		cmd = exec.Command("xdg-open", target)
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	go cmd.Wait()
	return nil
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
	if snapshot, ok := orchestrator.SnapshotOpencodeRuntime(id); ok {
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

// answerRuntimePermission resolves one parked tool-approval prompt on a
// runtime (mimo ACP or claude SDK) with the action chosen in the Runtime
// Console: allow_once | allow_always | reject.
func (h *orchestratorHandler) answerRuntimePermission(w http.ResponseWriter, r *http.Request, id string) {
	var body struct {
		RequestID string `json:"requestId"`
		Action    string `json:"action"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.RequestID == "" {
		writeErr(w, "requestId is required", http.StatusBadRequest)
		return
	}
	switch body.Action {
	case "allow_once", "allow_always", "reject":
	default:
		writeErr(w, "action must be allow_once | allow_always | reject", http.StatusBadRequest)
		return
	}
	if err := orchestrator.AnswerMimoRuntimePermission(id, body.RequestID, body.Action); err == nil {
		writeJSON(w, map[string]string{"ok": "true"})
		return
	}
	if err := orchestrator.AnswerClaudeRuntimePermission(id, body.RequestID, body.Action); err == nil {
		writeJSON(w, map[string]string{"ok": "true"})
		return
	}
	if err := orchestrator.AnswerOpencodeRuntimePermission(id, body.RequestID, body.Action); err != nil {
		writeErr(w, err.Error(), http.StatusNotFound)
		return
	}
	writeJSON(w, map[string]string{"ok": "true"})
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
	if err == nil {
		writeJSON(w, map[string]string{"turnID": turnID})
		return
	}
	if err := orchestrator.SendOpencodeRuntimeMessage(id, body.Text); err != nil {
		writeErr(w, err.Error(), http.StatusBadRequest)
		return
	}
	writeJSON(w, map[string]string{"turnID": "manual"})
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
		if err := orchestrator.InterruptOpencodeRuntime(id); err != nil {
			writeErr(w, err.Error(), http.StatusBadRequest)
			return
		}
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
		Lang    string `json:"lang"` // "zh" | "en"; UI language for replies
		History []struct {
			Role    string `json:"role"`
			Content string `json:"content"`
		} `json:"history"`
		PipelineInfo string `json:"pipelineInfo"`
		Executor     string `json:"executor"`
		Model        string `json:"model"`
		Agent        string `json:"agent"`
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
	lang := body.Lang
	if lang != "en" {
		lang = "zh"
	}
	prompt := buildAnalysisPrompt(lang, skillCatalog, h.analysisCapabilitySummary(), historyText)

	bin := "reasonix"
	workDir := "."
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Join(filepath.Dir(exe), "reasonix.exe")
		if _, statErr := os.Stat(bin); statErr != nil {
			// Absolute path via PATH: a bare name with a cmd.Dir is rejected
			// by Go ("cannot run executable found relative to current
			// directory") inside the desktop app.
			if lp, lpErr := exec.LookPath("reasonix"); lpErr == nil {
				bin = lp
			}
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
	runOnce := func(attempt int, p string) (string, error) {
		start := time.Now()
		stdout, errMsg, err := runAnalysisExec(ctx, bin, workDir, p, body.Executor, body.Model, body.Agent, func(line string) {
			h.emitAnalyzeProgress("thinking", int(time.Since(start).Seconds()), attempt, line)
		})
		if err != nil {
			h.emitAnalyzeProgress("failed", int(time.Since(start).Seconds()), attempt, errMsg)
			slog.Error("analyze failed", "err", errMsg, "bin", bin, "stdout", truncate(stdout, 200))
			return "", fmt.Errorf("%s", errMsg)
		}
		slog.Info("analyze: subprocess completed", "stdout_len", len(stdout))
		// Parse the JSON response. reasonix run outputs thinking text BEFORE the
		// JSON and token stats AFTER it; find the LAST complete JSON object
		// containing the "analysis" key.
		raw := stdout
		var jsonStr string
		for k := 0; k < len(raw); k++ {
			if raw[k] != '{' {
				continue
			}
			depth := 0
			end := -1
			for m := k; m < len(raw); m++ {
				if raw[m] == '{' {
					depth++
				} else if raw[m] == '}' {
					depth--
					if depth == 0 {
						end = m
						break
					}
				}
			}
			if end < 0 {
				continue
			}
			candidate := raw[k : end+1]
			if strings.Contains(candidate, `"analysis"`) {
				jsonStr = candidate
			}
			k = end
		}
		if jsonStr == "" {
			return "", fmt.Errorf("no JSON in analysis response")
		}
		return jsonStr, nil
	}
	h.emitAnalyzeProgress("spawning", 0, 1, "")
	jsonStr, err := runOnce(1, prompt)
	if err != nil {
		// Retry once with a strict instruction; models often emit thinking text
		// on the first attempt which then lacks the analysis JSON.
		h.emitAnalyzeProgress("retrying", 0, 2, "")
		strict := prompt + "\n\n重要：你必须只输出一个 JSON 对象，禁止输出思考过程、解释或 Markdown 代码围栏。"
		jsonStr, err = runOnce(2, strict)
		if err != nil {
			writeErr(w, "analysis failed: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	h.emitAnalyzeProgress("done", 0, 0, "")
	// Fix corrupted UTF-8 and escape sequences from model output
	raw := cleanJSON(jsonStr)

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

// analysisCapabilitySummary lists what the orchestrator can actually orchestrate
// with today: node types with their executors/models, presets, and skills. It is
// injected into the analysis prompt so the conversational agent knows the system
// inventory instead of guessing.
func (h *orchestratorHandler) analysisCapabilitySummary() string {
	var b strings.Builder
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	catalog := orchestrator.NodeTypeCatalogWithProbes(ctx)
	b.WriteString("\n\n## 系统能力清单（只能使用以下节点类型/执行器/模型/预设）\n")
	for _, t := range catalog {
		execs := make([]string, 0, len(t.Executors))
		for _, e := range t.Executors {
			execs = append(execs, string(e))
		}
		b.WriteString(fmt.Sprintf("- 节点类型 %s（%s）：执行器 %s；", t.Type, t.Label, strings.Join(execs, "/")))
		if len(t.Models) > 0 {
			b.WriteString("模型 " + strings.Join(t.Models, ", "))
		}
		b.WriteString("\n")
	}
	b.WriteString("\n可用编排预设：\n")
	for _, p := range orchestrator.Presets() {
		b.WriteString(fmt.Sprintf("- %s（%s）：%s\n", p.ID, p.Name, p.Desc))
	}
	return b.String()
}

// buildAnalysisPrompt builds the requirement-analysis prompt for the requested
// UI language. The model must reply in that language; casual/greeting messages
// are handled conversationally (no fabricated requirements), like a concierge
// would: ask follow-up questions step by step and keep it friendly.
func buildAnalysisPrompt(lang, skillCatalog, capability, historyText string) string {
	jsonSpec := `{
  "analysis": "one-line summary of the user's request",
  "rewritten": "structured requirement document (background, goals, constraints, acceptance criteria)",
  "steps": [
    {"id": "s1", "step": "step description", "agent": "architect/executor/reviewer", "model": "model name", "executor": "reasonix or mimo", "mode": "serve or run", "skill": "optional skill name, empty if none", "roleDesc": "detailed duty of this agent, including input/output format"}
  ],
  "edges": [
    {"from": "s1", "to": "s2"},
    {"from": "s1", "to": "s3"},
    {"from": "s2", "to": "s4"},
    {"from": "s3", "to": "s4"}
  ],
  "suggestion": "optional advice for the user",
  "loopConfig": {
    "enabled": false,
    "mode": "review_decides",
    "maxIterations": 3,
    "fixedIterations": 0,
    "reviewNodeID": "",
    "protocol": "loop-review-v1"
  }
}`
	// Shared rules expressed in the UI language; the JSON schema stays fixed.
	if lang == "en" {
		return `You are the orchestration console's conversational requirement analyst (a concierge, not a robot).
Rules:
1. Read the full conversation history; understand context and follow-ups.
2. When the conversation is only greetings / small talk / casual messages, or there is no actionable request yet: DO NOT fabricate a requirement. Reply conversationally: analysis = a friendly one-liner, rewritten = a short "please describe your request" guide, steps = [], edges = [], suggestion = friendly guidance (you may chat and ask step by step: what is the goal? what should be delivered? any constraints?).
3. Otherwise rewrite the request into a structured document (rewritten), break it into executable steps with dependencies (serial/parallel/converge), assign agent roles and models, and design Loop semantics (iteration/review-until-pass) as a RUNTIME loop — never expand rounds into duplicated nodes.
4. Every roleDesc describes ONE invocation of that node only: the agent must not set its own Goal, loop three rounds by itself, run the whole pipeline, or hand off only after finishing; Orchestrator owns all of that.
5. Skills must come from the real catalog below; never invent names; at most one skill per node; empty when none fits. Reviewers may only pick review-agent/code-review style skills, never "loop" (a scheduler, not a reviewer).

Available Skill catalog:
` + skillCatalog + capability + `

Full conversation history:
` + historyText + `

Output strictly the JSON below (no markdown fences, raw JSON only):
` + jsonSpec + `

Model selection (cost-first):
- architect: deepseek-pro (only when strong reasoning is required)
- executor: xiaomi/mimo-v2.5 (best value) or deepseek-flash (light tasks)
- reviewer: deepseek-flash (cheapest, enough for review)
- Never default everything to pro.

Executor mapping: deepseek models -> executor "reasonix"; xiaomi models -> executor "mimo".

Mode: prefer "serve"; use "run" only for one-shot stateless tasks.

Edge types: serial (default) = B starts after A; parallel = independent; converge = C starts after A and B.

Node design constraints:
- every node must have a clear, valuable output; no re-printing already-known info
- architect produces design + implementation checklist only (no code, no execution, no internal loop), then hands off immediately
- executor produces new content (code/docs/analysis), handles only the current round, never sets its own Goal or simulates the next round
- reviewer judges based on upstream output, no need to re-read original files
- reviewer converges at the end after all implementation

Loop rules (mandatory):
- Loop = runtime repetition of the SAME base DAG; never copy rounds onto the canvas.
- Draw one base DAG only; no edges back from reviewer to executor/architect, no duplicate nodes.
- One set of base nodes. "architect -> executor -> reviewer, max 3 rounds" = 3 nodes + 2 edges, NEVER 9 nodes / 3 role copies / "round 1/2/3" in nodes.
- review_decides: reviewer outputs pass/revise/blocked; revise reschedules the same base DAG, pass ends early, blocked terminates; the reviewer never starts the next round itself.
- fixed: run the same base DAG exactly N rounds; reviewer still runs each round; pass does not end early.
- loopConfig.enabled=true requires exactly one reviewer step and its step id in loopConfig.reviewNodeID; false unless the request explicitly asks for a Loop.
- Loop round counts must be integers 1..10 (review_decides uses maxIterations, fixed uses fixedIterations).
- Node ids use s1, s2, s3...; edges reference them.

Agent roles:
- architect (pro): analyze requirements, design, define interfaces, evaluate quality
- executor (mimo): read/write files, run commands, implement
- reviewer (flash): review code, find problems, advise

IMPORTANT LANGUAGE RULE: every piece of output text — analysis, rewritten, suggestion, steps[].step, steps[].roleDesc — MUST be written in English. The user interface is currently English.
`
	}
	return `你是一个多Agent编排控制台的对话式需求分析管家（是金牌销售式的管家，不是冷冰冰的机器人）。
规则：
1. 阅读完整对话历史，理解用户需求和上下文；用户后续修改需求时结合之前的讨论理解。
2. 如果对话只是问候/闲聊/寒暄，或还没有任何可执行的需求：绝对不要编造需求。此时 analysis 写一句友好的回应，rewritten 写"请描述你的需求"的简短引导，steps 返回空数组 []，edges 返回空数组 []，suggestion 用中文友好地引导用户——可以闲聊，并一步步追问：目标是什么？要交付什么？有什么约束或参考？
3. 否则把需求改写为结构化文档（rewritten），分解为可执行步骤并设计依赖（串行/并行/汇聚），分配 Agent 角色与模型；提到"循环、迭代、反复执行、审查后修改、直到通过"等 Loop 语义时，设计成"运行时循环"，不能把每一轮展开成多组节点。
4. 每个 roleDesc 只描述该节点一次调用的职责：Agent 不得自己设定 Goal、自己循环三轮、自己执行完整流水线或执行完才交接；这些都由 Orchestrator 负责。
5. Skill 只能从下面的真实目录选择，不能编造；每节点最多一个 Skill，没有合适的留空；Reviewer 只能选 review-agent/code-review 类审查 Skill，绝对不能选 loop（那是定时调度器不是审查器）。

可用 Skill 目录：
` + skillCatalog + capability + `

完整对话历史：
` + historyText + `

请严格输出以下JSON格式（不要加markdown代码块标记，直接输出JSON）：
` + jsonSpec + `

模型选择原则（成本优先）：
- 架构师：deepseek-pro（需要强推理时才用）
- 执行者：xiaomi/mimo-v2.5（性价比最高）或 deepseek-flash（轻量任务）
- 审查者：deepseek-flash（最便宜，审查足够）
- 不要默认全用 pro

执行器映射：deepseek 模型 → executor "reasonix"；xiaomi 模型 → executor "mimo"。

运行模式：默认优先 serve；只有一次性、无状态任务才用 run。

边类型：serial（默认）= A 完成后 B 开始；parallel = 独立并行；converge = A 和 B 都完成后 C 开始。

节点任务设计原则：
- 每节点必须有明确、有价值的输出目标；不要重复已有信息
- 架构师只产出方案和实施清单（不写代码、不执行、不内部循环），输出后立即交给执行者
- 执行者产出新内容（代码/文档/分析），只处理当前轮，不得自设 Goal 或模拟下一轮
- 审查者基于上游输出判断，不重新读原始文件；审查者在所有实现完成后汇聚

Loop 设计规则（必须遵守）：
- Loop 是运行时对同一份 Pipeline DAG 重复执行，不是把多轮复制到 Canvas。
- Canvas 只画一轮基础 DAG；不要加审查者回到执行者/架构师的回边，不要画重复节点。
- 只生成一组基础节点："架构师 → 执行者 → 审查者，最多 3 轮" = 3 节点 + 2 边；绝对不能生成 9 节点/3 组相同角色/把"第1轮/第2轮/第3轮"写进节点。
- review_decides：审查者输出 pass/revise/blocked；revise 由 Orchestrator 重新调度同一组基础节点，pass 提前结束，blocked 终止；审查者不能自己调用下一轮。
- fixed：同一组基础节点精确执行 N 轮；每轮仍执行审查者，pass 不能提前结束。
- loopConfig.enabled=true 时 steps 必须恰好有一个 reviewer 节点并在 reviewNodeID 填其 step id；需求没明确要求 Loop 则 enabled=false。
- 轮数必须是 1~10 的整数（review_decides 填 maxIterations，fixed 填 fixedIterations）。
- 节点 id 用 s1, s2, s3...，edges 引用这些 id。

Agent角色：
- 架构师(pro)：分析需求、设计方案、定义接口、评估质量
- 执行者(mimo)：读写文件、执行命令、实现功能
- 审查者(flash)：审查代码、发现问题、给出建议

重要语言规则：所有输出文本（analysis、rewritten、suggestion、steps[].step、steps[].roleDesc）必须使用中文。用户界面当前是中文。
`
}

func (h *orchestratorHandler) expandRequirement(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text     string `json:"text"`
		Executor string `json:"executor"`
		Model    string `json:"model"`
		Agent    string `json:"agent"`
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
		if _, statErr := os.Stat(bin); statErr != nil {
			// Absolute path via PATH: a bare name with a cmd.Dir is rejected
			// by Go ("cannot run executable found relative to current
			// directory") inside the desktop app.
			if lp, lpErr := exec.LookPath("reasonix"); lpErr == nil {
				bin = lp
			}
		}
	}

	stdout, errMsg, err := runAnalysisExec(context.Background(), bin, workDir, prompt, body.Executor, body.Model, body.Agent, nil)
	if err != nil {
		slog.Error("expand requirement failed", "err", errMsg)
		writeJSON(w, map[string]string{"expanded": body.Text}) // fallback to original
		return
	}

	expanded := strings.TrimSpace(stdout)
	if expanded == "" {
		expanded = body.Text
	}
	writeJSON(w, map[string]string{"expanded": expanded})
}

func (h *orchestratorHandler) understandRequirement(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Text     string `json:"text"`
		Executor string `json:"executor"`
		Model    string `json:"model"`
		Agent    string `json:"agent"`
		Nodes    []struct {
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
	workDir := "."
	if exe, err := os.Executable(); err == nil {
		bin = filepath.Join(filepath.Dir(exe), "reasonix.exe")
		if _, statErr := os.Stat(bin); statErr != nil {
			// Absolute path via PATH: a bare name with a cmd.Dir is rejected
			// by Go ("cannot run executable found relative to current
			// directory") inside the desktop app.
			if lp, lpErr := exec.LookPath("reasonix"); lpErr == nil {
				bin = lp
			}
		}
		workDir = filepath.Dir(exe)
	}
	stdout, errMsg, err := runAnalysisExec(context.Background(), bin, workDir, prompt, body.Executor, body.Model, body.Agent, nil)
	if err != nil {
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
	raw := strings.TrimSpace(stdout)
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
