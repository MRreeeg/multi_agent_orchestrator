package serve

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"reasonix/internal/config"
	"reasonix/internal/control"
	"reasonix/internal/orchestrator"
)

func TestOrchestrationSessionReturnsPersistedRuntimeConsoleState(t *testing.T) {
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	h := &orchestratorHandler{store: orchestrator.NewStore(), loadedSessions: make(map[string]bool)}
	sess, err := h.store.CreateOrchSession("runtime console", `G:\workspace`)
	if err != nil {
		t.Fatal(err)
	}
	h.loadedSessions[sess.ID] = true
	runtime := orchestrator.RuntimeState{
		RuntimeID:     "codex_rt_saved",
		SessionID:     sess.ID,
		NodeID:        "reviewer",
		RunID:         "run_saved",
		Executor:      "codex",
		Model:         "ccs",
		Endpoint:      "ws://127.0.0.1:43111",
		Port:          43111,
		Status:        orchestrator.RuntimeIdle,
		CleanupPolicy: orchestrator.CleanupRetained,
		AccessMode:    "runtime_console",
		ThreadID:      "thread_saved",
	}
	if err := h.store.CreateRuntimeState(runtime); err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions/"+sess.ID, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("session detail status = %d: %s", w.Code, w.Body.String())
	}
	var detail struct {
		RuntimeStates map[string]orchestrator.RuntimeState `json:"runtimeStates"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &detail); err != nil {
		t.Fatal(err)
	}
	got, ok := detail.RuntimeStates[runtime.RuntimeID]
	if !ok {
		t.Fatalf("runtimeStates = %#v, want %q", detail.RuntimeStates, runtime.RuntimeID)
	}
	if got.NodeID != runtime.NodeID || got.Model != "ccs" || got.AccessMode != "runtime_console" || got.ThreadID != runtime.ThreadID {
		t.Fatalf("returned runtime = %#v", got)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions/"+sess.ID+"/runtimes", nil)
	listW := httptest.NewRecorder()
	h.ServeHTTP(listW, listReq)
	if listW.Code != http.StatusOK {
		t.Fatalf("runtime list status = %d: %s", listW.Code, listW.Body.String())
	}
	var listed []orchestrator.RuntimeState
	if err := json.Unmarshal(listW.Body.Bytes(), &listed); err != nil {
		t.Fatal(err)
	}
	if len(listed) != 1 || listed[0].RuntimeID != runtime.RuntimeID || listed[0].NodeID != runtime.NodeID || listed[0].AccessMode != "runtime_console" {
		t.Fatalf("runtime list = %#v", listed)
	}
}

func TestOrchestrationSessionPipelineSaveAndReloadAPI(t *testing.T) {
	bc := NewBroadcaster()
	ctrl := control.New(control.Options{Sink: bc})
	srv := httptest.NewServer(New(ctrl, bc, config.ServeConfig{}).Handler())
	defer srv.Close()

	var session struct {
		ID string `json:"id"`
	}
	requestJSON(t, srv.Client(), http.MethodPost, srv.URL+"/orchestrator/api/orch-sessions", `{"title":"API regression"}`, http.StatusOK, &session)
	if session.ID == "" {
		t.Fatal("create session returned an empty ID")
	}

	pipeline := `{"name":"C++ notes","nodes":[{"id":"1","type":"executor","name":"读取笔记","model":"deepseek-flash","executor":"reasonix","x":60,"y":80}],"edges":[]}`
	var revision struct {
		ID    string `json:"id"`
		Nodes []any  `json:"nodes"`
	}
	requestJSON(t, srv.Client(), http.MethodPost, srv.URL+"/orchestrator/api/orch-sessions/"+session.ID+"/pipelines", pipeline, http.StatusOK, &revision)
	if revision.ID == "" || len(revision.Nodes) != 1 {
		t.Fatalf("create revision = %#v", revision)
	}

	var current struct {
		ID    string `json:"id"`
		Nodes []any  `json:"nodes"`
	}
	requestJSON(t, srv.Client(), http.MethodGet, srv.URL+"/orchestrator/api/orch-sessions/"+session.ID+"/pipelines/current", "", http.StatusOK, &current)
	if current.ID != revision.ID || len(current.Nodes) != 1 {
		t.Fatalf("current revision = %#v, want %s and one node", current, revision.ID)
	}

	updated := `{"name":"C++ notes","nodes":[{"id":"1","type":"executor","name":"读取并分析笔记","model":"deepseek-flash","executor":"reasonix","x":60,"y":80}],"edges":[],"source":"manual_edit"}`
	var updatedRevision struct {
		ID    string `json:"id"`
		Nodes []struct {
			Name string `json:"name"`
		} `json:"nodes"`
	}
	requestJSON(t, srv.Client(), http.MethodPut, srv.URL+"/orchestrator/api/orch-sessions/"+session.ID+"/pipelines/current", updated, http.StatusOK, &updatedRevision)
	if updatedRevision.ID == revision.ID || len(updatedRevision.Nodes) != 1 || updatedRevision.Nodes[0].Name != "读取并分析笔记" {
		t.Fatalf("updated revision = %#v", updatedRevision)
	}
}

func TestOrchestrationSessionConversationAndNativeBinding(t *testing.T) {
	bc := NewBroadcaster()
	ctrl := control.New(control.Options{Sink: bc})
	srv := httptest.NewServer(New(ctrl, bc, config.ServeConfig{}).Handler())
	defer srv.Close()

	var first orchestrator.OrchestrationSession
	requestJSON(t, srv.Client(), http.MethodPost, srv.URL+"/orchestrator/api/orch-sessions/migrate", `{"nativeSessionPath":"C:\\sessions\\one.jsonl","nativeSessionName":"one.jsonl"}`, http.StatusOK, &first)
	if first.ID == "" {
		t.Fatal("migration did not return an orchestration session")
	}
	var second orchestrator.OrchestrationSession
	requestJSON(t, srv.Client(), http.MethodPost, srv.URL+"/orchestrator/api/orch-sessions/migrate", `{"nativeSessionPath":"C:\\sessions\\one.jsonl","nativeSessionName":"one.jsonl"}`, http.StatusOK, &second)
	if second.ID != first.ID {
		t.Fatalf("duplicate migration created %q, want %q", second.ID, first.ID)
	}
	body := `{"chatMessages":[{"role":"user","text":"第一轮需求","meta":""}],"requirementMessages":[{"role":"user","text":"第一轮需求"}],"pipelineMessages":[]}`
	requestJSON(t, srv.Client(), http.MethodPut, srv.URL+"/orchestrator/api/orch-sessions/"+first.ID+"/conversation", body, http.StatusNoContent, nil)
	var conversation map[string]interface{}
	requestJSON(t, srv.Client(), http.MethodGet, srv.URL+"/orchestrator/api/orch-sessions/"+first.ID+"/conversation", "", http.StatusOK, &conversation)
	messages, ok := conversation["chatMessages"].([]interface{})
	if !ok || len(messages) != 1 {
		t.Fatalf("conversation was not persisted: %#v", conversation)
	}
}

func TestUpdateCurrentRevisionRejectsUnknownSessionAsNotFound(t *testing.T) {
	bc := NewBroadcaster()
	ctrl := control.New(control.Options{Sink: bc})
	srv := httptest.NewServer(New(ctrl, bc, config.ServeConfig{}).Handler())
	defer srv.Close()

	requestJSON(t, srv.Client(), http.MethodPut,
		srv.URL+"/orchestrator/api/orch-sessions/stale-session/pipelines/current",
		`{"nodes":[],"edges":[]}`, http.StatusNotFound, nil)
}

func TestUpdateOrchestrationSessionPersistsRewrittenTask(t *testing.T) {
	bc := NewBroadcaster()
	ctrl := control.New(control.Options{Sink: bc})
	srv := httptest.NewServer(New(ctrl, bc, config.ServeConfig{}).Handler())
	defer srv.Close()

	var session orchestrator.OrchestrationSession
	requestJSON(t, srv.Client(), http.MethodPost,
		srv.URL+"/orchestrator/api/orch-sessions", `{"title":"task persistence"}`,
		http.StatusOK, &session)
	requestJSON(t, srv.Client(), http.MethodPut,
		srv.URL+"/orchestrator/api/orch-sessions/"+session.ID,
		`{"task":"active task","rewrittenTask":"latest analyzed task"}`,
		http.StatusNoContent, nil)

	var loaded map[string]any
	requestJSON(t, srv.Client(), http.MethodGet,
		srv.URL+"/orchestrator/api/orch-sessions/"+session.ID,
		"", http.StatusOK, &loaded)
	loadedSession, ok := loaded["session"].(map[string]any)
	if !ok || loadedSession["rewrittenTask"] != "latest analyzed task" {
		t.Fatalf("rewrittenTask was not persisted: %#v", loaded)
	}
}

func requestJSON(t *testing.T, client *http.Client, method, url, body string, wantStatus int, out any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != wantStatus {
		t.Fatalf("%s %s status = %d, want %d: %s", method, url, resp.StatusCode, wantStatus, data)
	}
	if out != nil && len(data) > 0 {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("decode %s %s: %v: %s", method, url, err, data)
		}
	}
}
