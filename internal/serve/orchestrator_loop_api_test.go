package serve

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"reasonix/internal/orchestrator"
)

func TestOrchestratorHandlerLoadsIndexImmediatelyAndSessionEntitiesOnDemand(t *testing.T) {
	root := t.TempDir()
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)
	if err := os.WriteFile(filepath.Join(root, ".migrated"), []byte("test"), 0644); err != nil {
		t.Fatal(err)
	}

	seed := orchestrator.NewStore()
	sess, err := seed.CreateOrchSession("lazy startup", "")
	if err != nil {
		t.Fatal(err)
	}
	rev, err := seed.CreatePipelineRevision(sess.ID, orchestrator.PipelineRevision{
		Name:  "pipeline",
		Nodes: []orchestrator.AgentNode{{ID: "executor", Type: orchestrator.NodeExecutor}},
	})
	if err != nil {
		t.Fatal(err)
	}

	h := newOrchestratorHandler(nil)
	if _, ok := h.store.GetPipelineRevision(rev.ID); ok {
		t.Fatal("handler eagerly loaded pipeline entities; startup should load the index only")
	}

	list := httptest.NewRecorder()
	h.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions", nil))
	if list.Code != http.StatusOK {
		t.Fatalf("list sessions status = %d: %s", list.Code, list.Body.String())
	}

	current := httptest.NewRecorder()
	h.ServeHTTP(current, httptest.NewRequest(http.MethodGet,
		"/orchestrator/api/orch-sessions/"+sess.ID+"/pipelines/current", nil))
	if current.Code != http.StatusOK {
		t.Fatalf("lazy current pipeline status = %d: %s", current.Code, current.Body.String())
	}
	loaded, ok := h.store.GetPipelineRevision(rev.ID)
	if !ok || loaded.Name != "pipeline" {
		t.Fatalf("pipeline was not loaded on demand: %#v", loaded)
	}
}

func TestLoopPipelineConfigAndIterationAPI(t *testing.T) {
	h := &orchestratorHandler{store: orchestrator.NewStore()}
	sess, err := h.store.CreateOrchSession("loop api", "")
	if err != nil {
		t.Fatal(err)
	}
	create := httptest.NewRequest(http.MethodPost, "/orchestrator/api/orch-sessions/"+sess.ID+"/pipelines", jsonReader(`{
		"name":"loop", "nodes":[{"id":"review","type":"reviewer","name":"review"}], "edges":[],
		"loopConfig":{"enabled":true,"mode":"fixed","maxIterations":3,"reviewNodeID":"review","protocol":"loop-review-v1"}
	}`))
	create.Header.Set("Content-Type", "application/json")
	createW := httptest.NewRecorder()
	h.ServeHTTP(createW, create)
	if createW.Code != http.StatusOK {
		t.Fatalf("create pipeline status = %d: %s", createW.Code, createW.Body.String())
	}
	var rev orchestrator.PipelineRevision
	if err := json.Unmarshal(createW.Body.Bytes(), &rev); err != nil {
		t.Fatal(err)
	}
	if rev.LoopConfig.FixedIterations != 3 {
		t.Fatalf("created loop config = %+v", rev.LoopConfig)
	}

	run, err := h.store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	for _, number := range []int{2, 1} {
		if err := h.store.CreateIteration(orchestrator.LoopIteration{ID: run.ID + "_" + string(rune('0'+number)), RunID: run.ID, Number: number, Status: "passed"}); err != nil {
			t.Fatal(err)
		}
	}
	get := httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions/"+sess.ID+"/runs/"+run.ID+"/iterations", nil)
	getW := httptest.NewRecorder()
	h.ServeHTTP(getW, get)
	if getW.Code != http.StatusOK {
		t.Fatalf("iterations status = %d: %s", getW.Code, getW.Body.String())
	}
	var iterations []orchestrator.LoopIteration
	if err := json.Unmarshal(getW.Body.Bytes(), &iterations); err != nil {
		t.Fatal(err)
	}
	if len(iterations) != 2 || iterations[0].Number != 1 || iterations[1].Number != 2 {
		t.Fatalf("iterations = %+v", iterations)
	}
}

func TestIterationAPIRejectsCrossSessionRun(t *testing.T) {
	h := &orchestratorHandler{store: orchestrator.NewStore()}
	one, _ := h.store.CreateOrchSession("one", "")
	two, _ := h.store.CreateOrchSession("two", "")
	rev, _ := h.store.CreatePipelineRevision(one.ID, orchestrator.PipelineRevision{Nodes: []orchestrator.AgentNode{{ID: "n", Type: orchestrator.NodeExecutor}}})
	run, err := h.store.CreateRun(one.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions/"+two.ID+"/runs/"+run.ID+"/iterations", nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("cross-session status = %d, want 404", w.Code)
	}
}

func TestOrchestrationSessionWorkspaceUpdateAPI(t *testing.T) {
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	h := &orchestratorHandler{store: orchestrator.NewStore(), loadedSessions: make(map[string]bool)}
	sess, err := h.store.CreateOrchSession("workspace update", `G:\before`)
	if err != nil {
		t.Fatal(err)
	}
	h.loadedSessions[sess.ID] = true

	req := httptest.NewRequest(http.MethodPut, "/orchestrator/api/orch-sessions/"+sess.ID,
		strings.NewReader(`{"workspace":"G:\\after"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("workspace update status = %d: %s", w.Code, w.Body.String())
	}
	updated, ok := h.store.GetOrchSession(sess.ID)
	if !ok || updated.Workspace != `G:\after` {
		t.Fatalf("updated session = %#v, want workspace G:\\after", updated)
	}
}

func TestOrchestratorRunCancelRoute(t *testing.T) {
	h := &orchestratorHandler{store: orchestrator.NewStore(), loadedSessions: make(map[string]bool)}
	sess, err := h.store.CreateOrchSession("cancel route", "")
	if err != nil {
		t.Fatal(err)
	}
	h.loadedSessions[sess.ID] = true
	rev, err := h.store.CreatePipelineRevision(sess.ID, orchestrator.PipelineRevision{
		Nodes: []orchestrator.AgentNode{{ID: "executor", Type: orchestrator.NodeExecutor}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, err := h.store.CreateRun(sess.ID, rev.ID, "task", "", "manual", "")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost,
		"/orchestrator/api/orch-sessions/"+sess.ID+"/runs/"+run.ID+"/cancel", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("cancel status = %d: %s", w.Code, w.Body.String())
	}
	updated, ok := h.store.GetRun(run.ID)
	if !ok || updated.Status != "canceled" {
		t.Fatalf("run after cancel = %#v, want canceled", updated)
	}
}

func TestDeleteOrchestrationSessionArchivesAndHidesItFromList(t *testing.T) {
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	h := &orchestratorHandler{store: orchestrator.NewStore()}
	deleted, err := h.store.CreateOrchSession("test", "")
	if err != nil {
		t.Fatal(err)
	}
	kept, err := h.store.CreateOrchSession("keep", "")
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodDelete,
		"/orchestrator/api/orch-sessions/"+deleted.ID, nil)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d: %s", w.Code, w.Body.String())
	}

	archived, ok := h.store.GetOrchSession(deleted.ID)
	if !ok || archived.Status != "archived" {
		t.Fatalf("deleted session = %#v, want archived", archived)
	}
	listed := h.store.ListOrchSessions()
	if len(listed) != 1 || listed[0].ID != kept.ID {
		t.Fatalf("listed sessions = %#v, want only %s", listed, kept.ID)
	}

	listReq := httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions", nil)
	listW := httptest.NewRecorder()
	h.ServeHTTP(listW, listReq)
	if listW.Code != http.StatusOK {
		t.Fatalf("list status = %d: %s", listW.Code, listW.Body.String())
	}
	var response []orchestrator.OrchestrationSession
	if err := json.Unmarshal(listW.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response) != 1 || response[0].ID != kept.ID {
		t.Fatalf("API listed sessions = %#v, want only %s", response, kept.ID)
	}
}

func jsonReader(body string) *strings.Reader { return strings.NewReader(body) }

func TestOrchestrationSessionListReturnsSummary(t *testing.T) {
	root := t.TempDir()
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)

	h := &orchestratorHandler{store: orchestrator.NewStore()}
	sess, err := h.store.CreateOrchSession("large history", "G:\\workspace")
	if err != nil {
		t.Fatal(err)
	}
	if err := h.store.UpdateOrchSession(sess.ID, func(s *orchestrator.OrchestrationSession) {
		s.ChatMessages = []orchestrator.ChatMsg{{Role: "user", Text: strings.Repeat("history ", 10000)}}
		s.RequirementMessages = []orchestrator.ChatMsg{{Role: "user", Text: "requirements"}}
		s.PipelineRevisionIDs = []string{"rev_1", "rev_2"}
		s.RunIDs = []string{"run_1"}
	}); err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("list sessions status = %d: %s", w.Code, w.Body.String())
	}
	var summaries []map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &summaries); err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 1 {
		t.Fatalf("summary count = %d, want 1", len(summaries))
	}
	for _, field := range []string{"id", "title", "workspace", "createdAt", "updatedAt"} {
		if _, ok := summaries[0][field]; !ok {
			t.Errorf("summary is missing %q: %s", field, w.Body.String())
		}
	}
	for _, field := range []string{"chatMessages", "requirementMessages", "pipelineRevisionIDs", "runIDs"} {
		if _, ok := summaries[0][field]; ok {
			t.Errorf("summary unexpectedly contains large field %q", field)
		}
	}
}

func TestOrchestrationSessionListLimitReturnsNewest(t *testing.T) {
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	h := &orchestratorHandler{store: orchestrator.NewStore()}
	for _, title := range []string{"old", "middle", "new"} {
		if _, err := h.store.CreateOrchSession(title, ""); err != nil {
			t.Fatal(err)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/orchestrator/api/orch-sessions?limit=2", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("list sessions status = %d: %s", w.Code, w.Body.String())
	}
	var summaries []orchestrator.OrchestrationSessionSummary
	if err := json.Unmarshal(w.Body.Bytes(), &summaries); err != nil {
		t.Fatal(err)
	}
	if len(summaries) != 2 {
		t.Fatalf("limited summary count = %d, want 2", len(summaries))
	}
	if summaries[0].Title != "new" || summaries[1].Title != "middle" {
		t.Fatalf("summaries are not newest-first: %+v", summaries)
	}
}

func TestUpdateCurrentRevisionWithoutLoopConfigPreservesPersistedLoop(t *testing.T) {
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", t.TempDir())
	h := &orchestratorHandler{store: orchestrator.NewStore()}
	sess, err := h.store.CreateOrchSession("preserve loop", "")
	if err != nil {
		t.Fatal(err)
	}
	nodes := []orchestrator.AgentNode{
		{ID: "executor", Type: orchestrator.NodeExecutor},
		{ID: "reviewer", Type: orchestrator.NodeReviewer},
	}
	rev, err := h.store.CreatePipelineRevision(sess.ID, orchestrator.PipelineRevision{Nodes: nodes, LoopConfig: orchestrator.LoopConfig{
		Enabled: true, Mode: "fixed", FixedIterations: 3, ReviewNodeID: "reviewer", Protocol: "loop-review-v1",
	}})
	if err != nil {
		t.Fatal(err)
	}

	body := `{"name":"edited","nodes":[{"id":"executor","type":"executor"},{"id":"reviewer","type":"reviewer"}],"edges":[],"baseRevisionID":"` + rev.ID + `","source":"manual_edit"}`
	req := httptest.NewRequest(http.MethodPut, "/orchestrator/api/orch-sessions/"+sess.ID+"/pipelines/current", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("update status = %d: %s", w.Code, w.Body.String())
	}
	current, ok := h.store.GetCurrentRevision(sess.ID)
	if !ok {
		t.Fatal("current revision not found")
	}
	if !current.LoopConfig.Enabled || current.LoopConfig.Mode != "fixed" || current.LoopConfig.FixedIterations != 3 {
		t.Fatalf("loop config was lost when omitted: %+v", current.LoopConfig)
	}
}
