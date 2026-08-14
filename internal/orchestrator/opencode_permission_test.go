package orchestrator

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	opencodeclient "reasonix/internal/executor/opencode"
)

func TestOpencodePermissionConfig(t *testing.T) {
	ask := opencodePermissionConfig(ExecSpec{ApprovalMode: "ask"})
	if !strings.Contains(ask, `"question":"deny"`) || strings.Contains(ask, `"*":"allow"`) {
		t.Fatalf("ask config = %s", ask)
	}
	auto := opencodePermissionConfig(ExecSpec{ApprovalMode: "auto"})
	if !strings.Contains(auto, `"*":"allow"`) || !strings.Contains(auto, `"question":"deny"`) {
		t.Fatalf("auto config = %s", auto)
	}
	yolo := opencodePermissionConfig(ExecSpec{ApprovalMode: "yolo"})
	if !strings.Contains(yolo, `"*":"allow"`) {
		t.Fatalf("yolo config = %s", yolo)
	}
}

func TestOpencodeResponseForAction(t *testing.T) {
	cases := map[string]string{
		"allow_once":   "once",
		"allow_always": "always",
		"reject":       "reject",
	}
	for in, want := range cases {
		if got := opencodeResponseForAction(in); got != want {
			t.Fatalf("opencodeResponseForAction(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestOpencodePermissionUpdatedParksInAskMode verifies that ask mode parks
// the permission request for a Runtime Console card and never answers it.
func TestOpencodePermissionUpdatedParksInAskMode(t *testing.T) {
	mgr := newOpenCodeRuntimeManager()
	rt := &opencodeRuntime{
		ID:           "oc_rt_test",
		ApprovalMode: "ask",
		pendingPerms: make(map[string]PermissionRequestInfo),
	}
	ev := opencodeSSEEvent{
		Type: "permission.updated",
		Properties: struct {
			SessionID    string `json:"sessionID"`
			MessageID    string `json:"messageID"`
			PartID       string `json:"partID"`
			Field        string `json:"field"`
			Delta        string `json:"delta"`
			Part         struct {
				ID    string `json:"id"`
				Type  string `json:"type"`
				Text  string `json:"text"`
				State string `json:"state"`
			} `json:"part"`
			Permission struct {
				ID        string `json:"id"`
				Type      string `json:"type"`
				Title     string `json:"title"`
				Pattern   string `json:"pattern"`
				SessionID string `json:"sessionID"`
				Time      struct {
					Created int64 `json:"created"`
				} `json:"time"`
			} `json:"permission"`
			PermissionID string `json:"permissionID"`
		}{
			Permission: struct {
				ID        string `json:"id"`
				Type      string `json:"type"`
				Title     string `json:"title"`
				Pattern   string `json:"pattern"`
				SessionID string `json:"sessionID"`
				Time      struct {
					Created int64 `json:"created"`
				} `json:"time"`
			}{
				ID: "perm_1", Type: "bash", Title: "bash", Pattern: "git status*",
				SessionID: "ses-1",
				Time:      struct{ Created int64 `json:"created"` }{Created: time.Now().UnixMilli()},
			},
		},
	}
	mgr.handlePermissionUpdated(rt, ev)
	rt.mu.Lock()
	parked, ok := rt.pendingPerms["perm_1"]
	rt.mu.Unlock()
	if !ok {
		t.Fatal("ask mode must park the permission request")
	}
	if parked.ToolName != "bash" || parked.ToolInput != "git status*" {
		t.Fatalf("parked card = %+v", parked)
	}
}

// TestOpencodePermissionUpdatedAutoApproves verifies that auto mode answers
// the request through the opencode permissions API ("always") instead of
// parking it.
func TestOpencodePermissionUpdatedAutoApproves(t *testing.T) {
	var gotPath, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		buf := make([]byte, 256)
		n, _ := r.Body.Read(buf)
		gotBody = string(buf[:n])
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`true`))
	}))
	defer srv.Close()

	mgr := newOpenCodeRuntimeManager()
	rt := &opencodeRuntime{
		ID:           "oc_rt_test",
		ApprovalMode: "auto",
		sessionID:    "ses-1",
		client:       opencodeclient.NewClient(srv.URL),
		pendingPerms: make(map[string]PermissionRequestInfo),
	}
	ev := opencodeSSEEvent{Type: "permission.updated"}
	ev.Properties.Permission.ID = "perm_2"
	ev.Properties.Permission.Title = "bash"
	ev.Properties.Permission.SessionID = "ses-1"
	ev.Properties.Permission.Time.Created = time.Now().UnixMilli()

	mgr.handlePermissionUpdated(rt, ev)

	rt.mu.Lock()
	_, parked := rt.pendingPerms["perm_2"]
	rt.mu.Unlock()
	if parked {
		t.Fatal("auto mode must not park the permission request")
	}
	if !strings.HasSuffix(gotPath, "/session/ses-1/permissions/perm_2") {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.Contains(gotBody, `"response":"always"`) {
		t.Fatalf("body = %q, want response always", gotBody)
	}
}