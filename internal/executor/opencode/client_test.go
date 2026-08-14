package opencode

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientPrompt(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/session":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"id":"ses_test"}`))
		case "/session/ses_test/message":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"info":{"id":"m1"},"parts":[{"type":"text","text":"hello opencode"}]}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	ctx := context.Background()
	id, err := c.NewSession(ctx, "test")
	if err != nil || id != "ses_test" {
		t.Fatalf("NewSession = %q, %v", id, err)
	}
	text, err := c.Prompt(ctx, id, "opencode/deepseek-v4-flash-free", "", "hello", nil)
	if err != nil || text != "hello opencode" {
		t.Fatalf("Prompt = %q, %v", text, err)
	}
}

func TestClientAbort(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/abort" && r.Method == http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`true`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	if err := c.Abort(context.Background(), "ses_test"); err != nil {
		t.Fatalf("Abort: %v", err)
	}
}

func TestClientRespondPermission(t *testing.T) {
	var gotBody, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/permissions/perm_1" && r.Method == http.MethodPost {
			buf := make([]byte, 256)
			n, _ := r.Body.Read(buf)
			gotBody = string(buf[:n])
			gotPath = r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`true`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	c := NewClient(srv.URL)
	ctx := context.Background()
	if err := c.RespondPermission(ctx, "ses_test", "perm_1", "always"); err != nil {
		t.Fatalf("RespondPermission: %v", err)
	}
	if gotPath != "/session/ses_test/permissions/perm_1" {
		t.Fatalf("path = %q", gotPath)
	}
	if !strings.Contains(gotBody, `"response":"always"`) {
		t.Fatalf("body = %q, want response always", gotBody)
	}
	if err := c.RespondPermission(ctx, "ses_test", "perm_1", "bogus"); err == nil {
		t.Fatal("bogus response value must be rejected")
	}
}

func TestClientHistory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/message" && r.Method == http.MethodGet {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"info":{"id":"m1","role":"user"},"parts":[{"type":"text","text":"hi"}]}]`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	msgs, err := c.History(context.Background(), "ses_test")
	if err != nil || len(msgs) != 1 || msgs[0].Text != "hi" {
		t.Fatalf("History = %+v, %v", msgs, err)
	}
}

func TestClientPromptSystem(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/message" {
			buf := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(buf)
			gotBody = string(buf)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"info":{"id":"m1"},"parts":[{"type":"text","text":"ok"}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	_, _ = c.Prompt(context.Background(), "ses_test", "", "discipline-beacon", "hi", nil)
	if !strings.Contains(gotBody, `"system":"discipline-beacon"`) {
		t.Fatalf("system was not sent in payload: %s", gotBody)
	}
}

func TestClientPromptNoTools(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/message" {
			buf := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(buf)
			gotBody = string(buf)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"info":{"id":"m1"},"parts":[{"type":"text","text":"ok"}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	_, _ = c.Prompt(context.Background(), "ses_test", "", "", "hi", map[string]bool{"*": false})
	if !strings.Contains(gotBody, `"*":false`) {
		t.Fatalf("noTools did not produce deny-all tools map: %s", gotBody)
	}
}

func TestClientPromptDenyMap(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/message" {
			buf := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(buf)
			gotBody = string(buf)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"info":{"id":"m1"},"parts":[{"type":"text","text":"ok"}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	_, _ = c.Prompt(context.Background(), "ses_test", "", "", "hi", map[string]bool{"bash": false, "edit": false})
	if !strings.Contains(gotBody, `"bash":false`) || !strings.Contains(gotBody, `"edit":false`) {
		t.Fatalf("deny map not sent as-is: %s", gotBody)
	}
}

func TestClientPromptModelObject(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/session/ses_test/message" {
			buf := make([]byte, r.ContentLength)
			_, _ = r.Body.Read(buf)
			gotBody = string(buf)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"info":{"id":"m1"},"parts":[{"type":"text","text":"ok"}]}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	c := NewClient(srv.URL)
	_, _ = c.Prompt(context.Background(), "ses_test", "opencode/deepseek-v4-flash-free", "", "hi", nil)
	if !strings.Contains(gotBody, `"providerID":"opencode"`) || !strings.Contains(gotBody, `"modelID":"deepseek-v4-flash-free"`) {
		t.Fatalf("model was not sent as object: %s", gotBody)
	}
}
