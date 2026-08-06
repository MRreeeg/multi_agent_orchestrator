package opencode

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	text, err := c.Prompt(ctx, id, "opencode/deepseek-v4-flash-free", "hello")
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
