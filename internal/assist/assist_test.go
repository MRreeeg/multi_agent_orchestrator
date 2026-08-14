package assist

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestImageMime(t *testing.T) {
	cases := map[string]string{
		"a.png":   "image/png",
		"a.jpg":   "image/jpeg",
		"a.jpeg":  "image/jpeg",
		"a.gif":   "image/gif",
		"a.webp":  "image/webp",
		"a.bmp":   "image/bmp",
		"a.weird": "image/png",
	}
	for path, want := range cases {
		if got := imageMime(path); got != want {
			t.Errorf("imageMime(%q) = %q, want %q", path, got, want)
		}
	}
}

func TestRunSendsTaskAndImages(t *testing.T) {
	dir := t.TempDir()
	img := filepath.Join(dir, "shot.png")
	if err := os.WriteFile(img, []byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a}, 0o600); err != nil {
		t.Fatal(err)
	}

	var gotReq struct {
		Model    string `json:"model"`
		Messages []struct {
			Role    string `json:"role"`
			Content []struct {
				Type     string `json:"type"`
				Text     string `json:"text"`
				ImageURL *struct {
					URL string `json:"url"`
				} `json:"image_url"`
			} `json:"content"`
		} `json:"messages"`
		MaxTokens int `json:"max_tokens"`
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/chat/completions" {
			t.Errorf("path = %q, want /chat/completions", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("auth = %q, want Bearer test-key", got)
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &gotReq); err != nil {
			t.Errorf("bad request json: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"choices":[{"message":{"content":"图中有一个红色按钮"}}]}`)
	}))
	defer ts.Close()

	got, err := Run(Options{
		Task:     "描述这张截图",
		Images:   []string{img},
		Endpoint: ts.URL,
		Model:    "mimo-v2.5",
		APIKey:   "test-key",
		Timeout:  5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got != "图中有一个红色按钮" {
		t.Errorf("result = %q, want completion text", got)
	}
	if gotReq.Model != "mimo-v2.5" {
		t.Errorf("model = %q, want mimo-v2.5", gotReq.Model)
	}
	if len(gotReq.Messages) != 1 || gotReq.Messages[0].Role != "user" {
		t.Fatalf("messages = %+v, want one user message", gotReq.Messages)
	}
	parts := gotReq.Messages[0].Content
	if len(parts) != 2 {
		t.Fatalf("content parts = %d, want 2 (text + image)", len(parts))
	}
	if parts[0].Type != "text" || parts[0].Text != "描述这张截图" {
		t.Errorf("first part = %+v, want text task", parts[0])
	}
	if parts[1].Type != "image_url" || parts[1].ImageURL == nil {
		t.Fatalf("second part = %+v, want image_url", parts[1])
	}
	wantData := "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a})
	if parts[1].ImageURL.URL != wantData {
		t.Errorf("image url = %q..., want data URI", parts[1].ImageURL.URL[:40])
	}
	if gotReq.MaxTokens <= 0 {
		t.Errorf("max_tokens = %d, want > 0", gotReq.MaxTokens)
	}
}

func TestRunMissingImage(t *testing.T) {
	_, err := Run(Options{
		Task:     "看图",
		Images:   []string{filepath.Join(t.TempDir(), "nope.png")},
		Endpoint: "http://127.0.0.1:1",
	})
	if err == nil || !strings.Contains(err.Error(), "cannot read image") {
		t.Fatalf("err = %v, want cannot read image", err)
	}
}

func TestRunEmptyTaskAndNoImages(t *testing.T) {
	_, err := Run(Options{Endpoint: "http://127.0.0.1:1"})
	if err == nil || !strings.Contains(err.Error(), "empty task") {
		t.Fatalf("err = %v, want empty task error", err)
	}
}

func TestRunHTTPError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, "bad model")
	}))
	defer ts.Close()
	_, err := Run(Options{Task: "x", Endpoint: ts.URL, APIKey: "k", Timeout: 5 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("err = %v, want 400", err)
	}
}
