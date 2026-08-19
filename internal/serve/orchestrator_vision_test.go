package serve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"reasonix/internal/assist"
)

func TestBuildHistoryTextInjectsVision(t *testing.T) {
	dir := imageAttachmentDir()
	os.MkdirAll(dir, 0755)
	id := "1234567890123_abcdef12"
	os.WriteFile(filepath.Join(dir, id+".png"), tinyPNGBytes, 0644)
	orig := analyzeVision
	analyzeVision = func(o assist.Options) (string, error) {
		return "设计稿：登录表单", nil
	}
	defer func() { analyzeVision = orig }()

	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{{ID: id, Name: "login.png"}}
	got := buildHistoryTextWithImages("[用户]: 做登录页", images)
	if !strings.Contains(got, "[图片 login.png]: 设计稿：登录表单") {
		t.Fatalf("missing vision injection, got:\n%s", got)
	}
}

func TestBuildHistoryTextVisionFailureDegrades(t *testing.T) {
	orig := analyzeVision
	analyzeVision = func(o assist.Options) (string, error) {
		return "", fmt.Errorf("vision api down")
	}
	defer func() { analyzeVision = orig }()

	// well-formed id (13 digits + "_" + 8 hex) with no backing file: the
	// vision call itself fails, which must degrade to a placeholder
	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{{ID: "1234567890123_00000000", Name: "a.png"}}
	got := buildHistoryTextWithImages("[用户]: x", images)
	if !strings.Contains(got, "[图片 a.png 无法解析：vision api down]") {
		t.Fatalf("missing degrade injection, got:\n%s", got)
	}
	if !strings.Contains(got, "[用户]: x") {
		t.Fatal("history text must survive vision failure")
	}
}

// TestBuildHistoryTextWithImagesSkipsInvalidIDs guards the analyze path against
// the glob-metacharacter bypass fixed for serveImage: an image id failing
// imageIDRe (e.g. "*" globbing every stored attachment, or a path escape) must
// never reach analyzeVision or filepath.Glob.
func TestBuildHistoryTextWithImagesSkipsInvalidIDs(t *testing.T) {
	dir := imageAttachmentDir()
	os.MkdirAll(dir, 0755)
	validID := "1234567890123_abcdef34"
	os.WriteFile(filepath.Join(dir, validID+".png"), tinyPNGBytes, 0644)
	var calls []string
	orig := analyzeVision
	analyzeVision = func(o assist.Options) (string, error) {
		calls = append(calls, o.Images[0])
		return "转述成功", nil
	}
	defer func() { analyzeVision = orig }()

	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{
		{ID: "*", Name: "glob.png"},
		{ID: "..", Name: "dotdot.png"},
		{ID: "not-an-id", Name: "plain.png"},
		{ID: validID, Name: "ok.png"},
	}
	got := buildHistoryTextWithImages("[用户]: x", images)
	if len(calls) != 1 || calls[0] != filepath.Join(dir, validID+".png") {
		t.Fatalf("vision calls = %v, want only the valid id path", calls)
	}
	if !strings.Contains(got, "[图片 ok.png]: 转述成功") {
		t.Fatalf("missing valid vision injection, got:\n%s", got)
	}
	for _, bad := range []string{"glob.png", "dotdot.png", "plain.png"} {
		if strings.Contains(got, bad) {
			t.Fatalf("invalid id %q leaked into output, got:\n%s", bad, got)
		}
	}
}

// TestBuildHistoryTextWithImagesDedupsIDs: the same image id must be
// transcribed at most once (keep first occurrence, preserve order).
func TestBuildHistoryTextWithImagesDedupsIDs(t *testing.T) {
	dir := imageAttachmentDir()
	os.MkdirAll(dir, 0755)
	validID := "1234567890123_abcdef34"
	os.WriteFile(filepath.Join(dir, validID+".png"), tinyPNGBytes, 0644)
	var calls []string
	orig := analyzeVision
	analyzeVision = func(o assist.Options) (string, error) {
		calls = append(calls, o.Images[0])
		return "转述成功", nil
	}
	defer func() { analyzeVision = orig }()

	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{
		{ID: validID, Name: "first.png"},
		{ID: validID, Name: "dup.png"},
		{ID: validID, Name: "dup2.png"},
	}
	got := buildHistoryTextWithImages("[用户]: x", images)
	if len(calls) != 1 {
		t.Fatalf("vision calls = %d, want 1 (dedup), calls=%v", len(calls), calls)
	}
	if !strings.Contains(got, "[图片 first.png]: 转述成功") {
		t.Fatalf("first occurrence must be kept, got:\n%s", got)
	}
	if strings.Contains(got, "dup.png") || strings.Contains(got, "dup2.png") {
		t.Fatalf("duplicates must not be transcribed, got:\n%s", got)
	}
}

// TestAnalyzeRequirementRejectsTooManyImages: an analyze body carrying more
// than maxAnalyzeImages images must be rejected with 400 before any vision
// transcription (each entry costs an assist.Run network call).
func TestAnalyzeRequirementRejectsTooManyImages(t *testing.T) {
	h := newOrchestratorHandler(nil)
	var images []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	for i := 0; i < 21; i++ {
		images = append(images, struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		}{ID: fmt.Sprintf("1234567890123_%08x", i), Name: "a.png"})
	}
	body, _ := json.Marshal(map[string]any{
		"text":   "做个登录页",
		"images": images,
	})
	req := httptest.NewRequest(http.MethodPost, "/orchestrator/api/requirements/analyze", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.analyzeRequirement(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400, body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "20") {
		t.Fatalf("error must mention the image cap, got: %s", w.Body.String())
	}
}
