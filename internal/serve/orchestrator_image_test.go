package serve

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func imageTestHandler(t *testing.T) *orchestratorHandler {
	t.Helper()
	h := newOrchestratorHandler(nil)
	return h
}

// tiny 1x1 PNG（魔数可被 DetectContentType 识别为 image/png）
var tinyPNGBytes = []byte{
	0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A,
	0x00, 0x00, 0x00, 0x0D, 0x49, 0x48, 0x44, 0x52,
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4,
	0x89, 0x00, 0x00, 0x00, 0x0D, 0x49, 0x44, 0x41,
	0x54, 0x78, 0x9C, 0x63, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00,
	0x00, 0x00, 0x00, 0x49, 0x45, 0x4E, 0x44, 0xAE,
	0x42, 0x60, 0x82,
}

func TestUploadImageAcceptsPNG(t *testing.T) {
	h := imageTestHandler(t)
	body, _ := json.Marshal(map[string]string{
		"data": base64.StdEncoding.EncodeToString(tinyPNGBytes),
		"name": "shot.png",
	})
	req := httptest.NewRequest(http.MethodPost, "/orchestrator/api/upload-image", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.uploadImage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200, body=%s", w.Code, w.Body.String())
	}
	var out struct {
		ID   string `json:"id"`
		Name string `json:"name"`
		URL  string `json:"url"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.ID == "" || out.Name != "shot.png" || out.URL != "/orchestrator/api/images/"+out.ID {
		t.Fatalf("bad response: %+v", out)
	}
	if _, err := os.Stat(filepath.Join(imageAttachmentDir(), out.ID+".png")); err != nil {
		t.Fatalf("image file not persisted: %v", err)
	}
}

func TestUploadImageRejectsNonImage(t *testing.T) {
	h := imageTestHandler(t)
	body, _ := json.Marshal(map[string]string{
		"data": base64.StdEncoding.EncodeToString([]byte("hello world, definitely not an image")),
		"name": "fake.png",
	})
	req := httptest.NewRequest(http.MethodPost, "/orchestrator/api/upload-image", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.uploadImage(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestUploadImageRejectsOversized(t *testing.T) {
	h := imageTestHandler(t)
	big := make([]byte, 10*1024*1024+1024) // > 10MB
	for i := range big {
		big[i] = byte(i % 251)
	}
	// 前缀补 PNG 魔数以通过类型校验，但长度超限
	raw := append(append([]byte{}, tinyPNGBytes...), big...)
	body, _ := json.Marshal(map[string]string{
		"data": base64.StdEncoding.EncodeToString(raw),
		"name": "big.png",
	})
	req := httptest.NewRequest(http.MethodPost, "/orchestrator/api/upload-image", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.uploadImage(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
}

func TestServeImageReturnsContentType(t *testing.T) {
	h := imageTestHandler(t)
	body, _ := json.Marshal(map[string]string{
		"data": base64.StdEncoding.EncodeToString(tinyPNGBytes),
		"name": "shot.png",
	})
	req := httptest.NewRequest(http.MethodPost, "/orchestrator/api/upload-image", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.uploadImage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("upload failed: %d %s", w.Code, w.Body.String())
	}
	var out struct {
		ID string `json:"id"`
	}
	json.Unmarshal(w.Body.Bytes(), &out)

	imgReq := httptest.NewRequest(http.MethodGet, "/orchestrator/api/images/"+out.ID, nil)
	imgW := httptest.NewRecorder()
	h.serveImage(imgW, imgReq)
	if imgW.Code != http.StatusOK {
		t.Fatalf("serve status = %d, want 200", imgW.Code)
	}
	if ct := imgW.Header().Get("Content-Type"); !strings.HasPrefix(ct, "image/png") {
		t.Fatalf("content-type = %q, want image/png", ct)
	}
	if !bytes.Equal(imgW.Body.Bytes(), tinyPNGBytes) {
		t.Fatal("served bytes differ from uploaded bytes")
	}
}

func TestServeImageUnknownID404(t *testing.T) {
	h := imageTestHandler(t)
	// well-formed id (13 digits + "_" + 8 hex) that was never uploaded
	req := httptest.NewRequest(http.MethodGet, "/orchestrator/api/images/0000000000000_00000000", nil)
	w := httptest.NewRecorder()
	h.serveImage(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

// TestServeImageRejectsGlobMetaIDs guards the strict id validation: glob
// metacharacters must never be interpreted as patterns or serve stored files.
func TestServeImageRejectsGlobMetaIDs(t *testing.T) {
	h := imageTestHandler(t)
	body, _ := json.Marshal(map[string]string{
		"data": base64.StdEncoding.EncodeToString(tinyPNGBytes),
		"name": "shot.png",
	})
	req := httptest.NewRequest(http.MethodPost, "/orchestrator/api/upload-image", bytes.NewReader(body))
	w := httptest.NewRecorder()
	h.uploadImage(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("upload failed: %d %s", w.Code, w.Body.String())
	}
	for _, id := range []string{"*", "?", "[abc]", "*.png"} {
		imgReq := httptest.NewRequest(http.MethodGet, "/orchestrator/api/images/"+id, nil)
		imgW := httptest.NewRecorder()
		h.serveImage(imgW, imgReq)
		if imgW.Code != http.StatusBadRequest {
			t.Fatalf("id %q: status = %d, want 400", id, imgW.Code)
		}
	}
}
