# 聊天输入框图片附件 + 需求分析视觉上下文（L-54）实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用户可拖拽或通过 `+` 按钮把图片附件加入聊天输入框，随需求一起发送；后端用视觉模型（assist）转述图片后注入分析 prompt，图片缩略图在对话中展示并随会话持久化。

**Architecture:** 前端 composer 新增附件区与拖拽/文件选择；图片经 `POST /orchestrator/api/upload-image` 落盘 `<DataRoot>/attachments/<id>.<ext>`；`analyzeRequirement` 接收 `images`，逐张调 `assist.Run`（mimo-v2.5 视觉转述）注入 historyText 后照常走 reasonix 子进程分析；`ChatMsg` 增加 `Images []ChatImage` 随会话 conversation API 持久化；回读走 `GET /orchestrator/api/images/{id}`。

**Tech Stack:** Go 1.22+（net/http ServeMux 方法路由）、内嵌单文件前端 index.html（原生 JS）、assist 包（视觉转述）、orchestrator.Store（会话持久化）。

## Global Constraints

- 图片大小上限 10MB；类型白名单 `image/png|jpeg|gif|webp|bmp`（以 `http.DetectContentType` 魔数校验，与声明 MIME 不符即拒绝）。
- 图片文件不依赖 sessionID，存 `<DataRoot>/attachments/<id>.<ext>`；`id = <unixms>_<rand8>`。
- 前端不接触磁盘路径：消息持久化只存 `{id, name}`，回读 URL 统一 `/orchestrator/api/images/<id>`。
- 视觉转述失败必须降级：注入 `[图片 <name> 无法解析：<err>]` 占位，**不阻塞**分析。
- 转述调用走包级可替换变量 `analyzeVision = assist.Run`（测试注入）。
- 提交消息带 `（L-54）` 后缀；每任务完成即 commit。

---

### Task 1: 后端图片上传与回读端点

**Files:**
- Modify: `internal/serve/orchestrator.go`（orchestratorAPI switch + 两个 handler + imports）
- Modify: `internal/serve/serve.go`（两行路由注册）
- Test: `internal/serve/orchestrator_image_test.go`（新建）

**Interfaces:**
- Consumes: `orchestrator.DataRoot()`（已导出，persistence.go:28）；`writeJSON`（serve.go:910）、`writeErr`（orchestrator.go:340）
- Produces:
  - `func (h *orchestratorHandler) uploadImage(w http.ResponseWriter, r *http.Request)` — POST，body `{"data":"<base64>","name":"a.png"}` → `{"id":"<id>","name":"a.png","url":"/orchestrator/api/images/<id>"}`
  - `func (h *orchestratorHandler) serveImage(w http.ResponseWriter, r *http.Request)` — GET `/orchestrator/api/images/<id>`，按 `<DataRoot>/attachments/<id>.*` 回读
  - `func imageAttachmentDir() string` = `filepath.Join(orchestrator.DataRoot(), "attachments")`

- [ ] **Step 1: 写失败测试** `internal/serve/orchestrator_image_test.go`

```go
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
	req := httptest.NewRequest(http.MethodGet, "/orchestrator/api/images/does-not-exist", nil)
	w := httptest.NewRecorder()
	h.serveImage(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/serve/ -run 'TestUpload|TestServeImage' -v`
Expected: 编译失败（uploadImage/serveImage/imageAttachmentDir 未定义）

- [ ] **Step 3: 实现**。在 `internal/serve/orchestrator.go` 顶部 imports 增加 `crypto/rand`、`encoding/base64`、`encoding/hex`、`mime`、`net/http`（已有？检查）。新增：

```go
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

// serveImage reads back an uploaded image by id.
func (h *orchestratorHandler) serveImage(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/orchestrator/api/images/")
	if id == "" || strings.ContainsAny(id, `/\`) {
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
```

在 `orchestratorAPI` 的 switch 里加两个 case（放在 `case path == "/requirements/analyze"` 前后均可，放在它前面）：

```go
	case path == "/upload-image" && r.Method == http.MethodPost:
		h.uploadImage(w, r)
	case strings.HasPrefix(path, "/images/") && r.Method == http.MethodGet:
		h.serveImage(w, r)
```

在 `internal/serve/serve.go` 路由区（551 行 `POST /orchestrator/api/requirements/analyze` 旁）加：

```go
	mux.HandleFunc("POST /orchestrator/api/upload-image", s.orchestratorAPI)
	mux.HandleFunc("GET /orchestrator/api/images/", s.orchestratorAPI)
```

检查 imports：`internal/serve/orchestrator.go` 需新增 `crypto/rand`、`encoding/base64`、`encoding/hex`、`mime`；`fmt`/`os`/`path/filepath`/`strings`/`time` 已 import。`json`、`net/http` 已 import。

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./internal/serve/ -run 'TestUpload|TestServeImage' -v`
Expected: PASS（4 个用例）

- [ ] **Step 5: 全量编译 + 提交**

Run: `go build ./...` 且 `go vet ./internal/serve/`
Expected: 干净

```bash
git add internal/serve/orchestrator.go internal/serve/serve.go internal/serve/orchestrator_image_test.go
git commit -m "feat: 图片附件上传与回读端点（L-54）"
```

---

### Task 2: ChatMsg 图片字段持久化

**Files:**
- Modify: `internal/orchestrator/types.go:398-402`（ChatMsg）
- Test: `internal/orchestrator/types_image_test.go`（新建）

**Interfaces:**
- Consumes: 无（纯类型扩展）
- Produces: `type ChatImage struct{ ID, Name string }`；`ChatMsg.Images []ChatImage \`json:"images,omitempty"\``

- [ ] **Step 1: 写失败测试** `internal/orchestrator/types_image_test.go`

```go
package orchestrator

import (
	"encoding/json"
	"testing"
)

func TestChatMsgImagesRoundTrip(t *testing.T) {
	m := ChatMsg{
		Role:   "user",
		Text:   "看图",
		Meta:   "",
		Images: []ChatImage{{ID: "123_abcd", Name: "shot.png"}},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back ChatMsg
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Images) != 1 || back.Images[0].ID != "123_abcd" || back.Images[0].Name != "shot.png" {
		t.Fatalf("images = %+v", back.Images)
	}
}

func TestChatMsgImagesOmitEmpty(t *testing.T) {
	m := ChatMsg{Role: "ai", Text: "x"}
	b, _ := json.Marshal(m)
	if str := string(b); str != `{"role":"ai","text":"x"}` {
		t.Fatalf("marshal = %s", str)
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/orchestrator/ -run 'TestChatMsgImages' -v`
Expected: 编译失败（ChatImage 未定义）

- [ ] **Step 3: 实现** `internal/orchestrator/types.go:398`

```go
type ChatImage struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type ChatMsg struct {
	Role   string      `json:"role"`
	Text   string      `json:"text"`
	Meta   string      `json:"meta,omitempty"`
	Images []ChatImage `json:"images,omitempty"`
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./internal/orchestrator/ -run 'TestChatMsgImages' -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add internal/orchestrator/types.go internal/orchestrator/types_image_test.go
git commit -m "feat: ChatMsg 支持图片附件字段（L-54）"
```

---

### Task 3: 需求分析图片视觉转述注入

**Files:**
- Modify: `internal/serve/orchestrator.go`（analyzeRequirement body + 转述注入）
- Test: `internal/serve/orchestrator_vision_test.go`（新建）

**Interfaces:**
- Consumes: Task 1 的 `imageAttachmentDir()`；`internal/assist` 包（`Options`、`Run`）
- Produces:
  - 包级变量 `analyzeVision func(assist.Options) (string, error) = assist.Run`
  - analyzeRequirement body 增加 `Images []struct{ ID, Name string } \`json:"images"\``
  - 转述文本注入 historyText：每张图一行 `[图片 <name>]: <描述>`；失败 `[图片 <name> 无法解析：<err>]`
  - 日志 `slog.Info("analyze: images", "count", n, "vision_failed", k)`

- [ ] **Step 1: 写失败测试** `internal/serve/orchestrator_vision_test.go`

> 说明：`analyzeRequirement` 会真实启动 reasonix 子进程，测试不应依赖它成功。因此把**转述 + 注入**提取为独立纯函数 `buildHistoryTextWithImages(historyText string, images []struct{ID, Name string}) string`，测试只测该函数（不触发子进程）。

```go
package serve

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"reasonix/internal/assist"
)

func TestBuildHistoryTextInjectsVision(t *testing.T) {
	dir := imageAttachmentDir()
	os.MkdirAll(dir, 0755)
	id := "999_visiontest"
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

	images := []struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}{{ID: "missing", Name: "a.png"}}
	got := buildHistoryTextWithImages("[用户]: x", images)
	if !strings.Contains(got, "[图片 a.png 无法解析：vision api down]") {
		t.Fatalf("missing degrade injection, got:\n%s", got)
	}
	if !strings.Contains(got, "[用户]: x") {
		t.Fatal("history text must survive vision failure")
	}
}
```

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./internal/serve/ -run 'TestBuildHistoryText' -v`
Expected: 编译失败（buildHistoryTextWithImages / analyzeVision 未定义）

- [ ] **Step 3: 实现** `internal/serve/orchestrator.go`

包级变量（放在 analyzeRequirement 前）：

```go
// analyzeVision 把一张图片转述为文本；默认为 assist.Run，测试可替换。
var analyzeVision = assist.Run
```

analyzeRequirement body 增加：

```go
		Images       []struct {
			ID   string `json:"id"`
			Name string `json:"name"`
		} `json:"images"`
```

在 `historyText += fmt.Sprintf("[用户]: %s", body.Text)` 之后（body.PipelineInfo 拼接之前）插入：

```go
	historyText = buildHistoryTextWithImages(historyText, body.Images)
```

新增函数：

```go
// buildHistoryTextWithImages 对每张图片做视觉转述并注入 historyText。
// 转述失败降级为占位文本，绝不阻塞需求分析。
func buildHistoryTextWithImages(historyText string, images []struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}) string {
	var b strings.Builder
	b.WriteString(historyText)
	visionFailed := 0
	for _, img := range images {
		path := filepath.Join(imageAttachmentDir(), img.ID+".png")
		// 上传时按真实类型落盘，可能是 .jpg/.gif 等；先 glob 定位
		if matches, err := filepath.Glob(filepath.Join(imageAttachmentDir(), img.ID+".*")); err == nil && len(matches) > 0 {
			path = matches[0]
		}
		desc, err := analyzeVision(assist.Options{
			Task:   "请用中文描述这张图片的内容与关键细节（用户把它作为需求上下文：可能是 UI 截图、设计稿或报错截图）。",
			Images: []string{path},
		})
		if err != nil {
			visionFailed++
			b.WriteString(fmt.Sprintf("\n[图片 %s 无法解析：%v]", img.Name, err))
			continue
		}
		b.WriteString(fmt.Sprintf("\n[图片 %s]: %s", img.Name, desc))
	}
	if len(images) > 0 {
		slog.Info("analyze: vision", "count", len(images), "failed", visionFailed)
	}
	return b.String()
}
```

imports 增加：`"reasonix/internal/assist"`。

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./internal/serve/ -run 'TestBuildHistoryText' -v`
Expected: PASS（2 个用例）

- [ ] **Step 5: 全量编译 + 提交**

Run: `go build ./...` 且 `go vet ./internal/serve/`
Expected: 干净

```bash
git add internal/serve/orchestrator.go internal/serve/orchestrator_vision_test.go
git commit -m "feat: 需求分析图片视觉转述注入（L-54）"
```

---

### Task 4: 前端输入区附件 UI（+ 按钮、拖拽、待发送区）

**Files:**
- Modify: `internal/serve/orchestrator_frontend/index.html`
- Test: `node --check`（提取内联脚本）

**Interfaces:**
- Consumes: `POST /orchestrator/api/upload-image`（body `{data,name}` → `{id,name,url}`）；`toast(msg, type)`；`apiTimeoutMs`（已有）；`state`（1272）
- Produces:
  - DOM：`#chat-attach-btn`（+ 按钮）、`#chat-file-input`（隐藏 file input）、`#chat-attachments`（待发送区）
  - `state.pendingAttachments: [{id, name, url}]`
  - `async function uploadPendingImage(file)` → 成功入 `state.pendingAttachments` 并 render，失败 toast
  - `function renderPendingAttachments()`、`function removePendingAttachment(id)`
  - `function isImageFile(file)`（`file.type.startsWith('image/')`）
  - 拖拽监听：`chatInputBar`（或 composer）dragover/dragleave/drop；`dataTransfer.files` 过滤图片

- [ ] **Step 1: 加 HTML 结构**（`index.html:601-608` chat__input-bar 内，composer 之前）：

```html
        <div class="chat__input-bar" id="chat-input-bar">
          <div class="chat__attachments" id="chat-attachments" style="display:none"></div>
          <div class="chat__composer">
            <input type="file" id="chat-file-input" accept="image/*" multiple style="display:none" />
            <button class="chat__attach-btn" id="chat-attach-btn" title="添加图片">＋</button>
            <textarea class="chat__input" id="chat-input" rows="1" placeholder="输入需求，Enter 发送，Shift+Enter 换行…" autocomplete="off"></textarea>
            <button class="chat__send" id="chat-send" title="生成编排">
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><line x1="12" y1="19" x2="12" y2="5"/><polyline points="5 12 12 5 19 12"/></svg>
            </button>
          </div>
        </div>
```

- [ ] **Step 2: 加 CSS**（`.chat__composer` 附近，186 行后）：

```css
.chat__attachments{display:flex;flex-wrap:wrap;gap:8px;width:min(860px,100%);margin:0 auto 8px}
.chat__attach-chip{position:relative;display:flex;align-items:center;gap:6px;border:1px solid var(--border);border-radius:8px;padding:4px 8px;background:var(--bg-2, var(--bg))}
.chat__attach-chip img{width:36px;height:36px;object-fit:cover;border-radius:4px}
.chat__attach-chip .chat__attach-name{max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}
.chat__attach-chip .chat__attach-del{cursor:pointer;border:none;background:none;color:var(--fg-2, #999);font-size:14px;line-height:1}
.chat__attach-btn{flex-shrink:0;align-self:stretch;width:38px;border:1px solid var(--border);border-radius:8px;background:var(--bg-2, var(--bg));color:var(--fg, inherit);font-size:18px;cursor:pointer}
.chat__attach-btn:hover{background:var(--bg-3, var(--bg))}
.chat__composer.is-dragging{outline:2px dashed var(--accent, #4a9eff);outline-offset:-4px;border-radius:10px}
```

- [ ] **Step 3: 加 JS**。DOM refs（`index.html:1562` 后）：

```js
const chatAttachBtn = $('#chat-attach-btn');
const chatFileInput = $('#chat-file-input');
const chatAttachments = $('#chat-attachments');
```

state（`index.html:1318` 前）加 `pendingAttachments: []`：

```js
  pendingAttachments: [], // 待发送图片附件 [{id, name, url}]
```

事件绑定与函数（放在 `sendChat` 定义之前、toast 定义之后均可；建议放在 DOM refs 之后的新函数区）：

```js
function isImageFile(file) {
  return file && file.type && file.type.startsWith('image/');
}

function renderPendingAttachments() {
  if (!chatAttachments) return;
  chatAttachments.innerHTML = '';
  state.pendingAttachments.forEach(a => {
    const chip = document.createElement('div');
    chip.className = 'chat__attach-chip';
    chip.innerHTML = '<img src="' + escAttr(a.url) + '" alt="" />' +
      '<span class="chat__attach-name">' + escHtml(a.name) + '</span>' +
      '<button class="chat__attach-del" title="移除" onclick="removePendingAttachment(\'' + a.id + '\')">×</button>';
    chatAttachments.appendChild(chip);
  });
  chatAttachments.style.display = state.pendingAttachments.length ? 'flex' : 'none';
}

async function uploadPendingImage(file) {
  if (!isImageFile(file)) { toast('仅支持图片文件（png/jpg/gif/webp/bmp）', 'error'); return; }
  if (file.size > 10 * 1024 * 1024) { toast('图片不能超过 10MB', 'error'); return; }
  const data = await new Promise((resolve, reject) => {
    const fr = new FileReader();
    fr.onload = () => resolve(String(fr.result).split(',')[1] || '');
    fr.onerror = () => reject(new Error('读取文件失败'));
    fr.readAsDataURL(file);
  });
  try {
    const up = await apiPost('/upload-image', { data, name: file.name || 'image' });
    if (state.pendingAttachments.some(a => a.id === up.id)) return;
    state.pendingAttachments.push({ id: up.id, name: up.name || file.name, url: up.url });
    renderPendingAttachments();
  } catch (e) {
    toast('图片上传失败: ' + e.message, 'error');
  }
}

function removePendingAttachment(id) {
  state.pendingAttachments = state.pendingAttachments.filter(a => a.id !== id);
  renderPendingAttachments();
}

function handleAttachFiles(files) {
  if (!files || !files.length) return;
  Array.from(files).forEach(f => uploadPendingImage(f));
}

function setupChatAttachments() {
  if (!chatAttachBtn || !chatFileInput) return;
  chatAttachBtn.addEventListener('click', () => chatFileInput.click());
  chatFileInput.addEventListener('change', () => {
    handleAttachFiles(chatFileInput.files);
    chatFileInput.value = '';
  });
  const composer = document.querySelector('.chat__composer');
  if (!composer) return;
  composer.addEventListener('dragover', e => {
    e.preventDefault();
    composer.classList.add('is-dragging');
  });
  composer.addEventListener('dragleave', () => composer.classList.remove('is-dragging'));
  composer.addEventListener('drop', e => {
    e.preventDefault();
    composer.classList.remove('is-dragging');
    handleAttachFiles(e.dataTransfer && e.dataTransfer.files);
  });
}
setupChatAttachments();
```

`escAttr` 若不存在需定义（检查；`escHtml` 存在）。`escAttr` 定义：

```js
function escAttr(s) { return String(s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
```

（放在 escHtml 附近；若 escHtml 已存在则在其后加。）

- [ ] **Step 4: 语法检查**

Run（提取内联脚本后 `node --check`）：
```powershell
$html = [System.IO.File]::ReadAllText("internal\serve\orchestrator_frontend\index.html", [System.Text.Encoding]::UTF8)
$matches = [regex]::Matches($html, '<script(?![^>]*src=)[^>]*>(.*?)</script>', 'Singleline')
$i = 0
foreach ($m in $matches) {
  $i++
  $js = $m.Groups[1].Value
  [System.IO.File]::WriteAllText("$env:TEMP\opencode\inline_$i.js", $js, [System.Text.UTF8Encoding]::new($false))
  node --check "$env:TEMP\opencode\inline_$i.js"
  if ($LASTEXITCODE -ne 0) { Write-Host "FAILED inline_$i.js"; exit 1 }
}
Write-Host "all inline scripts OK"
```
Expected: `all inline scripts OK`

- [ ] **Step 5: 提交**

```bash
git add internal/serve/orchestrator_frontend/index.html
git commit -m "feat: 聊天输入框图片附件 UI（拖拽/加号/待发送区）（L-54）"
```

---

### Task 5: 前端发送集成（消息渲染 + 历史回显 + analyze 传图）

**Files:**
- Modify: `internal/serve/orchestrator_frontend/index.html`
- Test: `node --check`

**Interfaces:**
- Consumes: Task 4 的 `state.pendingAttachments`、`renderPendingAttachments`；Task 2 的 `ChatMsg.Images`（conversation API round-trip 自动生效）
- Produces:
  - `addChatMessage(role, text, meta, persist, images)` — images 渲染进气泡
  - `sendChat` 发送时：清空 pendingAttachments 并入用户消息；analyze body 加 `images`（本次 + 历史 requirementMessages 中带图的）

- [ ] **Step 1: 修改 `addChatMessage`**（`index.html:3265`）。签名加 `images` 参数；气泡渲染加图片块；持久化 push 加 images：

```js
function addChatMessage(role, text, meta, persist = true, images) {
  hideChatWelcome();
  const div = document.createElement('div');
  div.className = 'chat-bubble chat-bubble--' + role;
  const imgHtml = (images && images.length)
    ? '<div class="chat-bubble__imgs">' + images.map(im =>
        '<img class="chat-bubble__img" src="' + escAttr(im.url || ('/orchestrator/api/images/' + im.id)) + '" alt="' + escAttr(im.name || '') + '" title="' + escAttr(im.name || '') + '" />'
      ).join('') + '</div>'
    : '';
  div.innerHTML = `
    <div class="chat-bubble__inner">${renderMarkdown(text)}</div>
    ${imgHtml}
    ${meta ? `<div class="chat-bubble__meta">${escHtml(meta)}</div>` : ''}
  `;
  chatMessages.appendChild(div);
  chatMessages.scrollTop = chatMessages.scrollHeight;
  if (!persist) return;
  state.chatMessages.push({ role, text, meta, images: images || [] });
  // Categorize: requirement messages vs pipeline logs
  const isPipelineLog = meta === '流水线' || meta === '工具' || meta === '工具结果' || meta === '通知';
  if (role === 'user' || (!isPipelineLog && meta !== '分析中' && meta !== '错误')) {
    state.requirementMessages.push({ role, text, meta, images: images || [] });
  } else {
    state.pipelineMessages.push({ role, text, meta, images: images || [] });
  }
}
```

CSS（`.chat-bubble` 157 行附近）：

```css
.chat-bubble__imgs{display:flex;flex-wrap:wrap;gap:8px;margin-top:8px}
.chat-bubble__img{max-width:240px;max-height:180px;border-radius:8px;border:1px solid var(--border);cursor:zoom-in}
```

- [ ] **Step 2: 修改 `refreshChatHistory`**（`index.html:3322`）。加载历史时渲染图片（`m.images`）：

```js
    for (const m of msgs) {
      if (!m || !m.role || m.role === 'system' || m.role === 'tool') continue;
      const role = m.role === 'assistant' ? 'ai' : m.role;
      const text = [m.reasoning, m.content].filter(Boolean).join(m.reasoning && m.content ? '\n\n' : '');
      const images = (m.images || []).map(im => ({ id: im.id, name: im.name, url: '/orchestrator/api/images/' + im.id }));
      addChatMessage(role, text || '', m.toolName || m.toolCallId || '', true, images);
    }
```

> 注意：`refreshChatHistory` 走 `/history`（main server），不是 orch-sessions conversation。历史消息结构来自 main server 的 history —— 它不含 images 字段。因此**历史回显的正确数据源是 orch-sessions conversation**。若 `refreshChatHistory` 是唯一历史来源，需要改成从 `GET /orch-sessions/{id}/conversation` 读。检查：`refreshChatHistory` 调用点 —— 它由 history panel 触发。**简化决策**：聊天窗口打开时的历史加载已由 `loadOrchestratorState`（3726+）处理（它会 GET orch-sessions/{id} 并渲染 conversation）。确认 `loadOrchestratorState` 中渲染 conversation 的位置并同样传 images。若它调 `addChatMessage` 时用了 `m.images`，一并处理。执行时 grep `requirementMessages.forEach` 或 `chatMessages.forEach` 在 loadOrchestratorState 中的渲染点，给 addChatMessage 传 images。

- [ ] **Step 3: 修改 `sendChat`**（`index.html:3459`）。发送时处理 pendingAttachments：

```js
  // 图片附件：本次发送的 + 历史 requirementMessages 中带图的（保持上下文）
  const pendingImages = state.pendingAttachments.splice(0);
  renderPendingAttachments();
  const historyImages = [];
  state.requirementMessages.forEach(m => {
    if (m.images && m.images.length) {
      m.images.forEach(im => historyImages.push({ id: im.id, name: im.name || '' }));
    }
  });
  const allImages = pendingImages.concat(historyImages);
  // 用户消息持久化（含本次图片）
  addChatMessage('user', text, '', true, pendingImages);
```

> 注意：原代码 `addChatMessage('user', text)` 在 3475 行。替换为带 images 的调用。且必须在 `state.requirementMessages` push 之前执行（addChatMessage 内部会 push）。

analyze body（3515-3523）加 `images: allImages`：

```js
    const analysis = await apiPost('/requirements/analyze', {
      text,
      history,
      pipelineInfo,
      lang: currentLang || 'zh',
      executor: $('#chat-executor').value || '',
      model: $('#chat-model').value || '',
      agent: $('#chat-agent').value || '',
      images: allImages
    });
```

- [ ] **Step 4: 确认 `loadOrchestratorState` 渲染 conversation 传 images**。执行时 grep `loadOrchestratorState` 中渲染消息的代码，把 `addChatMessage(role, text, meta)` 调用加上 images 参数（从会话数据 `sess.chatMessages / sess.requirementMessages` 取 `m.images`）。若渲染逻辑共用 `refreshChatHistory` 或独立 forEach，均需补 images。

- [ ] **Step 5: 语法检查**

Run（同 Task 4 Step 4 的提取 + node --check 命令）
Expected: `all inline scripts OK`

- [ ] **Step 6: 全量构建 + 手工验证清单 + 提交**

Run: `go build ./...`
Expected: 干净

手工验证（serve 起服务后）：
1. 拖一张 png 到 composer → chip 出现缩略图
2. `+` 按钮 → 文件选择器 → 多选两张 → 两个 chip
3. 发送 → 用户气泡含缩略图；Flash 分析正常返回
4. 刷新页面 → 历史消息图片回显
5. 删除 chip → 发送不含该图
6. 拖入 .txt → toast 拒绝

```bash
git add internal/serve/orchestrator_frontend/index.html
git commit -m "feat: 聊天发送集成图片附件（渲染/回显/分析传图）（L-54）"
```

---

### Task 6: 全量回归 + 调试记录

**Files:**
- Modify: `docs/调试记录.md`

- [ ] **Step 1: 全量测试**

Run: `go test ./internal/serve/ ./internal/orchestrator/ 2>&1 | Select-String -Pattern 'FAIL|ok '`
Expected: 仅剩预存在失败 `TestRuntimeStatePersistsAndReloads`（pipeline_v2_test.go，干净基线也失败，非本次回归）

- [ ] **Step 2: vet + build**

Run: `go vet ./...` 且 `go build ./...`
Expected: 干净

- [ ] **Step 3: 写调试记录** `docs/调试记录.md` 追加 L-54 条目（日期、commit、实现要点、已知边界：图片文件不随会话删除清理；历史图片每次分析都重新转述；视觉模型默认 mimo-v2.5 可经 env 覆盖）

- [ ] **Step 4: 提交**

```bash
git add docs/调试记录.md
git commit -m "docs: L-54 调试记录（图片附件）"
```