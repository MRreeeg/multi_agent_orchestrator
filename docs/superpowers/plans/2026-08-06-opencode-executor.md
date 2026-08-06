# opencode 执行器 + DeepSeek 官方 API 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在编排器中新增 `opencode` 执行器（run + serve），并把 DeepSeek 官方 API（deepseek-v4-flash，key 取自 D:\code\appii\deepseek.txt）接入 opencode 与 reasonix 两侧。

**Architecture:** 镜像现有 claude/mimo 执行器模式：新增 `internal/executor/opencode` 包（run 一次性 + serve HTTP 客户端），新增 `internal/orchestrator/opencode_runtime.go`（保留运行时管理），在 pipeline 注册表、校验、模型解析和 serve 前端注册，最后配置 DeepSeek 凭据并分步提交 GitHub。

**Tech Stack:** Go（net/http、os/exec、encoding/json）、opencode CLI 1.18（`run --format json`、`serve` HTTP API）、reasonix config。

---

## 文件结构

| 文件 | 职责 |
|---|---|
| `internal/executor/opencode/opencode.go`（新建） | run 模式：命令构造 + JSON 事件流解析 |
| `internal/executor/opencode/opencode_test.go`（新建） | run 解析测试 |
| `internal/executor/opencode/client.go`（新建） | serve 模式：opencode server HTTP API 客户端 |
| `internal/executor/opencode/client_test.go`（新建） | HTTP 客户端测试（httptest） |
| `internal/orchestrator/opencode_runtime.go`（新建） | 保留运行时管理（spawn serve、状态机、console、interrupt/stop） |
| `internal/orchestrator/types.go`（修改） | `ExecutorOpencode` 常量 |
| `internal/orchestrator/pipeline.go`（修改） | 注册 OpenCodePipelineExecutor、校验、模型透传、runtime 访问模式 |
| `internal/serve/orchestrator.go`（修改） | nodeTypes 加 opencode 及模型；runtime 路由分派 |
| `reasonix.toml`（修改，gitignored） | DeepSeek 官方 provider |
| `.env`（修改，gitignored） | `DEEPSEEK_API_KEY` = appii key |
| `docs/调试记录.md`（新建） | 每步改动与验证记录 |

---

## Task 1: opencode run 执行器（解析 + 命令）

**Files:**
- Create: `internal/executor/opencode/opencode.go`
- Test: `internal/executor/opencode/opencode_test.go`

- [ ] **Step 1: 写失败测试**

`internal/executor/opencode/opencode_test.go`:

```go
package opencode

import (
	"strings"
	"testing"
)

const sampleStream = `{"type":"step_start","timestamp":1785997454089,"sessionID":"ses_abc","part":{"type":"step-start"}}
{"type":"text","timestamp":1785997459262,"sessionID":"ses_abc","part":{"type":"text","text":"OK"}}
{"type":"step_finish","timestamp":1785997459473,"sessionID":"ses_abc","part":{"type":"step-finish","tokens":{"total":16012,"input":14217,"output":3,"cost":0}}}`

func TestParseRunOutput(t *testing.T) {
	res := ParseRunOutput([]byte(sampleStream))
	if res.Output != "OK" {
		t.Fatalf("output = %q, want %q", res.Output, "OK")
	}
	if res.SessionID != "ses_abc" {
		t.Fatalf("sessionID = %q, want ses_abc", res.SessionID)
	}
	if res.TotalTokens != 16012 {
		t.Fatalf("tokens = %d, want 16012", res.TotalTokens)
	}
	if res.Cost != 0 {
		t.Fatalf("cost = %v, want 0", res.Cost)
	}
}

func TestDiscoverBin(t *testing.T) {
	// 不应 panic；在装有 opencode 的机器上返回非空路径。
	_ = DiscoverBin()
}
```

- [ ] **Step 2: 运行测试确认失败**

```powershell
go test ./internal/executor/opencode -run TestParseRunOutput -count=1
```
Expected: FAIL（`undefined: ParseRunOutput`）

- [ ] **Step 3: 实现 `internal/executor/opencode/opencode.go`**

```go
// Package opencode implements the opencode CLI executor for the orchestrator.
//
// run mode executes `opencode run -m <provider/model> --format json` as a
// one-shot process and parses the JSON event stream. Retained orchestration
// uses `opencode serve` over loopback HTTP (see client.go) and is owned by
// OpenCodeRuntimeManager in the orchestrator package.
package opencode

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"reasonix/internal/proc"
)

// ExecutorResult is the outcome of one `opencode run` invocation.
type ExecutorResult struct {
	Output      string  `json:"output"`
	RawStdout   string  `json:"rawStdout"`
	Stderr      string  `json:"stderr"`
	ExitCode    int     `json:"exitCode"`
	SessionID   string  `json:"sessionID,omitempty"`
	TotalTokens int64   `json:"totalTokens,omitempty"`
	Cost        float64 `json:"cost,omitempty"`
}

// ExecOptions configures an opencode run execution.
type ExecOptions struct {
	Model           string // provider/model, e.g. opencode/deepseek-v4-flash-free
	Workspace       string
	ResumeSessionID string
	MaxSteps        int
}

// Executor executes tasks via the opencode CLI (`opencode run`).
type Executor struct {
	OpencodeBin string // optional override
}

func (e *Executor) opencodeBin() string {
	if e.OpencodeBin != "" {
		return e.OpencodeBin
	}
	if bin := DiscoverBin(); bin != "" {
		return bin
	}
	return "opencode"
}

// DiscoverBin returns the native opencode binary path found on this machine,
// or "" when nothing is installed. npm shims (.ps1/.cmd) are skipped because
// they cannot be spawned directly by os/exec on Windows.
func DiscoverBin() string {
	candidates := []string{
		filepath.Join(os.Getenv("APPDATA"), "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
		filepath.Join(os.Getenv("USERPROFILE"), "AppData", "Roaming", "npm", "node_modules", "opencode-ai", "bin", "opencode.exe"),
	}
	for _, candidate := range candidates {
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate
		}
	}
	if path, err := exec.LookPath("opencode"); err == nil {
		return path
	}
	return ""
}

// Execute runs one `opencode run` call and parses the JSON event stream.
func (e *Executor) Execute(ctx context.Context, opts ExecOptions, prompt string) (*ExecutorResult, error) {
	args := []string{"run"}
	if opts.ResumeSessionID != "" {
		args = append(args, "--session", opts.ResumeSessionID)
	}
	if opts.Model != "" {
		args = append(args, "-m", opts.Model)
	}
	args = append(args, "--format", "json", prompt)

	bin := e.opencodeBin()
	cmd := exec.CommandContext(ctx, bin, args...)
	// One-shot opencode run must not flash a console window on Windows.
	proc.HideWindow(cmd)
	if opts.Workspace != "" {
		cmd.Dir = opts.Workspace
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("opencode run: %w", err)
		}
	}
	res := ParseRunOutput(stdout.Bytes())
	res.Stderr = strings.TrimSpace(stderr.String())
	res.ExitCode = exitCode
	res.RawStdout = stdout.String()
	if exitCode != 0 && strings.TrimSpace(res.Output) == "" {
		return res, fmt.Errorf("opencode run failed (exit %d): %s", exitCode, res.Stderr)
	}
	return res, nil
}

type runTokens struct {
	Total int64   `json:"total"`
	Input int64   `json:"input"`
	Output int64  `json:"output"`
	Cost  float64 `json:"cost"`
}

type runPart struct {
	Type   string     `json:"type"`
	Text   string     `json:"text"`
	Tokens *runTokens `json:"tokens,omitempty"`
}

type runEvent struct {
	Type      string  `json:"type"`
	SessionID string  `json:"sessionID,omitempty"`
	Error     string  `json:"error,omitempty"`
	Part      runPart `json:"part,omitempty"`
}

// ParseRunOutput parses the newline-delimited JSON event stream emitted by
// `opencode run --format json`. Assistant text parts are concatenated; the
// step_finish event carries token usage.
func ParseRunOutput(data []byte) *ExecutorResult {
	res := &ExecutorResult{}
	var text []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var ev runEvent
		if err := json.Unmarshal([]byte(line), &ev); err != nil {
			continue
		}
		if ev.SessionID != "" && res.SessionID == "" {
			res.SessionID = ev.SessionID
		}
		switch ev.Type {
		case "text":
			if ev.Part.Text != "" {
				text = append(text, ev.Part.Text)
			}
		case "step_finish":
			if ev.Part.Tokens != nil {
				res.TotalTokens = ev.Part.Tokens.Total
				res.Cost = ev.Part.Tokens.Cost
			}
		case "error":
			if ev.Error != "" {
				res.Output = ev.Error
			} else if ev.Part.Text != "" {
				res.Output = ev.Part.Text
			}
		}
	}
	res.Output = strings.TrimSpace(strings.Join(text, ""))
	return res
}
```

- [ ] **Step 4: 运行测试确认通过**

```powershell
go test ./internal/executor/opencode -count=1
```
Expected: PASS（2 个测试）

- [ ] **Step 5: 冒烟验证真实 opencode 免费模型**

```powershell
go run ./cmd/reasonix version
```

再直接调用一次（验证 DiscoverBin 找到原生 exe 且免费模型可用）：

```powershell
$oc = "$env:APPDATA\npm\node_modules\opencode-ai\bin\opencode.exe"
& $oc run -m opencode/deepseek-v4-flash-free --format json "Reply with exactly one word: OK" 2>$null | Select-Object -Last 3
```
Expected: 输出含 `"type":"text"` 且文本为 OK；退出码 0。

- [ ] **Step 6: 提交**

```powershell
git add internal/executor/opencode/opencode.go internal/executor/opencode/opencode_test.go
git commit -m "feat: add opencode run executor (one-shot)"
```

---

## Task 2: opencode serve HTTP 客户端

**Files:**
- Create: `internal/executor/opencode/client.go`
- Test: `internal/executor/opencode/client_test.go`

- [ ] **Step 1: 写失败测试**

`internal/executor/opencode/client_test.go`:

```go
package opencode

import (
	"context"
	"encoding/json"
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

func TestClientJSON(t *testing.T) {
	_ = json.Marshal
}
```

- [ ] **Step 2: 运行测试确认失败**

```powershell
go test ./internal/executor/opencode -run TestClient -count=1
```
Expected: FAIL（`undefined: NewClient`）

- [ ] **Step 3: 实现 `internal/executor/opencode/client.go`**

```go
package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Client drives a retained `opencode serve` process over its loopback HTTP
// API (documented at https://opencode.ai/docs/server/).
type Client struct {
	BaseURL string
	HTTP    *http.Client
}

// NewClient creates a client for one opencode serve endpoint.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		HTTP:    &http.Client{Timeout: 10 * time.Minute},
	}
}

func (c *Client) post(ctx context.Context, path string, body []byte) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return c.HTTP.Do(req)
}

func (c *Client) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+path, nil)
	if err != nil {
		return nil, err
	}
	return c.HTTP.Do(req)
}

func status(resp *http.Response) string {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// NewSession creates a session and returns its ID.
func (c *Client) NewSession(ctx context.Context, title string) (string, error) {
	payload, _ := json.Marshal(map[string]string{"title": title})
	resp, err := c.post(ctx, "/session", payload)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("opencode create session: %s", status(resp))
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	if out.ID == "" {
		return "", fmt.Errorf("opencode create session: empty id")
	}
	return out.ID, nil
}

// Prompt sends one message and waits for the complete assistant response.
func (c *Client) Prompt(ctx context.Context, sessionID, model, prompt string) (string, error) {
	payload := map[string]any{
		"parts": []map[string]string{{"type": "text", "text": prompt}},
	}
	if model != "" {
		payload["model"] = model
	}
	body, _ := json.Marshal(payload)
	resp, err := c.post(ctx, "/session/"+sessionID+"/message", body)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("opencode prompt: %s", status(resp))
	}
	var out struct {
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	var text []string
	for _, p := range out.Parts {
		if p.Type == "text" && p.Text != "" {
			text = append(text, p.Text)
		}
	}
	return strings.TrimSpace(strings.Join(text, "")), nil
}

// Abort cancels a running turn; the session stays usable.
func (c *Client) Abort(ctx context.Context, sessionID string) error {
	resp, err := c.post(ctx, "/session/"+sessionID+"/abort", []byte(`{}`))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("opencode abort: %s", status(resp))
	}
	return nil
}

// HistoryMessage is one message from GET /session/{id}/message.
type HistoryMessage struct {
	ID   string `json:"id"`
	Role string `json:"role"`
	Text string `json:"text"`
}

// History lists the most recent messages of a session for the Runtime Console.
func (c *Client) History(ctx context.Context, sessionID string) ([]HistoryMessage, error) {
	resp, err := c.get(ctx, "/session/"+sessionID+"/message?limit=100")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opencode history: %s", status(resp))
	}
	var raw []struct {
		Info struct {
			ID   string `json:"id"`
			Role string `json:"role"`
		} `json:"info"`
		Parts []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"parts"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, err
	}
	out := make([]HistoryMessage, 0, len(raw))
	for _, m := range raw {
		var text strings.Builder
		for _, p := range m.Parts {
			if p.Type == "text" {
				text.WriteString(p.Text)
			}
		}
		out = append(out, HistoryMessage{ID: m.Info.ID, Role: m.Info.Role, Text: text.String()})
	}
	return out, nil
}
```

- [ ] **Step 4: 运行测试确认通过**

```powershell
go test ./internal/executor/opencode -count=1
```
Expected: PASS（5 个测试）

- [ ] **Step 5: 提交**

```powershell
git add internal/executor/opencode/client.go internal/executor/opencode/client_test.go
git commit -m "feat: add opencode serve HTTP client"
```

---

## Task 3: opencode 保留运行时管理

**Files:**
- Create: `internal/orchestrator/opencode_runtime.go`

- [ ] **Step 1: 实现完整运行时管理（镜像 mimo_runtime.go，走 HTTP）**

```go
package orchestrator

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os/exec"
	"strings"
	"sync"
	"time"

	opencodeclient "reasonix/internal/executor/opencode"
)

// opencodeRuntime is a retained `opencode serve` runtime driven over its
// loopback HTTP API. One runtime hosts one retained opencode session.
type opencodeRuntime struct {
	ID           string
	Port         int
	Endpoint     string
	NodeID       string
	ModelRef     string
	Workspace    string
	ApprovalMode string
	Cmd          *exec.Cmd
	Stderr       *bytes.Buffer
	done         chan struct{}
	StartedAt    time.Time
	LastUsedAt   time.Time

	mu        sync.Mutex
	client    *opencodeclient.Client
	sessionID string
	turnID    string
	status    RuntimeStatus
	output    string
	lastErr   string
	events    []RuntimeConsoleEvent
	stream    *consoleStreamCoalescer
}

// OpenCodeRuntimeManager starts `opencode serve` as a retained, loopback-only
// runtime and drives it over HTTP.
type OpenCodeRuntimeManager struct {
	mu       sync.Mutex
	runtimes map[string]*opencodeRuntime
	onUpdate func(RuntimeState)
}

func newOpenCodeRuntimeManager() *OpenCodeRuntimeManager {
	return &OpenCodeRuntimeManager{runtimes: make(map[string]*opencodeRuntime)}
}

func (m *OpenCodeRuntimeManager) SetUpdateSink(sink func(RuntimeState)) {
	m.mu.Lock()
	m.onUpdate = sink
	m.mu.Unlock()
}

func (m *OpenCodeRuntimeManager) notify(rt *opencodeRuntime) {
	m.mu.Lock()
	sink := m.onUpdate
	m.mu.Unlock()
	if sink == nil {
		return
	}
	sink(*m.stateFor(rt, ExecSpec{}, CleanupRetained))
}

func (m *OpenCodeRuntimeManager) runtimeKey(spec ExecSpec) string {
	workspace := spec.Workspace
	if workspace == "" {
		workspace = "."
	}
	return spec.NodeID + "|" + spec.ModelRef + "|" + workspace
}

func (m *OpenCodeRuntimeManager) Borrow(ctx context.Context, spec ExecSpec, policy CleanupPolicy) (*RuntimeState, error) {
	rt, err := m.ensure(ctx, spec, nil)
	if err != nil {
		return nil, err
	}
	return m.stateFor(rt, spec, policy), nil
}

func (m *OpenCodeRuntimeManager) Release(runtimeID string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	if target.status != RuntimeBusy {
		target.status = RuntimeIdle
	}
	target.LastUsedAt = time.Now()
	target.mu.Unlock()
	m.notify(target)
	return nil
}

func (m *OpenCodeRuntimeManager) Stop(runtimeID string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	var key string
	for k, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target, key = rt, k
			break
		}
	}
	if target != nil {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	target.status = RuntimeStopped
	client := target.client
	target.client = nil
	stream := target.stream
	target.stream = nil
	target.mu.Unlock()
	if stream != nil {
		stream.stop()
	}
	if client != nil {
		// best-effort abort of an active turn
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = client.Abort(ctx, target.sessionID)
		cancel()
	}
	if target.Cmd != nil {
		stopRuntimeProcess(target.Cmd, target.done)
	}
	m.notify(target)
	return nil
}

func (m *OpenCodeRuntimeManager) Get(runtimeID string) (*RuntimeState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			return m.stateFor(rt, ExecSpec{NodeID: ""}, CleanupRetained), true
		}
	}
	return nil, false
}

func (m *OpenCodeRuntimeManager) List() []*RuntimeState {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*RuntimeState, 0, len(m.runtimes))
	for _, rt := range m.runtimes {
		out = append(out, m.stateFor(rt, ExecSpec{}, CleanupRetained))
	}
	return out
}

func (m *OpenCodeRuntimeManager) cleanupAll() {
	m.mu.Lock()
	runtimes := make([]*opencodeRuntime, 0, len(m.runtimes))
	for key, rt := range m.runtimes {
		delete(m.runtimes, key)
		runtimes = append(runtimes, rt)
	}
	m.mu.Unlock()
	for _, rt := range runtimes {
		rt.mu.Lock()
		client := rt.client
		rt.client = nil
		stream := rt.stream
		rt.stream = nil
		rt.mu.Unlock()
		if stream != nil {
			stream.stop()
		}
		if client != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = client.Abort(ctx, rt.sessionID)
			cancel()
		}
		stopRuntimeProcess(rt.Cmd, rt.done)
	}
}

func (m *OpenCodeRuntimeManager) stateFor(rt *opencodeRuntime, spec ExecSpec, policy CleanupPolicy) *RuntimeState {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	nodeID := rt.NodeID
	if nodeID == "" {
		nodeID = spec.NodeID
	}
	model := rt.ModelRef
	if model == "" {
		model = spec.ModelRef
	}
	pid := 0
	if rt.Cmd != nil && rt.Cmd.Process != nil {
		pid = rt.Cmd.Process.Pid
	}
	return &RuntimeState{
		RuntimeID:     rt.ID,
		NodeID:        nodeID,
		Executor:      "opencode",
		Model:         model,
		Endpoint:      rt.Endpoint,
		Port:          rt.Port,
		PID:           pid,
		Status:        rt.status,
		Error:         rt.lastErr,
		CreatedAt:     rt.StartedAt,
		LastActiveAt:  rt.LastUsedAt,
		CleanupPolicy: policy,
		AccessMode:    "runtime_console",
		ApprovalMode:  rt.ApprovalMode,
		ThreadID:      rt.sessionID,
		TurnID:        rt.turnID,
	}
}

func (m *OpenCodeRuntimeManager) ensure(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*opencodeRuntime, error) {
	key := m.runtimeKey(spec)
	m.mu.Lock()
	if rt := m.runtimes[key]; rt != nil {
		rt.mu.Lock()
		live := rt.client != nil && rt.status != RuntimeStopped && rt.status != RuntimeError
		rt.LastUsedAt = time.Now()
		rt.mu.Unlock()
		if live {
			if spec.NodeID != "" {
				rt.NodeID = spec.NodeID
			}
			m.mu.Unlock()
			return rt, nil
		}
		delete(m.runtimes, key)
	}
	port := findFreePort()
	rt := &opencodeRuntime{
		ID:           fmt.Sprintf("opencode_rt_%d", time.Now().UnixNano()),
		Port:         port,
		Endpoint:     fmt.Sprintf("http://127.0.0.1:%d", port),
		NodeID:       spec.NodeID,
		ModelRef:     spec.ModelRef,
		Workspace:    spec.Workspace,
		ApprovalMode: spec.ApprovalMode,
		StartedAt:    time.Now(),
		LastUsedAt:   time.Now(),
		done:         make(chan struct{}),
		status:       RuntimeStarting,
	}
	m.runtimes[key] = rt
	m.mu.Unlock()

	rt.stream = newConsoleStreamCoalescer(0, func(evt RuntimeConsoleEvent) {
		rt.mu.Lock()
		rt.events = append(rt.events, evt)
		if len(rt.events) > 300 {
			rt.events = append([]RuntimeConsoleEvent(nil), rt.events[len(rt.events)-300:]...)
		}
		rt.mu.Unlock()
		m.notify(rt)
	})

	args := []string{"serve", "--port", fmt.Sprint(port), "--hostname", "127.0.0.1"}
	cmd := newRetainedRuntimeCommand(ctx, "opencode", args...)
	if spec.Workspace != "" {
		cmd.Dir = spec.Workspace
	}
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		m.dropRuntime(key, rt)
		return nil, fmt.Errorf("start opencode serve: %w", err)
	}
	rt.Cmd = cmd
	rt.Stderr = stderr
	go m.watchRuntime(key, rt)
	if onStart != nil {
		onStart(rt.Endpoint, rt.Port)
	}

	client := opencodeclient.NewClient(rt.Endpoint)
	rt.mu.Lock()
	rt.client = client
	rt.mu.Unlock()

	// Wait for the server health endpoint.
	deadline := time.Now().Add(60 * time.Second)
	for {
		healthy := false
		checkCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		func() {
			defer cancel()
			req, _ := http.NewRequestWithContext(checkCtx, http.MethodGet, rt.Endpoint+"/global/health", nil)
			resp, err := client.HTTP.Do(req)
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					healthy = true
				}
			}
		}()
		if healthy {
			break
		}
		if time.Now().After(deadline) {
			diagnostic := strings.TrimSpace(stderr.String())
			m.Stop(rt.ID)
			return nil, fmt.Errorf("opencode serve did not become healthy within 60s: %s", diagnostic)
		}
		select {
		case <-ctx.Done():
			m.Stop(rt.ID)
			return nil, ctx.Err()
		case <-time.After(300 * time.Millisecond):
		}
	}

	rt.mu.Lock()
	rt.status = RuntimeIdle
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.notify(rt)
	return rt, nil
}

func (m *OpenCodeRuntimeManager) dropRuntime(key string, rt *opencodeRuntime) {
	m.mu.Lock()
	if current := m.runtimes[key]; current == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
}

func (m *OpenCodeRuntimeManager) watchRuntime(key string, rt *opencodeRuntime) {
	err := rt.Cmd.Wait()
	rt.mu.Lock()
	if rt.status != RuntimeStopped {
		rt.status = RuntimeError
		if err != nil {
			rt.lastErr = err.Error()
		} else {
			rt.lastErr = "opencode serve exited"
		}
	}
	rt.mu.Unlock()
	m.notify(rt)
	m.mu.Lock()
	if m.runtimes[key] == rt {
		delete(m.runtimes, key)
	}
	m.mu.Unlock()
	close(rt.done)
}

// Execute runs one orchestration node turn against the retained opencode
// session over HTTP.
func (m *OpenCodeRuntimeManager) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	rt, err := m.ensure(ctx, spec, onStart)
	if err != nil {
		return &ExecResult{ExitCode: -1}, err
	}
	client, err := m.reserveTurn(rt)
	if err != nil {
		return &ExecResult{RuntimeID: rt.ID, Endpoint: rt.Endpoint, ExitCode: -1}, err
	}

	sessionID, err := m.prepareSession(ctx, rt, client, spec)
	if err != nil {
		m.finishTurn(rt, sessionID, err)
		return m.execResult(rt, "", sessionID), err
	}
	text, promptErr := client.Prompt(ctx, sessionID, spec.ModelRef, spec.Prompt)
	rt.mu.Lock()
	if text != "" {
		rt.output = text
	}
	rt.mu.Unlock()
	if promptErr != nil {
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	if strings.TrimSpace(text) == "" {
		promptErr = fmt.Errorf("opencode turn completed without assistant output")
		m.finishTurn(rt, sessionID, promptErr)
		return m.execResult(rt, "", sessionID), promptErr
	}
	m.finishTurn(rt, sessionID, nil)
	return m.execResult(rt, text, sessionID), nil
}

func (m *OpenCodeRuntimeManager) prepareSession(ctx context.Context, rt *opencodeRuntime, client *opencodeclient.Client, spec ExecSpec) (string, error) {
	if spec.ContextPolicy == "fresh" || spec.ContextPolicy == "fresh_per_run" {
		return m.createSession(ctx, rt, client, spec)
	}
	sessionID := strings.TrimSpace(spec.ExternalSessionID)
	if sessionID == "" {
		rt.mu.Lock()
		sessionID = strings.TrimSpace(rt.sessionID)
		rt.mu.Unlock()
	}
	if sessionID == "" {
		return m.createSession(ctx, rt, client, spec)
	}
	return sessionID, nil
}

func (m *OpenCodeRuntimeManager) createSession(ctx context.Context, rt *opencodeRuntime, client *opencodeclient.Client, spec ExecSpec) (string, error) {
	title := strings.TrimSpace(spec.NodeLabel)
	if title == "" {
		title = "opencode-node"
	}
	sessionID, err := client.NewSession(ctx, title)
	if err != nil {
		return "", err
	}
	rt.mu.Lock()
	rt.sessionID = sessionID
	rt.mu.Unlock()
	rt.stream.append("session/new", sessionID, "system", "opencode session "+sessionID)
	return sessionID, nil
}

func (m *OpenCodeRuntimeManager) reserveTurn(rt *opencodeRuntime) (*opencodeclient.Client, error) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.client == nil {
		return nil, fmt.Errorf("opencode runtime lost its connection")
	}
	if rt.status == RuntimeBusy {
		return nil, fmt.Errorf("opencode runtime already has an active turn")
	}
	rt.status = RuntimeBusy
	rt.turnID = fmt.Sprintf("turn_%d", time.Now().UnixNano())
	return rt.client, nil
}

func (m *OpenCodeRuntimeManager) finishTurn(rt *opencodeRuntime, sessionID string, turnErr error) {
	rt.mu.Lock()
	rt.sessionID = sessionID
	rt.turnID = ""
	if turnErr == nil {
		rt.status = RuntimeIdle
		rt.lastErr = ""
	} else {
		rt.status = RuntimeIdle
		rt.lastErr = turnErr.Error()
	}
	rt.LastUsedAt = time.Now()
	rt.mu.Unlock()
	m.notify(rt)
}

func (m *OpenCodeRuntimeManager) execResult(rt *opencodeRuntime, text, sessionID string) *ExecResult {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return &ExecResult{
		RuntimeID:    rt.ID,
		Endpoint:     rt.Endpoint,
		FinalText:    text,
		Output:       text,
		ExternalSessionID: sessionID,
	}
}

// Interrupt cancels the active turn but keeps the runtime and session alive.
func (m *OpenCodeRuntimeManager) Interrupt(runtimeID string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	client := target.client
	sessionID := target.sessionID
	target.mu.Unlock()
	if client == nil || sessionID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return client.Abort(ctx, sessionID)
}

// Snapshot returns the Runtime Console view for one runtime.
func (m *OpenCodeRuntimeManager) Snapshot(runtimeID string) (*RuntimeConsoleSnapshot, bool) {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return nil, false
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	events := append([]RuntimeConsoleEvent(nil), target.events...)
	return &RuntimeConsoleSnapshot{
		Runtime:   m.stateFor(target, ExecSpec{NodeID: ""}, CleanupRetained),
		Output:    target.output,
		Error:     target.lastErr,
		Events:    events,
		CanSend:   target.status == RuntimeIdle && target.client != nil,
		CanInterrupt: target.status == RuntimeBusy,
		ThreadID:  target.sessionID,
		TurnID:    target.turnID,
	}, true
}

// SendMessage sends a manual turn from the Runtime Console (debug only; does
// not advance the Loop).
func (m *OpenCodeRuntimeManager) SendMessage(runtimeID, text string) error {
	m.mu.Lock()
	var target *opencodeRuntime
	for _, rt := range m.runtimes {
		if rt.ID == runtimeID {
			target = rt
			break
		}
	}
	m.mu.Unlock()
	if target == nil {
		return fmt.Errorf("opencode runtime %q not found", runtimeID)
	}
	target.mu.Lock()
	client := target.client
	sessionID := target.sessionID
	model := target.ModelRef
	target.mu.Unlock()
	if client == nil || sessionID == "" {
		return fmt.Errorf("opencode runtime has no session")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	out, err := client.Prompt(ctx, sessionID, model, text)
	if err != nil {
		return err
	}
	target.mu.Lock()
	target.output = out
	target.mu.Unlock()
	m.notify(target)
	return nil
}

var opencodeRuntimeMgr = newOpenCodeRuntimeManager()
```

> 集成注意点：`opencode serve` 的权限策略在真实运行中验证——若 `POST /session/{id}/message` 因等待权限确认而挂起，需在 `prepareSession` 后读取会话权限并逐条 `POST /session/{id}/permissions/{pid}` 应答 deny（以实测响应为准，见 Task 5 验证）。

- [ ] **Step 2: 编译验证**

```powershell
go build ./internal/orchestrator
```
Expected: 成功（若提示未使用变量，清理 `var (...)` 中多余占位）。

- [ ] **Step 3: 提交**

```powershell
git add internal/orchestrator/opencode_runtime.go
git commit -m "feat: add opencode retained runtime manager (serve)"
```

---

## Task 4: pipeline 注册（常量 / 执行器 / 校验 / 模型解析）

**Files:**
- Modify: `internal/orchestrator/types.go`
- Modify: `internal/orchestrator/pipeline.go`

- [ ] **Step 1: types.go 加常量**

`internal/orchestrator/types.go` 的 `const (...)` 块：

```go
	ExecutorReasonix ExecutorType = "reasonix"
	ExecutorMimo     ExecutorType = "mimo"
	ExecutorCodex    ExecutorType = "codex"
	ExecutorClaude   ExecutorType = "claude"
	ExecutorOpencode ExecutorType = "opencode"
```

- [ ] **Step 2: pipeline.go 注册执行器**

`executors` map 加一行：

```go
		ExecutorClaude:   &ClaudePipelineExecutor{},
		ExecutorOpencode: &OpenCodePipelineExecutor{},
```

并在文件末尾附近新增：

```go
// OpenCodePipelineExecutor executes a node via the opencode CLI.
type OpenCodePipelineExecutor struct{}

func (e *OpenCodePipelineExecutor) Name() string { return "opencode" }

func (e *OpenCodePipelineExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if strings.EqualFold(strings.TrimSpace(spec.Mode), "run") {
		return executeOpencodeRun(ctx, spec)
	}
	return opencodeRuntimeMgr.Execute(ctx, spec, onStart)
}

func executeOpencodeRun(ctx context.Context, spec ExecSpec) (*ExecResult, error) {
	executor := &opencodeExecutor.Executor{}
	opts := opencodeExecutor.ExecOptions{
		Model:           spec.ModelRef,
		Workspace:       spec.Workspace,
		ResumeSessionID: "",
	}
	if spec.ContextPolicy != "fresh" && spec.ContextPolicy != "fresh_per_run" {
		opts.ResumeSessionID = strings.TrimSpace(spec.ExternalSessionID)
	}
	start := time.Now()
	res, err := executor.Execute(ctx, opts, buildExecutorPrompt(spec))
	duration := time.Now().Sub(start).Milliseconds()
	if err != nil {
		return &ExecResult{ExitCode: res.ExitCode, RuntimeID: "", Endpoint: ""}, err
	}
	result := &ExecResult{
		ExitCode:          res.ExitCode,
		FinalText:         res.Output,
		Output:            res.Output,
		RawStdout:         res.RawStdout,
		Stderr:            res.Stderr,
		ExternalSessionID: res.SessionID,
	}
	if res.TotalTokens > 0 {
		result.TokenUsage = &TokenUsage{TotalTokens: int(res.TotalTokens)}
	}
	result.DurationMs = duration
	return result, nil
}
```

并在 pipeline.go import 中加入：

```go
	opencodeExecutor "reasonix/internal/executor/opencode"
```

- [ ] **Step 3: 校验与模型解析**

`validateNodeExecutionConfigAtWorkspaceWithRoute` 的 switch 中加：

```go
	case ExecutorOpencode:
		if mode != "run" && mode != "serve" {
			return fmt.Errorf("unsupported opencode mode %q; supported modes are run and serve", mode)
		}
		if strings.TrimSpace(model) == "" {
			return fmt.Errorf("model is required for opencode executor")
		}
		if !strings.Contains(model, "/") {
			return fmt.Errorf("opencode executor requires provider/model ref (e.g. opencode/deepseek-v4-flash-free)")
		}
```

`resolveExecutorModelRef` 的 switch 中加：

```go
	case ExecutorOpencode:
		// opencode owns provider resolution; pass provider/model refs verbatim.
		return model
```

- [ ] **Step 4: 编译 + 测试**

```powershell
go build ./...
go test ./internal/orchestrator -run TestValidateNode -count=1
```
Expected: 编译成功；测试通过。

- [ ] **Step 5: 提交**

```powershell
git add internal/orchestrator/types.go internal/orchestrator/pipeline.go
git commit -m "feat: register opencode executor in pipeline"
```

---

## Task 5: 前端 nodeTypes 与 runtime 路由

**Files:**
- Modify: `internal/serve/orchestrator.go`

- [ ] **Step 1: nodeTypes 加 opencode**

三处 `ModelsByExecutor` 各加一行，并加入 `Executors` 列表：

```go
				orchestrator.ExecutorOpencode: {"opencode/deepseek-v4-flash-free", "deepseek/deepseek-v4-flash", "deepseek/deepseek-v4-pro"},
```

`Executors: []orchestrator.ExecutorType{...}` 三处均追加 `orchestrator.ExecutorOpencode`。

- [ ] **Step 2: runtime 路由分派**

`listRuntimes` / `stopRuntime` / `runtimeConsole` / `runtimeMessage` / `runtimeInterrupt` 各追加 opencode 分派：

```go
	all = append(all, orchestrator.ListOpencodeRuntimes()...)
```

```go
	if err := orchestrator.StopOpencodeRuntime(id); err == nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
```

在 orchestrator 包导出函数（pipeline.go 或 opencode_runtime.go 末尾）：

```go
func ListOpencodeRuntimes() []*RuntimeState              { return opencodeRuntimeMgr.List() }
func GetOpencodeRuntime(id string) (*RuntimeState, bool) { return opencodeRuntimeMgr.Get(id) }
func StopOpencodeRuntime(id string) error                { return opencodeRuntimeMgr.Stop(id) }
func InterruptOpencodeRuntime(id string) error           { return opencodeRuntimeMgr.Interrupt(id) }
func SendOpencodeRuntimeMessage(id, text string) error   { return opencodeRuntimeMgr.SendMessage(id, text) }
func SnapshotOpencodeRuntime(id string) (*RuntimeConsoleSnapshot, bool) {
	return opencodeRuntimeMgr.Snapshot(id)
}
```

- [ ] **Step 3: 编译 + JS 校验 + 重新构建桌面版**

```powershell
go build ./...
go vet ./internal/serve ./internal/orchestrator
```

```powershell
go build -ldflags "-H=windowsgui" -o bin\orchestrator-app.exe ./cmd/orchestrator-app
go build -o bin\reasonix.exe ./cmd/reasonix
```
Expected: 全部成功。

- [ ] **Step 4: 提交**

```powershell
git add internal/serve/orchestrator.go internal/orchestrator/pipeline.go internal/orchestrator/opencode_runtime.go
git commit -m "feat: wire opencode into orchestrator UI and runtime routes"
```

---

## Task 6: DeepSeek 官方 API 配置

**Files:**
- Modify: `.env`（gitignored）
- Modify: `reasonix.toml`（gitignored）

- [ ] **Step 1: 写入 appii key 到 .env**

`.env`：

```text
DEEPSEEK_API_KEY=<D:\code\appii\deepseek.txt 的内容>
MIMO_API_KEY=
```

（key 值读取自 `D:\code\appii\deepseek.txt`，不写入 git。）

- [ ] **Step 2: reasonix.toml 加官方 provider**

在 `[[providers]]` 区追加：

```toml
[[providers]]
name           = "deepseek-official"
kind           = "openai"
base_url       = "https://api.deepseek.com"
model          = "deepseek-v4-flash"
api_key_env    = "DEEPSEEK_API_KEY"
context_window = 1000000
```

- [ ] **Step 3: 验证 opencode 可用官方模型**

```powershell
$env:DEEPSEEK_API_KEY = (Get-Content D:\code\appii\deepseek.txt -Raw).Trim()
$oc = "$env:APPDATA\npm\node_modules\opencode-ai\bin\opencode.exe"
& $oc run -m deepseek/deepseek-v4-flash --format json "Reply with exactly one word: OK" 2>$null | Select-Object -Last 3
```
Expected: 输出含 text OK。

- [ ] **Step 4: 验证 reasonix 官方 provider**

```powershell
$env:DEEPSEEK_API_KEY = (Get-Content D:\code\appii\deepseek.txt -Raw).Trim()
.\bin\reasonix.exe run --model deepseek-official "Reply with exactly one word: OK"
```
Expected: 输出 OK。

- [ ] **Step 5: 提交（仅提交 reasonix.toml 的 provider 声明之外的文档；key 不入库）**

```powershell
git add docs/调试记录.md
git commit -m "chore: document deepseek official api setup"
```

---

## Task 7: 端到端验证 + 调试记录 + GitHub 推送

- [ ] **Step 1: 重启桌面版并跑 opencode 流水线**

```powershell
Get-Process orchestrator-app -ErrorAction SilentlyContinue | Stop-Process -Force
Start-Process -FilePath "D:\code\multi_agent_orchestrator\bin\orchestrator-app.exe" -WorkingDirectory "D:\code\multi_agent_orchestrator"
```

在控制台建 3 节点流水线（架构师/执行者/审查者，执行器均选 opencode，
模型选 `opencode/deepseek-v4-flash-free`），任务写一个小文件，确认：
- 全程无黑框；
- 文件成功写入 workspace；
- Runtime Console（serve 节点）可打开并显示事件。

- [ ] **Step 2: 写调试记录**

`docs/调试记录.md`：

```markdown
# 调试记录

## 2026-08-06 opencode 执行器接入

- 新增 `internal/executor/opencode`（run 一次性 + serve HTTP 客户端）。
- 新增 `internal/orchestrator/opencode_runtime.go`（保留运行时）。
- pipeline 注册、校验、模型透传、前端 nodeTypes 与 runtime 路由。
- DeepSeek 官方 API：key 取自 D:\code\appii\deepseek.txt，配置于 .env（不入库）与 opencode/reasonix。
- 验证：免费模型 opencode/deepseek-v4-flash-free 冒烟 OK；官方 deepseek/deepseek-v4-flash OK；
  opencode 流水线端到端完成；无黑框。
```

- [ ] **Step 3: 提交并推送**

```powershell
git add docs/调试记录.md
git commit -m "docs: add debug log for opencode integration"
git push origin master
```

Expected: push 成功；若凭据缺失，报告并保留本地提交。

- [ ] **Step 4: 最终验证**

```powershell
go build ./...
go vet ./internal/orchestrator ./internal/serve ./internal/executor/opencode
go test ./internal/executor/opencode -count=1
```
Expected: 全部通过。

---

## 自检结果

- Spec 覆盖：opencode run/serve、DeepSeek 官方 API、前端注册、git/调试记录均有对应任务；
- 无占位符：所有代码块完整；
- 类型一致：`ExecutorOpencode`、`OpenCodePipelineExecutor`、`opencodeRuntimeMgr`、
  `ListOpencodeRuntimes` 等命名在任务间一致。
