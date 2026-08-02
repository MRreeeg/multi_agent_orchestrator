package orchestrator

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"reasonix/internal/agent"
	"reasonix/internal/event"
	"reasonix/internal/provider"
)

type stubExecutor struct {
	name    string
	result  *ExecResult
	err     error
	onStart func(string, int) // custom onStart callback for testing
}

func (s stubExecutor) Name() string { return s.name }

func (s stubExecutor) Execute(_ context.Context, _ ExecSpec, onStart func(string, int)) (*ExecResult, error) {
	if onStart != nil && s.result != nil && s.result.Endpoint != "" {
		onStart(s.result.Endpoint, portFromEndpoint(s.result.Endpoint))
	}
	if s.onStart != nil && s.result != nil && s.result.Endpoint != "" {
		s.onStart(s.result.Endpoint, portFromEndpoint(s.result.Endpoint))
	}
	return s.result, s.err
}

func TestExecuteNodeFailsWhenExecutorReturnsNoOutput(t *testing.T) {
	old := executors[ExecutorMimo]
	executors[ExecutorMimo] = stubExecutor{
		name: "mimo",
		result: &ExecResult{
			FinalText: "",
			RawStdout: "",
			RawStderr: "",
		},
	}
	defer func() { executors[ExecutorMimo] = old }()

	store := NewStore()
	node := &AgentNode{
		ID:       "n1",
		Type:     NodeExecutor,
		Label:    "执行者",
		Model:    "mimo-v2.5",
		Executor: ExecutorMimo,
	}

	output, stderr, usage, _, _, _, err := store.executeNode(context.Background(), node, "test task", "", "")
	if err == nil {
		t.Fatal("executeNode error = nil, want failure for empty output")
	}
	if !strings.Contains(err.Error(), "without any output") {
		t.Fatalf("executeNode error = %q, want empty-output marker", err.Error())
	}
	if output != "" {
		t.Fatalf("output = %q, want empty", output)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
	if usage != nil {
		t.Fatalf("usage = %#v, want nil", usage)
	}
}

func TestExecuteNodeResolvesModelPerExecutor(t *testing.T) {
	old := executors[ExecutorMimo]
	var gotSpec ExecSpec
	executors[ExecutorMimo] = stubExecutor{
		name:   "mimo",
		result: &ExecResult{FinalText: "ok"},
	}
	defer func() { executors[ExecutorMimo] = old }()

	executors[ExecutorMimo] = stubExecutorFunc(func(_ context.Context, spec ExecSpec) (*ExecResult, error) {
		gotSpec = spec
		return &ExecResult{FinalText: "ok"}, nil
	})

	store := NewStore()
	node := &AgentNode{
		ID:       "n1",
		Type:     NodeExecutor,
		Label:    "执行者",
		Model:    "mimo-v2.5",
		Executor: ExecutorMimo,
		Mode:     "serve",
	}

	workspace := t.TempDir()
	t.Setenv("APPDATA", workspace)
	t.Setenv("MIMO_API_KEY", "test-key")

	oldDetectWorkspace := detectWorkspaceForTest
	detectWorkspaceForTest = func() string { return workspace }
	defer func() { detectWorkspaceForTest = oldDetectWorkspace }()

	output, _, _, _, _, _, err := store.executeNode(context.Background(), node, "test task", "", "")
	if err != nil {
		t.Fatalf("executeNode error = %v", err)
	}
	if output != "ok" {
		t.Fatalf("output = %q, want ok", output)
	}
	if gotSpec.ModelRef == "" || gotSpec.ModelRef == "mimo-v2.5" {
		t.Fatalf("spec.ModelRef = %q, want resolved canonical mimo ref", gotSpec.ModelRef)
	}
}

func TestValidateNodeExecutionConfigRejectsMimoBareModelForMimoExecutorWhenUnresolvable(t *testing.T) {
	workspace := t.TempDir()
	t.Setenv("APPDATA", workspace)
	t.Setenv("MIMO_API_KEY", "")
	if err := validateNodeExecutionConfig(ExecutorMimo, "serve", "definitely-not-installed-mimo", ""); err == nil {
		t.Fatal("validateNodeExecutionConfig() error = nil, want failure for unresolved mimo model")
	}
}

func TestPresetsUseExplicitCompatibleExecutors(t *testing.T) {
	presets := Presets()
	if len(presets) == 0 {
		t.Fatal("Presets() returned no presets")
	}
	for _, preset := range presets {
		for _, node := range preset.Pipeline.Nodes {
			if node.Executor == "" {
				t.Fatalf("preset %q node %q missing explicit executor", preset.ID, node.ID)
			}
			if strings.HasPrefix(node.Model, "mimo-v2.5") && node.Executor != ExecutorMimo {
				t.Fatalf("preset %q node %q model %q executor = %q, want mimo", preset.ID, node.ID, node.Model, node.Executor)
			}
			if strings.HasPrefix(node.Model, "deepseek") && node.Executor != ExecutorReasonix {
				t.Fatalf("preset %q node %q model %q executor = %q, want reasonix", preset.ID, node.ID, node.Model, node.Executor)
			}
		}
	}
}

func TestExtractAssistantFinalTextPrefersAssistantContent(t *testing.T) {
	history := []provider.Message{
		{Role: provider.RoleUser, Content: "task"},
		{Role: provider.RoleAssistant, ReasoningContent: "hidden thinking"},
		{Role: provider.RoleAssistant, Content: "final answer"},
	}
	if got := extractAssistantFinalText(history); got != "final answer" {
		t.Fatalf("extractAssistantFinalText() = %q, want final answer", got)
	}
}

func TestExtractAssistantFinalTextFallsBackToReasoning(t *testing.T) {
	history := []provider.Message{
		{Role: provider.RoleUser, Content: "task"},
		{Role: provider.RoleAssistant, ReasoningContent: "final from reasoning"},
	}
	if got := extractAssistantFinalText(history); got != "final from reasoning" {
		t.Fatalf("extractAssistantFinalText() = %q, want reasoning fallback", got)
	}
}

func TestLoadReasonixRunFinalTextFromSession(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "run.jsonl")
	sess := &agent.Session{Messages: []provider.Message{{Role: provider.RoleAssistant, Content: "done"}}}
	if err := sess.Save(path); err != nil {
		t.Fatalf("Save() error = %v", err)
	}
	if got := loadReasonixRunFinalText(path); got != "done" {
		t.Fatalf("loadReasonixRunFinalText() = %q, want done", got)
	}
}

func TestNewReasonixRunSessionPathCreatesOrchestratorArea(t *testing.T) {
	workspace := t.TempDir()
	path, cleanup, err := newReasonixRunSessionPath(ExecSpec{Workspace: workspace, NodeID: "n1"})
	if err != nil {
		t.Fatalf("newReasonixRunSessionPath() error = %v", err)
	}
	defer cleanup()
	if !strings.Contains(path, filepath.Join(".reasonix-orchestrator", "run-sessions")) {
		t.Fatalf("path = %q, want orchestrator run-sessions dir", path)
	}
	if _, err := os.Stat(filepath.Dir(path)); err != nil {
		t.Fatalf("session dir stat error = %v", err)
	}
}

func TestExtractReasonixVisibleTextSkipsDiagnostics(t *testing.T) {
	stderr := "warning: bash not found on PATH\n" +
		"\x1b[2m  ▎ thinking\x1b[0m\n" +
		"[planner]\n" +
		"最终答案第一行\n" +
		"最终答案第二行\n" +
		"  · 123 tok · in 100 (20 cached / 80 new) · out 23\n"
	got := extractReasonixVisibleText(stderr)
	if got != "最终答案第一行\n最终答案第二行" {
		t.Fatalf("extractReasonixVisibleText() = %q", got)
	}
}

func TestSubmitTaskWaitsForTurnDoneBeforeReadingHistory(t *testing.T) {
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submit":
			w.WriteHeader(http.StatusAccepted)
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"idle","lastUsage":{"promptTokens":1,"completionTokens":1,"totalTokens":2}}`))
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			f, _ := w.(http.Flusher)
			_, _ = fmt.Fprint(w, ": connected\n\n")
			f.Flush()
			_, _ = fmt.Fprint(w, "data: {\"kind\":\"turn_done\"}\n\n")
			f.Flush()
			close(done)
		case "/history":
			<-done
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"role":"assistant","content":"OK"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var port int
	if _, err := fmt.Sscanf(strings.TrimPrefix(srv.URL, "http://127.0.0.1:"), "%d", &port); err != nil {
		t.Fatalf("scan port: %v", err)
	}

	got, usage, err := submitTask(context.Background(), port, "hi")
	if err != nil {
		t.Fatalf("submitTask() error = %v", err)
	}
	if got != "OK" {
		t.Fatalf("submitTask() output = %q, want OK", got)
	}
	if usage == nil || usage.TotalTokens != 2 {
		t.Fatalf("submitTask() usage = %#v, want total=2", usage)
	}
}

func TestReasonixRunReturnsVisibleTextEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end executor test in short mode")
	}
	spec := ExecSpec{
		Workspace: detectWorkspace(),
		Prompt:    "只回复OK",
		ModelRef:  "deepseek-flash",
		Mode:      "run",
		Trust:     true,
		NeverAsk:  true,
		NodeID:    "e2e-run",
		NodeLabel: "e2e-run",
	}
	res, err := executeReasonixRun(context.Background(), spec)
	if err != nil {
		t.Fatalf("executeReasonixRun() error = %v\nstderr:\n%s\nstdout:\n%s", err, res.RawStderr, res.RawStdout)
	}
	if strings.TrimSpace(res.FinalText) == "" {
		t.Fatalf("executeReasonixRun() final text empty\nstderr:\n%s\nstdout:\n%s", res.RawStderr, res.RawStdout)
	}
	if !strings.Contains(res.FinalText, "OK") {
		t.Fatalf("executeReasonixRun() final text = %q, want it to contain OK", res.FinalText)
	}
}

func TestSubmitTaskReturnsHistoryOutputWhenEventsTimeout(t *testing.T) {
	done := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/submit":
			w.WriteHeader(http.StatusNoContent)
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"idle","lastUsage":{"promptTokens":10,"completionTokens":5,"totalTokens":15}}`))
		case "/history":
			close(done)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"role":"assistant","content":"LATE OK"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	var port int
	if _, err := fmt.Sscanf(strings.TrimPrefix(srv.URL, "http://127.0.0.1:"), "%d", &port); err != nil {
		t.Fatalf("scan port: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()

	got, usage, err := submitTask(ctx, port, "hi")
	if err != nil {
		t.Fatalf("submitTask() error = %v", err)
	}
	if got != "LATE OK" {
		t.Fatalf("submitTask() output = %q, want LATE OK", got)
	}
	if usage != nil && usage.TotalTokens != 15 {
		t.Fatalf("submitTask() usage = %#v, want total=15 when present", usage)
	}
	<-done
}

func TestReasonixRunReturnsVisibleTextForMimoEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end executor test in short mode")
	}
	// Force-skip unless RUN_INTEGRATION=1 is set (avoids t.Setenv leakage from prior tests)
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("skipping: set RUN_INTEGRATION=1 to run MIMO integration tests")
	}
	spec := ExecSpec{
		Workspace: detectWorkspace(),
		Prompt:    "只回复OK",
		ModelRef:  resolveExecutorModelRef(detectWorkspace(), ExecutorReasonix, "run", "mimo-v2.5"),
		Mode:      "run",
		Trust:     true,
		NeverAsk:  true,
		NodeID:    "e2e-run-mimo",
		NodeLabel: "e2e-run-mimo",
	}
	res, err := executeReasonixRun(context.Background(), spec)
	if err != nil {
		t.Fatalf("executeReasonixRun() error = %v\nstderr:\n%s\nstdout:\n%s", err, res.RawStderr, res.RawStdout)
	}
	if strings.TrimSpace(res.FinalText) == "" {
		t.Fatalf("executeReasonixRun() final text empty\nstderr:\n%s\nstdout:\n%s", res.RawStderr, res.RawStdout)
	}
	if !strings.Contains(res.FinalText, "OK") {
		t.Fatalf("executeReasonixRun() final text = %q, want it to contain OK", res.FinalText)
	}
}

func TestMimoRunReturnsVisibleTextEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end executor test in short mode")
	}
	modelRef := resolveExecutorModelRef(detectWorkspace(), ExecutorMimo, "run", "mimo-v2.5")
	if modelRef == "" || modelRef == "mimo-v2.5" {
		t.Fatalf("resolved mimo model ref = %q, want installed provider/model ref", modelRef)
	}
	spec := ExecSpec{
		Workspace: detectWorkspace(),
		Prompt:    "只回复OK",
		ModelRef:  modelRef,
		Mode:      "run",
		NodeID:    "e2e-mimo-run",
		NodeLabel: "e2e-mimo-run",
	}
	res, err := executeMimoRun(context.Background(), spec)
	if err != nil {
		t.Fatalf("executeMimoRun() error = %v\nstderr:\n%s\nstdout:\n%s", err, res.RawStderr, res.RawStdout)
	}
	if strings.TrimSpace(res.FinalText) == "" {
		t.Fatalf("executeMimoRun() final text empty\nstderr:\n%s\nstdout:\n%s", res.RawStderr, res.RawStdout)
	}
	if !strings.Contains(res.FinalText, "OK") {
		t.Fatalf("executeMimoRun() final text = %q, want it to contain OK", res.FinalText)
	}
}

func TestShouldUseMimoPromptAttachment(t *testing.T) {
	if got := shouldUseMimoPromptAttachment("只回复OK"); got {
		t.Fatalf("short single-line prompt should not use attachment")
	}
	if got := shouldUseMimoPromptAttachment("第一行\n第二行"); !got {
		t.Fatalf("multi-line prompt should use attachment")
	}
	if got := shouldUseMimoPromptAttachment(strings.Repeat("a", 300)); !got {
		t.Fatalf("long prompt should use attachment")
	}
}

func TestBuildMimoRunArgsUsesPromptAttachmentForLongPrompt(t *testing.T) {
	workspace := t.TempDir()
	prompt := "第一轮任务：\n" + strings.Repeat("请检查当前工作区中的 Go 代码和测试结果。\n", 80)
	spec := ExecSpec{
		Workspace: workspace,
		Prompt:    prompt,
		ModelRef:  "xiaomi/mimo-v2.5",
		Mode:      "run",
		NodeID:    "executor-long-prompt",
	}

	args, cleanup, err := buildMimoRunArgs(spec, "")
	if err != nil {
		t.Fatalf("buildMimoRunArgs() error = %v", err)
	}
	defer cleanup()

	fileIndex := -1
	for i, arg := range args {
		if arg == "--file" {
			fileIndex = i + 1
			break
		}
	}
	if fileIndex < 0 || fileIndex >= len(args) {
		t.Fatalf("args = %#v, want --file prompt attachment", args)
	}
	attached, err := os.ReadFile(args[fileIndex])
	if err != nil {
		t.Fatalf("read attached prompt: %v", err)
	}
	if string(attached) != prompt {
		t.Fatalf("attached prompt differs from original: got %d bytes, want %d", len(attached), len(prompt))
	}
	messageIndex := -1
	for i, arg := range args {
		if arg == "请优先阅读附件中的完整上下文，然后在当前工作目录中执行任务，并直接返回最终结果。" {
			messageIndex = i
			break
		}
	}
	if messageIndex < 0 || messageIndex >= fileIndex {
		t.Fatalf("args = %#v, want short positional message before --file", args)
	}
	if fileIndex+1 != len(args) {
		t.Fatalf("args = %#v, want --file to be the final option/value pair", args)
	}
	for _, arg := range args {
		if strings.Contains(arg, prompt) {
			t.Fatalf("full prompt was still placed in command args")
		}
	}
}

func TestBuildMimoRunArgsKeepsShortPromptInline(t *testing.T) {
	spec := ExecSpec{Prompt: "只回复OK", Mode: "run", NodeID: "executor-short-prompt"}
	args, cleanup, err := buildMimoRunArgs(spec, "")
	if err != nil {
		t.Fatalf("buildMimoRunArgs() error = %v", err)
	}
	defer cleanup()
	for _, arg := range args {
		if arg == "--file" {
			t.Fatalf("short prompt unexpectedly used attachment: %#v", args)
		}
	}
	messageIndex := -1
	for i, arg := range args {
		if arg == "只回复OK" {
			messageIndex = i
			break
		}
	}
	if messageIndex != 1 {
		t.Fatalf("args = %#v, want inline prompt immediately after run", args)
	}
}

func TestBuildMimoRunArgsForServeAttachUsesAttachment(t *testing.T) {
	spec := ExecSpec{
		Workspace: t.TempDir(),
		Prompt:    strings.Repeat("审查当前轮次结果。\n", 100),
		Mode:      "serve",
		NodeID:    "reviewer-long-prompt",
	}
	args, cleanup, err := buildMimoRunArgs(spec, "http://127.0.0.1:4096")
	if err != nil {
		t.Fatalf("buildMimoRunArgs() error = %v", err)
	}
	defer cleanup()
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--attach http://127.0.0.1:4096") {
		t.Fatalf("args = %#v, want attach endpoint", args)
	}
	if !strings.Contains(joined, "--file") {
		t.Fatalf("args = %#v, want prompt attachment", args)
	}
}

func TestFormatMimoCommandDiagnosticTruncatesPromptLikeArguments(t *testing.T) {
	got := formatMimoCommandDiagnostic([]string{"run", strings.Repeat("x", 5000)}, "stderr")
	if len(got) > 2200 {
		t.Fatalf("diagnostic length = %d, want bounded output", len(got))
	}
	if !strings.Contains(got, "command truncated") {
		t.Fatalf("diagnostic = %q, want truncation marker", got)
	}
}

func TestMimoServeReturnsVisibleTextEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end executor test in short mode")
	}
	modelRef := resolveExecutorModelRef(detectWorkspace(), ExecutorMimo, "serve", "mimo-v2.5")
	if modelRef == "" || modelRef == "mimo-v2.5" {
		t.Fatalf("resolved mimo model ref = %q, want installed provider/model ref", modelRef)
	}
	exec := &MimoExecutor{}
	spec := ExecSpec{
		Workspace: detectWorkspace(),
		Prompt:    "只回复OK",
		ModelRef:  modelRef,
		Mode:      "serve",
		NodeID:    "e2e-mimo-serve",
		NodeLabel: "e2e-mimo-serve",
	}
	res, err := exec.Execute(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("MimoExecutor.Execute() error = %v\nstderr:\n%s\nstdout:\n%s", err, res.RawStderr, res.RawStdout)
	}
	if strings.TrimSpace(res.FinalText) == "" {
		t.Fatalf("MimoExecutor.Execute() final text empty\nstderr:\n%s\nstdout:\n%s", res.RawStderr, res.RawStdout)
	}
	if !strings.Contains(res.FinalText, "OK") {
		t.Fatalf("MimoExecutor.Execute() final text = %q, want it to contain OK", res.FinalText)
	}
}

func TestReasonixServeReturnsVisibleTextForDeepseekEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end executor test in short mode")
	}
	exec := &ReasonixExecutor{}
	spec := ExecSpec{
		Workspace: detectWorkspace(),
		Prompt:    "只回复OK",
		ModelRef:  resolveExecutorModelRef(detectWorkspace(), ExecutorReasonix, "serve", "deepseek-flash"),
		Mode:      "serve",
		NodeID:    "e2e-rx-serve-deepseek",
		NodeLabel: "e2e-rx-serve-deepseek",
	}
	res, err := exec.Execute(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("ReasonixExecutor.Execute() error = %v\nstderr:\n%s\nstdout:\n%s", err, res.RawStderr, res.RawStdout)
	}
	if strings.TrimSpace(res.FinalText) == "" {
		t.Fatalf("ReasonixExecutor.Execute() final text empty\nstderr:\n%s\nstdout:\n%s", res.RawStderr, res.RawStdout)
	}
	if !strings.Contains(res.FinalText, "OK") {
		t.Fatalf("ReasonixExecutor.Execute() final text = %q, want it to contain OK", res.FinalText)
	}
}

func TestReasonixServeReturnsVisibleTextForMimoEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping end-to-end executor test in short mode")
	}
	// Force-skip unless RUN_INTEGRATION=1 is set (avoids t.Setenv leakage from prior tests)
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("skipping: set RUN_INTEGRATION=1 to run MIMO integration tests")
	}
	exec := &ReasonixExecutor{}
	spec := ExecSpec{
		Workspace: detectWorkspace(),
		Prompt:    "只回复OK",
		ModelRef:  resolveExecutorModelRef(detectWorkspace(), ExecutorReasonix, "serve", "mimo-v2.5"),
		Mode:      "serve",
		NodeID:    "e2e-rx-serve-mimo",
		NodeLabel: "e2e-rx-serve-mimo",
	}
	res, err := exec.Execute(context.Background(), spec, nil)
	if err != nil {
		t.Fatalf("ReasonixExecutor.Execute() error = %v\nstderr:\n%s\nstdout:\n%s", err, res.RawStderr, res.RawStdout)
	}
	if strings.TrimSpace(res.FinalText) == "" {
		t.Fatalf("ReasonixExecutor.Execute() final text empty\nstderr:\n%s\nstdout:\n%s", res.RawStderr, res.RawStdout)
	}
	if !strings.Contains(res.FinalText, "OK") {
		t.Fatalf("ReasonixExecutor.Execute() final text = %q, want it to contain OK", res.FinalText)
	}
}

type stubExecutorFunc func(context.Context, ExecSpec) (*ExecResult, error)

func (f stubExecutorFunc) Name() string { return "stub" }

func (f stubExecutorFunc) Execute(ctx context.Context, spec ExecSpec, _ func(string, int)) (*ExecResult, error) {
	return f(ctx, spec)
}

func TestBuildExecutorPromptInjectsSkillContent(t *testing.T) {
	spec := ExecSpec{Prompt: "检查当前任务", Skill: "review-agent", SkillContent: "只读审查，不修改文件"}
	got := buildExecutorPrompt(spec)
	for _, want := range []string{"review-agent", "只读审查，不修改文件", "检查当前任务"} {
		if !strings.Contains(got, want) {
			t.Fatalf("buildExecutorPrompt() = %q, missing %q", got, want)
		}
	}
}

func TestBuildExecutorPromptDoesNotDuplicateWithoutSkill(t *testing.T) {
	spec := ExecSpec{Prompt: "只回复OK"}
	if got := buildExecutorPrompt(spec); got != spec.Prompt {
		t.Fatalf("buildExecutorPrompt() = %q, want unchanged prompt", got)
	}
}

func TestEmitRuntimeEventIncludesOutputSummary(t *testing.T) {
	s := NewStore()
	var gotDetail string
	s.emitter = event.FuncSink(func(ev event.Event) {
		if ev.Kind == event.PipelineNodeRuntime {
			gotDetail = ev.Detail
		}
	})
	s.emitRuntimeEvent("n1", "ws://127.0.0.1:1", 123, "idle", "codex", "runtime_console", "第一行输出\n第二行"+strings.Repeat("x", 300))
	if !strings.Contains(gotDetail, `"output":"`) {
		t.Fatalf("detail missing output: %s", gotDetail)
	}
	var parsed struct {
		Output string `json:"output"`
	}
	if err := json.Unmarshal([]byte(gotDetail), &parsed); err != nil {
		t.Fatalf("detail not valid JSON: %v -> %s", err, gotDetail)
	}
	if !strings.Contains(parsed.Output, "第一行输出") || len([]rune(parsed.Output)) > 205 {
		t.Fatalf("output summary = %q (runes=%d), want truncated first line", parsed.Output, len([]rune(parsed.Output)))
	}
	// Empty output must not add the field.
	gotDetail = ""
	s.emitRuntimeEvent("n2", "", 0, "starting", "claude", "runtime_console", "")
	if strings.Contains(gotDetail, `"output"`) {
		t.Fatalf("empty output should be omitted: %s", gotDetail)
	}
}
