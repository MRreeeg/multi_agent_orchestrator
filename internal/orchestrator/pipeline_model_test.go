package orchestrator

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNormalizeExecutionModelRefResolvesBareMimoModel(t *testing.T) {
	workspace := t.TempDir()
	userCfgDir := filepath.Join(workspace, "user-config")
	if err := os.MkdirAll(userCfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APPDATA", userCfgDir)
	t.Setenv("MIMO_API_KEY", "test-key")

	got := normalizeExecutionModelRef(workspace, "mimo-v2.5")
	if got == "mimo-v2.5" || got == "" {
		t.Fatalf("normalizeExecutionModelRef returned %q, want canonical provider/model ref", got)
	}
	if got != "mimo-pro/mimo-v2.5" && got != "mimo-flash/mimo-v2.5" && got != "mimo-api/mimo-v2.5" && got != "mimo-token-plan/mimo-v2.5" {
		t.Fatalf("normalizeExecutionModelRef returned %q, want recognized mimo provider/model ref", got)
	}
}

func TestNormalizeExecutionModelRefPreservesCanonicalRef(t *testing.T) {
	got := normalizeExecutionModelRef(t.TempDir(), "deepseek/deepseek-v4-pro")
	if got != "deepseek/deepseek-v4-pro" {
		t.Fatalf("normalizeExecutionModelRef = %q, want canonical ref unchanged", got)
	}
}

func TestResolveExecutorModelRefResolvesBareMimoModelForMimoExecutor(t *testing.T) {
	workspace := t.TempDir()
	got := resolveExecutorModelRef(workspace, ExecutorMimo, "serve", "mimo-v2.5")
	if got == "" || got == "mimo-v2.5" {
		t.Fatalf("resolveExecutorModelRef mimo bare model = %q, want canonical mimo provider/model ref", got)
	}
	if !strings.HasSuffix(got, "/mimo-v2.5") {
		t.Fatalf("resolveExecutorModelRef mimo bare model = %q, want recognized mimo provider/model ref", got)
	}
}

func TestResolveExecutorModelRefLeavesDeepseekBareModelForMimoExecutor(t *testing.T) {
	got := resolveExecutorModelRef(t.TempDir(), ExecutorMimo, "serve", "deepseek-flash")
	if got != "deepseek-flash" {
		t.Fatalf("resolveExecutorModelRef deepseek bare model = %q, want unchanged", got)
	}
}

func TestResolveExecutorModelRefUsesProviderAliasForReasonixRun(t *testing.T) {
	workspace := t.TempDir()
	userCfgDir := filepath.Join(workspace, "user-config")
	if err := os.MkdirAll(userCfgDir, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("APPDATA", userCfgDir)
	t.Setenv("MIMO_API_KEY", "test-key")

	if got := resolveExecutorModelRef(workspace, ExecutorReasonix, "run", "deepseek-flash"); got != "deepseek-flash" {
		t.Fatalf("reasonix run deepseek ref = %q, want deepseek-flash", got)
	}
	got := resolveExecutorModelRef(workspace, ExecutorReasonix, "run", "mimo-v2.5")
	if got != "xiaomi" && got != "mimo-flash" && got != "mimo-pro" && got != "mimo-api" && got != "mimo-token-plan" {
		t.Fatalf("reasonix run mimo bare model ref = %q, want provider alias", got)
	}
	got = resolveExecutorModelRef(workspace, ExecutorReasonix, "run", "xiaomi/mimo-v2.5")
	if got != "xiaomi" {
		t.Fatalf("reasonix run canonical mimo ref = %q, want provider alias xiaomi", got)
	}
}

func TestMimoModelArgRequiresCanonicalProviderModelRef(t *testing.T) {
	if got := mimoModelArg("mimo-v2.5"); got != "" {
		t.Fatalf("mimoModelArg bare model = %q, want empty so caller does not pass invalid mimo --model", got)
	}
	if got := mimoModelArg("xiaomi/mimo-v2.5"); got != "xiaomi/mimo-v2.5" {
		t.Fatalf("mimoModelArg provider/model = %q, want unchanged", got)
	}
	if got := mimoModelArg("deepseek/deepseek-v4-flash"); got != "deepseek/deepseek-v4-flash" {
		t.Fatalf("mimoModelArg non-mimo = %q, want unchanged", got)
	}
}

func TestParseMimoJSONOutputReturnsErrorEvents(t *testing.T) {
	stdout := "{\"type\":\"error\",\"error\":{\"data\":{\"message\":\"ProviderModelNotFoundError\"}}}\n" +
		"{\"type\":\"error\",\"error\":{\"data\":{\"message\":\"Model not found: mimo-v2.5\"}}}\n"
	text, err := parseMimoJSONOutput(stdout)
	if text != "" {
		t.Fatalf("parseMimoJSONOutput text = %q, want empty", text)
	}
	if err == nil {
		t.Fatal("parseMimoJSONOutput error = nil, want error")
	}
	if got := err.Error(); got != "ProviderModelNotFoundError; Model not found: mimo-v2.5" {
		t.Fatalf("parseMimoJSONOutput error = %q", got)
	}
}

func TestParseMimoOutputReadsJSONErrorFromStderr(t *testing.T) {
	stderr := "{\"type\":\"error\",\"error\":{\"data\":{\"message\":\"ProviderModelNotFoundError\"}}}\n" +
		"{\"type\":\"error\",\"error\":{\"data\":{\"message\":\"Model not found: mimo-flash/mimo-v2.5\"}}}\n"
	text, err := parseMimoOutput("", stderr)
	if text != "" {
		t.Fatalf("parseMimoOutput text = %q, want empty", text)
	}
	if err == nil {
		t.Fatal("parseMimoOutput error = nil, want error")
	}
	if got := err.Error(); got != "ProviderModelNotFoundError; Model not found: mimo-flash/mimo-v2.5" {
		t.Fatalf("parseMimoOutput error = %q", got)
	}
}

func TestParseMimoJSONOutputSupportsOpenCodeTextEvents(t *testing.T) {
	review := `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`
	reviewBytes, err := json.Marshal(review)
	if err != nil {
		t.Fatal(err)
	}
	stream := fmt.Sprintf(`{"type":"message.part.updated","properties":{"part":{"id":"p1","type":"text","text":%s}}}`+"\n"+`{"type":"session.idle","properties":{"sessionID":"s1"}}`, string(reviewBytes))
	got, err := parseMimoJSONOutput(stream)
	if err != nil {
		t.Fatalf("parseMimoJSONOutput() error = %v", err)
	}
	if got != review {
		t.Fatalf("parsed text = %q, want %q", got, review)
	}
	if !validLoopReviewOutput(got) {
		t.Fatalf("parsed text is not a valid loop review: %q", got)
	}
}

func TestParseMimoJSONOutputSupportsMimoV25TextEvents(t *testing.T) {
	review := `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.92,"summary":"第1轮审查通过","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`
	text := "```json\n" + review + "\n```"
	textBytes, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	stream := fmt.Sprintf(`{"type":"step_start","sessionID":"s1"}`+"\n"+
		`{"type":"text","part":{"type":"text","text":%s}}`+"\n"+
		`{"type":"step_finish","reason":"stop"}`, string(textBytes))

	got, err := parseMimoJSONOutput(stream)
	if err != nil {
		t.Fatalf("parseMimoJSONOutput() error = %v", err)
	}
	if got != text {
		t.Fatalf("parsed text = %q, want %q", got, text)
	}
	if !validLoopReviewOutput(got) {
		t.Fatalf("parsed text is not a valid loop review: %q", got)
	}
	decision, err := ParseReviewDecision(got)
	if err != nil {
		t.Fatalf("ParseReviewDecision() error = %v", err)
	}
	if decision.Decision != "pass" {
		t.Fatalf("decision = %q, want pass", decision.Decision)
	}
}

func TestMimoOutputLineTerminalRecognizesMimoV25Events(t *testing.T) {
	review := `{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.92,"summary":"ok","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":[]}`
	text := "```json\n" + review + "\n```"
	textBytes, err := json.Marshal(text)
	if err != nil {
		t.Fatal(err)
	}
	textEvent := fmt.Sprintf(`{"type":"text","part":{"type":"text","text":%s}}`, string(textBytes))
	if !mimoOutputLineTerminal(textEvent) {
		t.Fatal("valid Mimo text review event must be terminal")
	}
	if !mimoOutputLineTerminal(`{"type":"step_finish","reason":"stop"}`) {
		t.Fatal("step_finish event must be terminal")
	}
	if mimoTerminalStreamHasOutput(`{"type":"step_finish","reason":"stop"}`, `{"type":"step_finish","reason":"stop"}`) {
		t.Fatal("step_finish without assistant text must not terminate the client")
	}
	stream := textEvent + "\n" + `{"type":"step_finish","reason":"stop"}`
	if !mimoTerminalStreamHasOutput(`{"type":"step_finish","reason":"stop"}`, stream) {
		t.Fatal("step_finish after Mimo text must terminate the client")
	}
}

func TestMimoOutputLineTerminalRecognizesTerminalEvents(t *testing.T) {
	cases := []string{
		`{"type":"session.idle","properties":{"sessionID":"s1"}}`,
		`{"type":"final","text":"done"}`,
		`{"type":"message.updated","properties":{"info":{"role":"assistant","time":{"created":1,"completed":2}}}}`,
	}
	for _, line := range cases {
		if !mimoOutputLineTerminal(line) {
			t.Errorf("mimoOutputLineTerminal(%s) = false, want true", line)
		}
	}
	if mimoOutputLineTerminal(`{"type":"message.part.delta","properties":{"delta":"partial"}}`) {
		t.Fatal("delta event must not be treated as terminal")
	}
}

func TestMimoFastFailureReasonRecognizesPermissionEvent(t *testing.T) {
	got := mimoFastFailureReason(`{"type":"permission.asked","properties":{"permission":"external_directory"}}`)
	if got == "" {
		t.Fatal("permission event was not classified as a fast failure")
	}
}

func TestWorkspacePathFromTaskUsesExistingAbsoluteDirectory(t *testing.T) {
	root := t.TempDir()
	task := "请在项目 " + root + " 中执行测试。"
	if got := workspacePathFromTask(task); got != filepath.Clean(root) {
		t.Fatalf("workspacePathFromTask() = %q, want %q", got, filepath.Clean(root))
	}
}

func TestMimoTerminalStreamRequiresAssistantOutputForIdle(t *testing.T) {
	idle := `{"type":"session.idle","properties":{"sessionID":"s1"}}`
	if mimoTerminalStreamHasOutput(idle, idle) {
		t.Fatal("session.idle without assistant output must not terminate the client")
	}
	stream := `{"type":"message.part.updated","properties":{"part":{"id":"p1","type":"text","text":"review result"}}}` + "\n" + idle
	if !mimoTerminalStreamHasOutput(idle, stream) {
		t.Fatal("session.idle after assistant text must terminate the client")
	}
}

func TestGatherInputForNextIterationIncludesPreviousReview(t *testing.T) {
	store := NewStore()
	review := `{"schemaVersion":"loop-review-v1","decision":"revise","confidence":0.9,"summary":"需要修复","blockingIssues":[],"requiredChanges":["补充边界测试"],"nextTask":"补充边界测试","evidence":["测试失败"]}`
	store.iterations["iter-1"] = &LoopIteration{
		ID:              "iter-1",
		RunID:           "run-1",
		Number:          1,
		Status:          IterationRevising,
		ReviewAttemptID: "attempt-review-1",
	}
	store.iterations["iter-2"] = &LoopIteration{
		ID:     "iter-2",
		RunID:  "run-1",
		Number: 2,
		Status: IterationRunning,
	}
	store.attempts["attempt-review-1"] = &NodeAttempt{
		ID:          "attempt-review-1",
		IterationID: "iter-1",
		NodeID:      "reviewer",
		Status:      "complete",
		Output:      review,
	}
	run := &PipelineRun{
		ID:             "run-1",
		Task:           "补充边界测试",
		LoopConfig:     LoopConfig{Enabled: true, Mode: "fixed", FixedIterations: 3, ReviewNodeID: "reviewer", Protocol: "loop-review-v1"},
		IterationIDs:   []string{"iter-1", "iter-2"},
		NodeAttemptIDs: []string{"attempt-review-1"},
	}
	pipe := &Pipeline{Nodes: []AgentNode{{ID: "executor", Type: NodeExecutor, Label: "执行者"}}}
	got := store.gatherInputForIteration(pipe, run, "iter-2", "executor")
	if !strings.Contains(got, "## 上一轮审查结论（必须据此处理）") {
		t.Fatalf("next iteration input missing review handoff: %s", got)
	}
	if !strings.Contains(got, review) {
		t.Fatalf("next iteration input missing full review JSON: %s", got)
	}
}
