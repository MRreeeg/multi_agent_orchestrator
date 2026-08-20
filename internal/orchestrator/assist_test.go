package orchestrator

import (
	"strings"
	"testing"
)

func TestAssistHintInjection(t *testing.T) {
	newRun := func() *PipelineRun {
		return &PipelineRun{
			Task: "原始任务",
			NodeStates: map[string]RunState{
				"n1": {Status: "complete", Output: "上游输出内容"},
			},
		}
	}
	assistOn := &AssistConfig{Enabled: true}
	assistOff := &AssistConfig{Enabled: false}
	newPipe := func(assist *AssistConfig) *Pipeline {
		return &Pipeline{
			Nodes: []AgentNode{
				{ID: "n1", Type: NodeArchitect, Label: "架构师", RoleDesc: "design"},
				{ID: "n2", Type: NodeExecutor, Label: "执行者", RoleDesc: "implement", Assist: assist},
			},
			Edges: []Edge{{ID: "e1", FromID: "n1", ToID: "n2"}},
		}
	}

	store := NewStore()
	store.SetOrchestratorAddr("127.0.0.1:8080")

	const protocolMarker = "委派协议"

	// Enabled helper injects the delegation protocol with the dispatch endpoint.
	in := store.gatherInput(newPipe(assistOn), newRun(), "n2")
	if !strings.Contains(in, protocolMarker) {
		t.Errorf("enabled helper should get delegation protocol; input:\n%s", in)
	}
	if !strings.Contains(in, "辅助手") {
		t.Errorf("enabled helper should get helper section title")
	}
	if !strings.Contains(in, "curl") || !strings.Contains(in, "orch-assist/dispatch") {
		t.Errorf("enabled helper hint should carry the curl dispatch command; input:\n%s", in)
	}
	if !strings.Contains(in, "ok=true") || !strings.Contains(in, "禁止编造图像内容") {
		t.Errorf("enabled helper hint should carry the outcome rules; input:\n%s", in)
	}

	// Disabled helper gets no hint.
	in = store.gatherInput(newPipe(assistOff), newRun(), "n2")
	if strings.Contains(in, protocolMarker) {
		t.Errorf("disabled helper must NOT get delegation protocol; input:\n%s", in)
	}

	// Nil (legacy node) gets no hint.
	in = store.gatherInput(newPipe(nil), newRun(), "n2")
	if strings.Contains(in, protocolMarker) {
		t.Errorf("nil assist must NOT get hint; input:\n%s", in)
	}

	// Custom role/model/driver appear in the hint.
	custom := &AssistConfig{Enabled: true, Model: "deepseek-v4-flash", Driver: "claude", Role: "翻译英文文档"}
	in = store.gatherInput(newPipe(custom), newRun(), "n2")
	for _, want := range []string{"claude/deepseek-v4-flash", "翻译英文文档"} {
		if !strings.Contains(in, want) {
			t.Errorf("custom assist hint missing %q; input:\n%s", want, in)
		}
	}

	// No-upstream + task path also injects the hint.
	noEdge := &Pipeline{Nodes: newPipe(assistOn).Nodes}
	in = store.gatherInput(noEdge, newRun(), "n2")
	if !strings.Contains(in, protocolMarker) {
		t.Errorf("no-upstream node should get delegation protocol; input:\n%s", in)
	}
}

func TestAssistHintNoAddrDegrades(t *testing.T) {
	// Without a configured orchestrator address the hint must degrade to an
	// honest "cannot delegate" statement, never a dead curl command.
	store := NewStore()
	pipe := &Pipeline{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeExecutor, Label: "执行者", RoleDesc: "implement", Assist: &AssistConfig{Enabled: true}},
		},
	}
	run := &PipelineRun{Task: "任务", NodeStates: map[string]RunState{}}
	in := store.gatherInput(pipe, run, "n1")
	if !strings.Contains(in, "辅助手端点未配置") {
		t.Errorf("hint without orchestrator addr should degrade honestly; input:\n%s", in)
	}
	if strings.Contains(in, "curl ") {
		t.Errorf("hint without orchestrator addr must not emit a curl command; input:\n%s", in)
	}
}

func TestLoopIterationAssistHintInjection(t *testing.T) {
	store := NewStore()
	store.SetOrchestratorAddr("127.0.0.1:8080")
	pipe := &Pipeline{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeArchitect, Label: "架构师", RoleDesc: "design", Assist: &AssistConfig{Enabled: false}},
			{ID: "n2", Type: NodeExecutor, Label: "执行者", RoleDesc: "implement", Assist: &AssistConfig{Enabled: true}},
		},
		Edges: []Edge{{ID: "e1", FromID: "n1", ToID: "n2"}},
	}
	run := &PipelineRun{
		Task:           "任务",
		IterationIDs:   []string{"it1"},
		NodeAttemptIDs: []string{"a1"},
		NodeStates:     map[string]RunState{},
	}
	store.iterations = map[string]*LoopIteration{
		"it1": {ID: "it1", Number: 1},
	}
	store.attempts = map[string]*NodeAttempt{
		"a1": {ID: "a1", IterationID: "it1", NodeID: "n1", Status: "complete", Output: "out"},
	}

	// n1 has disabled helper: iteration input must not contain the hint.
	in := store.gatherInputForIteration(pipe, run, "it1", "n1")
	if strings.Contains(in, "委派协议") {
		t.Errorf("disabled helper must NOT get hint in loop iteration; input:\n%s", in)
	}

	// n2 has enabled helper: iteration input should contain the hint.
	in = store.gatherInputForIteration(pipe, run, "it1", "n2")
	if !strings.Contains(in, "委派协议") {
		t.Errorf("enabled helper should get assist hint in loop iteration; input:\n%s", in)
	}
}

func TestAssistDispatchValidation(t *testing.T) {
	store := NewStore()

	// Empty task and no images.
	if _, err := store.AssistDispatch(t.Context(), AssistDispatchOptions{}); err == nil {
		t.Fatal("empty task + no images must fail before touching any runtime")
	}

	// Image path that does not exist must fail before starting a runtime.
	if _, err := store.AssistDispatch(t.Context(), AssistDispatchOptions{
		Task:   "描述这张图",
		Images: []string{"/definitely/not/here/图.png"},
	}); err == nil || !strings.Contains(err.Error(), "not readable") {
		t.Fatalf("missing image should fail with readable-path error, got: %v", err)
	}
}

func TestAssistModelRef(t *testing.T) {
	cases := []struct{ driver, model, want string }{
		{"", "", "opencode/mimo-v2.5"},
		{"opencode", "", "opencode/mimo-v2.5"},
		{"opencode", "glm-4v", "opencode/glm-4v"},
		{"claude", "", "claude/claude-sonnet-4-6"},
		{"Claude", "claude-opus-4", "claude/claude-opus-4"},
	}
	for _, c := range cases {
		if got := assistModelRef(c.driver, c.model); got != c.want {
			t.Errorf("assistModelRef(%q,%q) = %q, want %q", c.driver, c.model, got, c.want)
		}
	}
}

func TestAssistPromptText(t *testing.T) {
	text := assistPromptText("描述差异", []string{"C:/a.png", "C:/b.png"})
	for _, want := range []string{"C:/a.png", "C:/b.png", "描述差异", "read 工具"} {
		if !strings.Contains(text, want) {
			t.Errorf("assist prompt missing %q:\n%s", want, text)
		}
	}
}