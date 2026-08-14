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

	const hintMarker = "reasonix assist"

	store := NewStore()

	// Enabled helper injects the hint.
	in := store.gatherInput(newPipe(assistOn), newRun(), "n2")
	if !strings.Contains(in, hintMarker) {
		t.Errorf("enabled helper should get assist hint; input:\n%s", in)
	}
	if !strings.Contains(in, "辅助手") {
		t.Errorf("enabled helper should get assist section title")
	}

	// Disabled helper gets no hint.
	in = store.gatherInput(newPipe(assistOff), newRun(), "n2")
	if strings.Contains(in, hintMarker) {
		t.Errorf("disabled helper must NOT get assist hint; input:\n%s", in)
	}

	// Nil (legacy node) gets no hint.
	in = store.gatherInput(newPipe(nil), newRun(), "n2")
	if strings.Contains(in, hintMarker) {
		t.Errorf("nil assist must NOT get hint; input:\n%s", in)
	}

	// Custom role/model/driver appear in the hint.
	custom := &AssistConfig{Enabled: true, Model: "deepseek-v4-flash", Driver: "claude", Role: "翻译英文文档"}
	in = store.gatherInput(newPipe(custom), newRun(), "n2")
	for _, want := range []string{"--model deepseek-v4-flash", "--driver claude", "翻译英文文档"} {
		if !strings.Contains(in, want) {
			t.Errorf("custom assist hint missing %q; input:\n%s", want, in)
		}
	}

	// No-upstream + task path also injects the hint.
	noEdge := &Pipeline{Nodes: newPipe(assistOn).Nodes}
	in = store.gatherInput(noEdge, newRun(), "n2")
	if !strings.Contains(in, hintMarker) {
		t.Errorf("no-upstream node should get assist hint; input:\n%s", in)
	}
}

func TestLoopIterationAssistHintInjection(t *testing.T) {
	store := NewStore()
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
	if strings.Contains(in, "reasonix assist") {
		t.Errorf("disabled helper must NOT get hint in loop iteration; input:\n%s", in)
	}

	// n2 has enabled helper: iteration input should contain the hint.
	in = store.gatherInputForIteration(pipe, run, "it1", "n2")
	if !strings.Contains(in, "reasonix assist") {
		t.Errorf("enabled helper should get assist hint in loop iteration; input:\n%s", in)
	}
}
