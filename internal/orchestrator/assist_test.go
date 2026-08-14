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
	newPipe := func(assist string) *Pipeline {
		return &Pipeline{
			Nodes: []AgentNode{
				{ID: "n1", Type: NodeArchitect, Label: "架构师", RoleDesc: "design"},
				{ID: "n2", Type: NodeExecutor, Label: "执行者", RoleDesc: "implement", AssistEnabled: assist},
			},
			Edges: []Edge{{ID: "e1", FromID: "n1", ToID: "n2"}},
		}
	}

	const hintMarker = "reasonix assist"

	store := NewStore()

	// Default (empty) enables the hint.
	in := store.gatherInput(newPipe(""), newRun(), "n2")
	if !strings.Contains(in, hintMarker) {
		t.Errorf("default node should get assist hint; input:\n%s", in)
	}
	if !strings.Contains(in, "辅助小任务分发") {
		t.Errorf("default node should get assist section title")
	}

	// Explicit "off" disables it.
	in = store.gatherInput(newPipe("off"), newRun(), "n2")
	if strings.Contains(in, hintMarker) {
		t.Errorf("assist=off node must NOT get assist hint; input:\n%s", in)
	}

	// No-upstream + task path also injects the hint.
	noEdge := &Pipeline{Nodes: newPipe("").Nodes}
	in = store.gatherInput(noEdge, newRun(), "n2")
	if !strings.Contains(in, hintMarker) {
		t.Errorf("no-upstream node should get assist hint; input:\n%s", in)
	}
}

func TestLoopIterationAssistHintInjection(t *testing.T) {
	store := NewStore()
	pipe := &Pipeline{
		Nodes: []AgentNode{
			{ID: "n1", Type: NodeArchitect, Label: "架构师", RoleDesc: "design", AssistEnabled: "off"},
			{ID: "n2", Type: NodeExecutor, Label: "执行者", RoleDesc: "implement"},
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

	// n1 has assist=off: iteration input must not contain the hint.
	in := store.gatherInputForIteration(pipe, run, "it1", "n1")
	if strings.Contains(in, "reasonix assist") {
		t.Errorf("assist=off node must NOT get hint in loop iteration; input:\n%s", in)
	}

	// n2 defaults on: iteration input should contain the hint.
	in = store.gatherInputForIteration(pipe, run, "it1", "n2")
	if !strings.Contains(in, "reasonix assist") {
		t.Errorf("default node should get assist hint in loop iteration; input:\n%s", in)
	}
}
