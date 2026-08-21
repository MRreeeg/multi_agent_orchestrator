package orchestrator

import "testing"

func TestLoopSkipsNode(t *testing.T) {
	arch := &AgentNode{ID: "arch", Type: NodeArchitect}
	exec1 := &AgentNode{ID: "exec1", Type: NodeExecutor}
	exec2 := &AgentNode{ID: "exec2", Type: NodeExecutor}
	rev := &AgentNode{ID: "rev", Type: NodeReviewer}

	targeted := &LoopConfig{Enabled: true, ReviewNodeID: "rev", TargetNodeIDs: []string{"exec2"}}
	if !loopSkipsNode(targeted, arch) {
		t.Fatal("architect must be skipped after round 1")
	}
	if loopSkipsNode(targeted, rev) {
		t.Fatal("reviewer must never be skipped")
	}
	if !loopSkipsNode(targeted, exec1) {
		t.Fatal("non-target executor must be skipped when targetNodeIDs is set")
	}
	if loopSkipsNode(targeted, exec2) {
		t.Fatal("target executor must rerun every iteration")
	}

	legacy := &LoopConfig{Enabled: true, ReviewNodeID: "rev"}
	if loopSkipsNode(legacy, exec1) {
		t.Fatal("without targets all non-architect nodes rerun (legacy behavior)")
	}
}

func TestValidateLoopTargets(t *testing.T) {
	nodes := []AgentNode{
		{ID: "arch", Type: NodeArchitect},
		{ID: "exec", Type: NodeExecutor},
		{ID: "rev", Type: NodeReviewer},
	}
	if err := ValidateLoopTargets(&LoopConfig{ReviewNodeID: "rev"}, nodes); err != nil {
		t.Fatalf("empty targets must pass: %v", err)
	}
	if err := ValidateLoopTargets(&LoopConfig{ReviewNodeID: "rev", TargetNodeIDs: []string{"exec"}}, nodes); err != nil {
		t.Fatalf("valid target must pass: %v", err)
	}
	for _, bad := range []string{"rev", "arch", "missing"} {
		cfg := &LoopConfig{ReviewNodeID: "rev", TargetNodeIDs: []string{bad}}
		if err := ValidateLoopTargets(cfg, nodes); err == nil {
			t.Fatalf("target %q must be rejected", bad)
		}
	}
}

func TestNormalizeLoopConfigTrimsTargets(t *testing.T) {
	cfg := &LoopConfig{
		Enabled:       true,
		Mode:          "review_decides",
		MaxIterations: 3,
		ReviewNodeID:  "rev",
		Protocol:      "loop-review-v1",
		TargetNodeIDs: []string{" exec ", "", "exec", "exec2"},
	}
	got, err := NormalizeLoopConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(got.TargetNodeIDs) != 2 || got.TargetNodeIDs[0] != "exec" || got.TargetNodeIDs[1] != "exec2" {
		t.Fatalf("targets = %v, want [exec exec2]", got.TargetNodeIDs)
	}
}
