package orchestrator

import "testing"

// TestBindingMatchesNodeGraded covers the memory-graded match: only an
// engine/persona change may detach a binding; a model or mode flip must keep
// the binding (and its ProviderSession) alive.
func TestBindingMatchesNodeGraded(t *testing.T) {
	base := AgentNode{
		ID: "1", Type: NodeExecutor, Executor: ExecutorOpencode,
		Model: "deepseek/deepseek-v4-flash", Mode: "serve",
	}
	b := &AgentBinding{ID: "b1", NodeID: "1", Executor: string(base.Executor), Model: base.Model, Agent: "", Skill: "", Mode: base.Mode}

	if !bindingMatchesNode(b, base) {
		t.Fatal("identical config must match")
	}
	// Model tweak keeps the binding.
	modelTweaked := base
	modelTweaked.Model = "opencode/deepseek-v4-flash-free"
	if !bindingMatchesNode(b, modelTweaked) {
		t.Fatal("model-only change must keep the binding (memory preservation)")
	}
	// run↔serve flip keeps the binding.
	modeFlipped := base
	modeFlipped.Mode = "run"
	if !bindingMatchesNode(b, modeFlipped) {
		t.Fatal("mode-only change must keep the binding")
	}
	// Engine swap detaches.
	engineSwapped := base
	engineSwapped.Executor = ExecutorCodex
	if bindingMatchesNode(b, engineSwapped) {
		t.Fatal("executor change must detach the binding")
	}
	// Persona change detaches.
	personaChanged := base
	personaChanged.Agent = "frontend-analyst"
	if bindingMatchesNode(b, personaChanged) {
		t.Fatal("agent/persona change must detach the binding")
	}
	// Skill change detaches.
	skillChanged := base
	skillChanged.Skill = "review-agent"
	if bindingMatchesNode(b, skillChanged) {
		t.Fatal("skill change must detach the binding")
	}
}

// TestFindOrCreateBindingKeepsSessionOnModelChange verifies end-to-end that a
// model-only drift reuses the SAME ProviderSession (the agent's conversation
// memory survives a cost tweak).
func TestFindOrCreateBindingKeepsSessionOnModelChange(t *testing.T) {
	store := NewStore()
	sess, err := store.CreateOrchSession("memory test", "/tmp")
	if err != nil {
		t.Fatal(err)
	}
	node := AgentNode{ID: "2", Type: NodeExecutor, Label: "exec", Executor: ExecutorOpencode, Model: "deepseek/deepseek-v4-flash", Mode: "serve"}

	b1, ps1, err := store.FindOrCreateBindingAndProviderSession(sess.ID, node.ID, node, string(node.Executor), "/tmp", "reuse", true)
	if err != nil {
		t.Fatal(err)
	}
	ps1.ExternalSessionID = "ses_memory_anchor"
	store.providerSessions[ps1.ID] = ps1

	drifted := node
	drifted.Model = "opencode/deepseek-v4-flash-free"
	b2, ps2, err := store.FindOrCreateBindingAndProviderSession(sess.ID, node.ID, drifted, string(drifted.Executor), "/tmp", "reuse", true)
	if err != nil {
		t.Fatal(err)
	}
	if b1.ID != b2.ID {
		t.Fatalf("binding changed on model-only drift: %s → %s", b1.ID, b2.ID)
	}
	if ps1.ID != ps2.ID {
		t.Fatalf("provider session reset on model-only drift: %s → %s", ps1.ID, ps2.ID)
	}
	if b2.Model != drifted.Model {
		t.Fatalf("binding should record the new model, got %q", b2.Model)
	}

	// Persona change detaches and creates a fresh provider session.
	repersona := node
	repersona.Agent = "frontend-analyst"
	b3, ps3, err := store.FindOrCreateBindingAndProviderSession(sess.ID, node.ID, repersona, string(repersona.Executor), "/tmp", "reuse", true)
	if err != nil {
		t.Fatal(err)
	}
	if b3.ID == b1.ID {
		t.Fatal("persona change must create a new binding")
	}
	if ps3.ID == ps1.ID {
		t.Fatal("persona change must not reuse the old provider session")
	}
}
