package control

import (
	"testing"

	"reasonix/internal/agent"
	"reasonix/internal/event"
	"reasonix/internal/provider"
)

func TestAdoptHistoryWithoutPathPreservesSystemPrompt(t *testing.T) {
	dir := t.TempDir()
	msgs := []provider.Message{
		{Role: provider.RoleSystem, Content: "old sys"},
		{Role: provider.RoleUser, Content: "hello"},
		{Role: provider.RoleAssistant, Content: "world"},
	}

	exec := agent.New(nil, nil, agent.NewSession("new sys"), agent.Options{}, event.Discard)
	c := New(Options{Executor: exec, SessionDir: dir, Label: "test", DisableColdResumePrune: true})

	c.AdoptHistory(msgs, "")
	if got := exec.Session().Snapshot()[0].Content; got != "new sys" {
		t.Fatalf("system prompt after AdoptHistory = %q, want new sys", got)
	}
	if !exec.Session().HasSystemMessage() {
		t.Fatal("adopted session lost its system message")
	}
}
