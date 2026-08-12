package orchestrator

import (
	"testing"
	"time"
)

func TestOpenCodeDisciplineDenyTools(t *testing.T) {
	d := opencodeDiscipline{}
	if got := d.denyTools(ExecSpec{}); got != nil {
		t.Fatalf("default spec should keep the server tool default, got %v", got)
	}
	got := d.denyTools(ExecSpec{ToolsReadOnly: true})
	if got == nil {
		t.Fatal("read-only spec must produce a deny map")
	}
	if len(got) == 0 {
		t.Fatal("read-only deny map must not be empty")
	}
	for _, denied := range []string{"bash", "edit", "write", "move", "patch", "create", "delete", "task", "webfetch", "websearch"} {
		if enabled, ok := got[denied]; !ok || enabled {
			t.Errorf("read-only mode must deny %q, got %v (ok=%v)", denied, enabled, ok)
		}
	}
	// Read-only tools must NOT be denied.
	for _, allowed := range []string{"read", "grep", "glob"} {
		if denied, ok := got[allowed]; ok && !denied {
			t.Errorf("read-only mode must not list read tool %q as denied, got denied=%v", allowed, denied)
		}
	}
}

func TestOpenCodeDisciplineTurnBudgetOverride(t *testing.T) {
	d := opencodeDiscipline{}
	if got := d.turnBudget(); got != 5*time.Minute {
		t.Fatalf("default turn budget = %v, want 5m", got)
	}
}
