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
	// The old fixed 5m budget is gone; the default now comes from the
	// activity-watchdog package knobs. Idle default is 15m: long silent
	// tool calls must not trip the stall, while wedged turns (no content
	// events at all — lifecycle pings no longer feed the clock) are still
	// caught well before the absolute ceiling.
	if got := turnIdleTimeoutDefault; got != 15*time.Minute {
		t.Fatalf("default idle timeout = %v, want 15m", got)
	}
	if got := turnMaxDurationDefault; got != 2*time.Hour {
		t.Fatalf("default max duration = %v, want 2h", got)
	}
}
