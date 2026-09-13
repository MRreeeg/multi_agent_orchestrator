package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	codexclient "reasonix/internal/executor/codex"
	mimoclient "reasonix/internal/executor/mimo"
)

// TestExecInterruptErrorClassification guards the loop's error-cause logic:
// user interrupts and scheduler kills must be treated as resumable interruptions,
// while real failures must be surfaced as-is (never downgraded to "context canceled").
func TestExecInterruptErrorClassification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "deadline exceeded", err: context.DeadlineExceeded, want: true},
		{name: "runtime interrupted sentinel", err: ErrRuntimeInterrupted, want: true},
		{name: "codex interrupted sentinel", err: codexclient.ErrAppServerTurnInterrupted, want: true},
		{name: "mimo interrupted sentinel", err: mimoclient.ErrTurnInterrupted, want: true},
		{name: "wrapped runtime interrupted", err: fmt.Errorf("runtime turn interrupted: %w", ErrRuntimeInterrupted), want: true},
		{name: "wrapped codex interrupted", err: fmt.Errorf("turn %q: %w", "t1", codexclient.ErrAppServerTurnInterrupted), want: true},
		{name: "wrapped mimo interrupted", err: fmt.Errorf("session canceled: %w", mimoclient.ErrTurnInterrupted), want: true},
		{name: "plain failure", err: errors.New("provider 429: concurrency limit exceeded"), want: false},
		{name: "node failed", err: fmt.Errorf("node execution: %w", errors.New("agent completed without assistant output")), want: false},
		{name: "nil", err: nil, want: false},
	}
	for _, tc := range cases {
		if got := execInterruptError(tc.err); got != tc.want {
			t.Errorf("%s: execInterruptError(%v) = %v, want %v", tc.name, tc.err, got, tc.want)
		}
	}
}

// TestLevelCancelCauseSurvivesWrapping ensures the sibling-cancel enrichment
// keeps the error classified as interrupted (so the run prefers the real
// failure) while still carrying the human-readable cancel cause.
func TestLevelCancelCauseSurvivesWrapping(t *testing.T) {
	ci := &levelCancelCause{}
	ci.set("同轮节点 \"executor\" 失败：provider 429")
	cause := ci.get()
	if cause == "" {
		t.Fatal("cancel cause should be recorded")
	}
	enriched := fmt.Errorf("%s（%w）", cause, context.Canceled)
	if !execInterruptError(enriched) {
		t.Fatalf("enriched error must still classify as interrupted: %v", enriched)
	}
	if !errors.Is(enriched, context.Canceled) {
		t.Fatalf("enriched error must wrap context.Canceled: %v", enriched)
	}
}

// TestOpencodeInterruptFlagsNodeTurn verifies that Interrupt marks the runtime
// so a later Execute reports a deliberate user interrupt instead of a generic
// "completed without assistant output".
func TestOpencodeInterruptFlagsNodeTurn(t *testing.T) {
	mgr := newOpenCodeRuntimeManager()
	rt := &opencodeRuntime{
		ID:           "oc_rt_interrupt",
		client:       nil, // no live serve process; Interrupt must still flag it
		sessionID:    "",
		status:       RuntimeBusy,
		interruptRequested: false,
	}
	mgr.mu.Lock()
	mgr.runtimes["oc_rt_interrupt"] = rt
	mgr.mu.Unlock()

	if err := mgr.Interrupt("oc_rt_interrupt"); err != nil {
		t.Fatalf("Interrupt = %v", err)
	}
	rt.mu.Lock()
	flagged := rt.interruptRequested
	rt.mu.Unlock()
	if !flagged {
		t.Fatal("Interrupt must set interruptRequested so Execute reports the user action")
	}

	if err := mgr.Interrupt("oc_rt_unknown"); err == nil {
		t.Fatal("Interrupt of an unknown runtime must error")
	}
}