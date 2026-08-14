package dsh

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

func resultStderr(r *ExecutorResult) string {
	if r == nil {
		return ""
	}
	return r.Stderr
}

// TestDshHeadlessReturnsVisibleTextEndToEnd runs the real one-shot profile
// against the installed dsh CLI and a working DeepSeek route. It is env-gated:
// set RUN_INTEGRATION=1 to execute (requires a dsh install and credentials).
func TestDshHeadlessReturnsVisibleTextEndToEnd(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run real-model E2E")
	}
	executor := New()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := executor.Exec(ctx, "Reply with exactly: E2E_OK", ExecOptions{PermissionMode: "danger-full-access"})
	if err != nil {
		t.Fatalf("Exec() error = %v (stderr: %s)", err, resultStderr(result))
	}
	if !strings.Contains(result.Output, "E2E_OK") {
		t.Errorf("Output = %q, want E2E_OK", result.Output)
	}
}

// TestDshHeadlessModelPatchEndToEnd verifies the generated `--patch` overlay
// (agent-default-model) is accepted by a real dsh launch. The saved harness
// default may still win for the actual model; this only asserts the overlay
// does not break the launch and the turn completes.
func TestDshHeadlessModelPatchEndToEnd(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run real-model E2E")
	}
	executor := New()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	result, err := executor.Exec(ctx, "Reply with exactly: E2E_PATCH_OK", ExecOptions{
		Model:          "deepseek-v4-flash",
		PermissionMode: "danger-full-access",
	})
	if err != nil {
		t.Fatalf("Exec() error = %v (stderr: %s)", err, resultStderr(result))
	}
	if !strings.Contains(result.Output, "E2E_PATCH_OK") {
		t.Errorf("Output = %q, want E2E_PATCH_OK", result.Output)
	}
}
