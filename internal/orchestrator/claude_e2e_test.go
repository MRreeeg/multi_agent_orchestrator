package orchestrator

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	claudeclient "reasonix/internal/executor/claude"
)

// TestClaudeServeReturnsVisibleTextEndToEnd drives a real retained claude
// stream-json runtime and verifies the loop-style turn returns visible text.
// It requires the Claude CLI plus a working provider route (ANTHROPIC_BASE_URL
// proxy or direct key) and is gated behind RUN_INTEGRATION=1 because the proxy
// can be unavailable in some environments.
func TestClaudeServeReturnsVisibleTextEndToEnd(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run the real Claude E2E")
	}
	bin := claudeclient.DiscoverBin()
	if bin == "" {
		t.Fatal("claude binary not found")
	}
	claudeBinaryOverride = bin
	defer func() { claudeBinaryOverride = "" }()

	workspace := t.TempDir()
	mgr := newClaudeRuntimeManager()
	defer mgr.cleanupAll()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	execResult, err := mgr.Execute(ctx, ExecSpec{
		Workspace:     workspace,
		Prompt:        "只回复OK",
		ModelRef:      "sonnet",
		DisplayModel:  "sonnet",
		NodeID:        "e2e-node",
		Mode:          "serve",
		Executor:      "claude",
		ApprovalMode:  "auto",
		ContextPolicy: "fresh",
	}, nil)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if strings.TrimSpace(execResult.FinalText) == "" {
		t.Fatalf("FinalText empty: %#v", execResult)
	}
	if execResult.ExternalSessionID == "" {
		t.Fatalf("ExternalSessionID empty: %#v", execResult)
	}
	t.Logf("E2E OK text=%q session=%s", execResult.FinalText, execResult.ExternalSessionID)
}
