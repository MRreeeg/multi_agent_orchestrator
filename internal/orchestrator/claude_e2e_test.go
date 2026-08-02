package orchestrator

import (
	"context"
	"os"
	"path/filepath"
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
// TestClaudeServeDeepseekOfficialEndToEnd drives a retained claude
// stream-json runtime pinned to the DeepSeek official endpoint via
// CLAUDE_CONFIG_DIR (~/.claude-deepseek) and verifies visible text. Requires
// the machine-local DeepSeek config directory; gated behind RUN_INTEGRATION=1.
func TestClaudeServeDeepseekOfficialEndToEnd(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run the real claude+deepseek E2E")
	}
	bin := claudeclient.DiscoverBin()
	if bin == "" {
		t.Fatal("claude binary not found")
	}
	claudeBinaryOverride = bin
	defer func() { claudeBinaryOverride = "" }()
	dir := claudeConfigDir(ExecSpec{ModelRef: "deepseek-v4-flash"})
	if dir == "" {
		t.Skip("no DeepSeek overlay dir (~/.claude-deepseek) configured")
	}
	if _, err := os.Stat(filepath.Join(dir, "settings.json")); err != nil {
		t.Skipf("DeepSeek overlay missing settings.json: %v", err)
	}

	// Use the working directory (a real project, e.g. the repo) rather than a
	// bare temp dir: the Claude CLI is more reliable about emitting its init
	// event inside a project workspace.
	workspace, err := os.Getwd()
	if err != nil {
		workspace = t.TempDir()
	}
	mgr := newClaudeRuntimeManager()
	defer func() {
		for _, rt := range mgr.List() {
			_ = mgr.Stop(rt.RuntimeID)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	execResult, err := mgr.Execute(ctx, ExecSpec{
		Workspace:     workspace,
		Prompt:        "只回复OK",
		ModelRef:      "deepseek-v4-flash",
		DisplayModel:  "deepseek-v4-flash",
		NodeID:        "e2e-claude-deepseek",
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

	// Second turn on the same retained runtime must reuse the same session.
	execResult2, err := mgr.Execute(ctx, ExecSpec{
		Workspace:    workspace,
		Prompt:       "再回复OK",
		ModelRef:     "deepseek-v4-flash",
		DisplayModel: "deepseek-v4-flash",
		NodeID:       "e2e-claude-deepseek",
		Mode:         "serve",
		Executor:     "claude",
		ApprovalMode: "auto",
	}, nil)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if strings.TrimSpace(execResult2.FinalText) == "" {
		t.Fatalf("second FinalText empty: %#v", execResult2)
	}
	if execResult2.ExternalSessionID != execResult.ExternalSessionID {
		t.Fatalf("session changed across turns: %q -> %q", execResult.ExternalSessionID, execResult2.ExternalSessionID)
	}
	t.Logf("E2E second turn OK text=%q session=%s (reused)", execResult2.FinalText, execResult2.ExternalSessionID)
}

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

	// Second turn on the same retained runtime must reuse the same session.
	execResult2, err := mgr.Execute(ctx, ExecSpec{
		Workspace:    workspace,
		Prompt:       "再回复OK",
		ModelRef:     "deepseek-v4-flash",
		DisplayModel: "deepseek-v4-flash",
		NodeID:       "e2e-claude-deepseek",
		Mode:         "serve",
		Executor:     "claude",
		ApprovalMode: "auto",
	}, nil)
	if err != nil {
		t.Fatalf("second Execute: %v", err)
	}
	if strings.TrimSpace(execResult2.FinalText) == "" {
		t.Fatalf("second FinalText empty: %#v", execResult2)
	}
	if execResult2.ExternalSessionID != execResult.ExternalSessionID {
		t.Fatalf("session changed across turns: %q -> %q", execResult.ExternalSessionID, execResult2.ExternalSessionID)
	}
	t.Logf("E2E second turn OK text=%q session=%s (reused)", execResult2.FinalText, execResult2.ExternalSessionID)
}
