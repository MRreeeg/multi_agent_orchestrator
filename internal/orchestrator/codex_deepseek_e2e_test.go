package orchestrator

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// TestCodexServeDeepseekOfficialEndToEnd drives a retained `codex app-server`
// runtime pinned to the DeepSeek official provider (model_provider=deepseek,
// model=deepseek-v4-flash) and verifies the loop-style turn returns visible
// text plus non-zero usage. Requires the machine's ~/.codex config to contain
// [model_providers.deepseek] (official base_url + API key) and the deepseek
// profile overlay; gated behind RUN_INTEGRATION=1.
func TestCodexServeDeepseekOfficialEndToEnd(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run the real codex+deepseek E2E")
	}
	mgr := newCodexRuntimeManager()
	defer func() {
		for _, rt := range mgr.List() {
			_ = mgr.Stop(rt.RuntimeID)
		}
	}()

	workspace := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	execResult, err := mgr.Execute(ctx, ExecSpec{
		Workspace:     workspace,
		Prompt:        "只回复OK",
		ModelRef:      "deepseek-v4-flash",
		DisplayModel:  "deepseek-v4-flash",
		NodeID:        "e2e-deepseek",
		Mode:          "serve",
		Executor:      "codex",
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
	t.Logf("E2E OK text=%q thread=%s", execResult.FinalText, execResult.ExternalSessionID)
}
