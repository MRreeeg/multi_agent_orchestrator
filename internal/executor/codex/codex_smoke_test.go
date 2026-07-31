package codex

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// TestRealCodexExecResume is a real integration test that runs the actual Codex CLI.
// It is gated by the RUN_CODEX_INTEGRATION environment variable.
//
// Usage:
//
//	$env:RUN_CODEX_INTEGRATION = "1"
//	go test -run TestRealCodexExecResume ./internal/executor/codex -v -timeout 120s
//
// This test verifies:
// A. codex exec --json初次执行 — exit 0, thread_id, agent_message
// B. codex exec resume <thread_id> --json — same thread_id, correct output
// C. Go CodexExecutor.Exec() first call — ExecResult.ThreadID, Output, ExitCode
// D. Go CodexExecutor.Exec() resume call — same ThreadID, correct output
func TestRealCodexExecResume(t *testing.T) {
	if os.Getenv("RUN_CODEX_INTEGRATION") != "1" {
		t.Skip("skipping real Codex integration test; set RUN_CODEX_INTEGRATION=1 to enable")
	}

	// ── Diagnostic: find codex executable ──
	bin, err := exec.LookPath("codex")
	if err != nil {
		t.Fatalf("codex not found in PATH: %v", err)
	}
	t.Logf("[diag] codex executable: %s", bin)

	// Check non-sensitive env vars
	for _, key := range []string{"OPENAI_API_KEY", "OPENAI_BASE_URL", "CODEX_HOME", "HTTP_PROXY", "HTTPS_PROXY"} {
		if v := os.Getenv(key); v != "" {
			t.Logf("[diag] %s is set (length=%d)", key, len(v))
		} else {
			t.Logf("[diag] %s is NOT set", key)
		}
	}

	// ── Step A: CLI first exec ──
	t.Run("A_CLI_FirstExec", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		cmd := exec.CommandContext(ctx, "codex", "exec", "--json", "--dangerously-bypass-approvals-and-sandbox", "请只输出 CODEX_SMOKE_OK")
		t.Logf("[diag] running: %s", strings.Join(cmd.Args, " "))

		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		err := cmd.Run()
		exitCode := 0
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				t.Fatalf("failed to start codex: %v", err)
			}
		}

		t.Logf("[diag] exit code: %d", exitCode)
		t.Logf("[diag] stdout length: %d", stdout.Len())
		t.Logf("[diag] stderr length: %d", stderr.Len())
		if stdout.Len() > 0 {
			t.Logf("[diag] stdout (first 2000 chars): %s", truncate(stdout.String(), 2000))
		}
		if stderr.Len() > 0 {
			t.Logf("[diag] stderr (first 2000 chars): %s", truncate(stderr.String(), 2000))
		}

		if exitCode != 0 {
			t.Fatalf("codex exec failed with exit code %d", exitCode)
		}

		// Parse JSONL for thread_id
		threadID := extractThreadID(stdout.String())
		if threadID == "" {
			t.Fatalf("no thread_id found in output")
		}
		t.Logf("[diag] thread_id: %s", threadID)

		// Verify agent_message exists
		if !strings.Contains(stdout.String(), "agent_message") {
			t.Fatalf("no agent_message found in output")
		}

		// Save thread_id for Step B
		t.Setenv("SMOKE_THREAD_ID", threadID)
	})

	// ── Step B: CLI resume exec ──
	t.Run("B_CLI_ResumeExec", func(t *testing.T) {
		threadID := os.Getenv("SMOKE_THREAD_ID")
		if threadID == "" {
			t.Skip("no thread_id from Step A")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		args := []string{"exec", "resume", threadID, "--json", "--dangerously-bypass-approvals-and-sandbox", "请只输出 CODEX_RESUME_OK"}
		cmd := exec.CommandContext(ctx, "codex", args...)
		t.Logf("[diag] running: %s", strings.Join(cmd.Args, " "))

		var stdout, stderr strings.Builder
		cmd.Stdout = &stdout
		cmd.Stderr = &stderr

		err := cmd.Run()
		exitCode := 0
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				exitCode = exitErr.ExitCode()
			} else {
				t.Fatalf("failed to start codex: %v", err)
			}
		}

		t.Logf("[diag] exit code: %d", exitCode)
		t.Logf("[diag] stdout length: %d", stdout.Len())
		if stdout.Len() > 0 {
			t.Logf("[diag] stdout (first 2000 chars): %s", truncate(stdout.String(), 2000))
		}

		if exitCode != 0 {
			t.Fatalf("codex exec resume failed with exit code %d", exitCode)
		}

		// Verify same thread_id
		resumeThreadID := extractThreadID(stdout.String())
		if resumeThreadID != threadID {
			t.Errorf("resume thread_id = %q, want %q", resumeThreadID, threadID)
		}

		// Verify output contains expected marker
		if !strings.Contains(stdout.String(), "CODEX_RESUME_OK") {
			t.Errorf("resume output does not contain CODEX_RESUME_OK")
		}
	})

	// ── Step C: Go Executor first exec ──
	var firstThreadID string
	t.Run("C_GoExecutor_FirstExec", func(t *testing.T) {
		executor := New()
		result, err := executor.Exec(context.Background(), "请只输出 CODEX_SMOKE_OK", ExecOptions{
			JSON:      true,
			BypassAll: true,
		})
		if err != nil {
			t.Fatalf("Go executor exec failed: %v", err)
		}

		t.Logf("[diag] exit code: %d", result.ExitCode)
		t.Logf("[diag] thread_id: %s", result.ThreadID)
		t.Logf("[diag] output length: %d", len(result.Output))
		t.Logf("[diag] output (first 500 chars): %s", truncate(result.Output, 500))
		t.Logf("[diag] raw stdout length: %d", len(result.RawStdout))
		t.Logf("[diag] stderr length: %d", len(result.Stderr))

		if result.ThreadID == "" {
			t.Fatal("ExecResult.ThreadID is empty")
		}
		if result.Output == "" {
			t.Fatal("ExecResult.Output is empty")
		}
		if result.ExitCode != 0 {
			t.Fatalf("ExecResult.ExitCode = %d, want 0", result.ExitCode)
		}

		firstThreadID = result.ThreadID
	})

	// ── Step D: Go Executor resume exec ──
	t.Run("D_GoExecutor_ResumeExec", func(t *testing.T) {
		if firstThreadID == "" {
			t.Skip("no thread_id from Step C")
		}

		executor := New()
		result, err := executor.Exec(context.Background(), "请只输出 CODEX_RESUME_OK", ExecOptions{
			JSON:            true,
			BypassAll:       true,
			ResumeSessionID: firstThreadID,
		})
		if err != nil {
			t.Fatalf("Go executor resume failed: %v", err)
		}

		t.Logf("[diag] exit code: %d", result.ExitCode)
		t.Logf("[diag] thread_id: %s", result.ThreadID)
		t.Logf("[diag] output length: %d", len(result.Output))
		t.Logf("[diag] output (first 500 chars): %s", truncate(result.Output, 500))

		// Verify same thread_id
		if result.ThreadID != firstThreadID {
			t.Errorf("resume thread_id = %q, want %q", result.ThreadID, firstThreadID)
		}

		// Verify output
		if result.Output == "" {
			t.Fatal("resume ExecResult.Output is empty")
		}
	})
}

// extractThreadID parses thread_id from JSONL output.
func extractThreadID(jsonl string) string {
	for _, line := range strings.Split(jsonl, "\n") {
		line = strings.TrimSpace(line)
		if !strings.Contains(line, "thread.started") {
			continue
		}
		// Simple extraction: find "thread_id":"..."
		idx := strings.Index(line, `"thread_id":"`)
		if idx < 0 {
			continue
		}
		start := idx + len(`"thread_id":"`)
		end := strings.Index(line[start:], `"`)
		if end < 0 {
			continue
		}
		return line[start : start+end]
	}
	return ""
}

// truncate shortens a string to maxLen characters.
func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return fmt.Sprintf("%s... (truncated, total %d chars)", s[:maxLen], len(s))
}
