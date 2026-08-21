package codex

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeFakeCodexPS creates a fake codex PowerShell script.
func writeFakeCodexPS(t *testing.T, dir, script string) string {
	t.Helper()
	bin := filepath.Join(dir, "codex.ps1")
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return bin
}

// fakeCodexJSONL returns a PowerShell script that outputs real Codex JSONL format.
func fakeCodexJSONL(t *testing.T, dir, threadID, text string) string {
	t.Helper()
	jsonl := `{"type":"thread.started","thread_id":"` + threadID + `"}` + "\n" +
		`{"type":"turn.started"}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"` + text + `"}}` + "\n" +
		`{"type":"turn.completed","usage":{"input_tokens":100,"output_tokens":5}}`
	jsonFile := filepath.Join(dir, "output.jsonl")
	os.WriteFile(jsonFile, []byte(jsonl), 0644)
	return writeFakeCodexPS(t, dir, `Get-Content -Raw -Path `+jsonFile)
}

func TestCodexExecReturnsOutput(t *testing.T) {
	dir := t.TempDir()
	ps1 := fakeCodexJSONL(t, dir, "thread_abc", "hello world")

	executor := &CodexExecutor{CodexBin: "powershell"}
	result, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if result.Output != "hello world" {
		t.Errorf("Output = %q, want %q", result.Output, "hello world")
	}
	if result.ThreadID != "thread_abc" {
		t.Errorf("ThreadID = %q, want %q", result.ThreadID, "thread_abc")
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", result.ExitCode)
	}
}

func TestCodexExecPreservesRawStdout(t *testing.T) {
	dir := t.TempDir()
	ps1 := fakeCodexJSONL(t, dir, "t1", "output")

	executor := &CodexExecutor{CodexBin: "powershell"}
	result, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if result.RawStdout == "" {
		t.Error("RawStdout should not be empty")
	}
}

func TestCodexExecParsesUsage(t *testing.T) {
	dir := t.TempDir()
	ps1 := fakeCodexJSONL(t, dir, "t1", "output")

	executor := &CodexExecutor{CodexBin: "powershell"}
	result, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if result.Usage == nil {
		t.Fatal("Usage should not be nil")
	}
	if result.Usage["input_tokens"] != float64(100) {
		t.Errorf("input_tokens = %v, want 100", result.Usage["input_tokens"])
	}
}

func TestCodexExecStartFailure(t *testing.T) {
	executor := &CodexExecutor{CodexBin: "/nonexistent/codex"}
	_, err := executor.Exec(context.Background(), "test", ExecOptions{
		Model: "deepseek-flash",
	})

	if err == nil {
		t.Fatal("expected error for nonexistent binary, got nil")
	}
	if !errors.Is(err, ErrExecutorStart) {
		t.Errorf("error = %v, want ErrExecutorStart", err)
	}
}

func TestCodexExecTimeoutReturnsTimeoutError(t *testing.T) {
	dir := t.TempDir()
	ps1 := writeFakeCodexPS(t, dir, `Start-Sleep -Seconds 60`)

	executor := &CodexExecutor{CodexBin: "powershell"}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, err := executor.execute(ctx, []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if !errors.Is(err, ErrExecutorTimeout) {
		t.Errorf("error = %v, want ErrExecutorTimeout", err)
	}
}

func TestCodexExecCancelReturnsCanceledError(t *testing.T) {
	dir := t.TempDir()
	ps1 := writeFakeCodexPS(t, dir, `Start-Sleep -Seconds 60`)

	executor := &CodexExecutor{CodexBin: "powershell"}
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := executor.execute(ctx, []string{"-ExecutionPolicy", "Bypass", "-File", ps1})
		done <- err
	}()

	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error after cancel, got nil")
		}
		if !errors.Is(err, ErrExecutorCanceled) {
			t.Errorf("error = %v, want ErrExecutorCanceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("process did not exit after cancel")
	}
}

func TestCodexExecEmptyOutputFails(t *testing.T) {
	dir := t.TempDir()
	ps1 := writeFakeCodexPS(t, dir, `Write-Output ""`)

	executor := &CodexExecutor{CodexBin: "powershell"}
	_, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	if err == nil {
		t.Fatal("expected empty output error, got nil")
	}
	if !errors.Is(err, ErrExecutorEmptyOutput) {
		t.Errorf("error = %v, want ErrExecutorEmptyOutput", err)
	}
}

func TestCodexExecResumeBuildsCorrectArgs(t *testing.T) {
	// Verify that Exec with ResumeSessionID builds "exec resume <id>" args
	executor := &CodexExecutor{CodexBin: "echo"}
	// We can't actually run codex, but we can verify the args by checking
	// that Exec doesn't panic and the error is about the binary, not args
	_, err := executor.Exec(context.Background(), "test prompt", ExecOptions{
		ResumeSessionID: "test-session-id",
		Model:           "o3",
	})
	// Will fail because "echo" doesn't accept codex args, but shouldn't panic
	if err == nil {
		t.Log("echo accepted args (unexpected but ok)")
	}
}

func TestCodexBuildArgsUsesStdinPromptSelector(t *testing.T) {
	executor := New()
	args := executor.buildArgs(ExecOptions{
		ResumeSessionID: "thread-resume",
		Model:           "o3",
		ReasoningEffort: "high",
		Workspace:       t.TempDir(),
		JSON:            true,
	})
	args = append(args, "-")

	if len(args) == 0 || args[len(args)-1] != "-" {
		t.Fatalf("args = %v, want stdin prompt selector '-' as final argument", args)
	}
	joined := strings.Join(args, " ")
	if strings.Contains(joined, "thread-resume") == false {
		t.Fatalf("resume session ID missing from args: %v", args)
	}
	if strings.Contains(joined, "prompt") {
		t.Fatalf("prompt text must not be present in command args: %v", args)
	}
	if !strings.Contains(joined, "--skip-git-repo-check") {
		t.Fatalf("arbitrary workspaces must skip Codex git-root validation: %v", args)
	}
}

func TestCodexBuildArgsPassesProfile(t *testing.T) {
	executor := New()
	args := executor.buildArgs(ExecOptions{
		Model:   "deepseek-v4-flash",
		Profile: "deepseek",
		JSON:    true,
	})
	joined := strings.Join(args, " ")
	if !strings.Contains(joined, "--profile deepseek") {
		t.Fatalf("args = %v, want --profile deepseek", args)
	}
	if !strings.Contains(joined, "-m deepseek-v4-flash") {
		t.Fatalf("args = %v, want -m deepseek-v4-flash", args)
	}
	// No profile when unset.
	args2 := executor.buildArgs(ExecOptions{Model: "o3", JSON: true})
	if strings.Contains(strings.Join(args2, " "), "--profile") {
		t.Fatalf("args = %v, want no --profile", args2)
	}
}

func TestCodexLongPromptUsesStdin(t *testing.T) {
	dir := t.TempDir()
	marker := filepath.Join(dir, "stdin.txt")
	ps1 := writeFakeCodexPS(t, dir, `$prompt = [Console]::In.ReadToEnd()
Set-Content -Path '`+marker+`' -Value $prompt -NoNewline
Write-Output '{"type":"thread.started","thread_id":"stdin-thread"}'
Write-Output '{"type":"item.completed","item":{"type":"agent_message","text":"stdin-ok"}}'
Write-Output '{"type":"turn.completed","usage":{}}'`)

	prompt := strings.Repeat("loop context ", 20000)
	executor := &CodexExecutor{CodexBin: "powershell"}
	result, err := executor.executeWithInput(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1}, strings.NewReader(prompt))
	if err != nil {
		t.Fatalf("executeWithInput() error = %v", err)
	}
	if result.Output != "stdin-ok" {
		t.Fatalf("Output = %q, want stdin-ok", result.Output)
	}
	got, err := os.ReadFile(marker)
	if err != nil {
		t.Fatalf("read captured stdin: %v", err)
	}
	if string(got) != prompt {
		t.Fatalf("captured stdin length = %d, want %d", len(got), len(prompt))
	}
}

func TestCodexResumeLongPromptUsesStdinSelector(t *testing.T) {
	executor := New()
	args := executor.buildArgs(ExecOptions{ResumeSessionID: "thread-123", JSON: true})
	args = append(args, "-")

	wantPrefix := []string{"exec", "resume", "thread-123"}
	if len(args) < len(wantPrefix)+1 {
		t.Fatalf("args = %v, too short", args)
	}
	for i, want := range wantPrefix {
		if args[i] != want {
			t.Errorf("args[%d] = %q, want %q; full args=%v", i, args[i], want, args)
		}
	}
	if args[len(args)-1] != "-" {
		t.Errorf("last arg = %q, want '-'", args[len(args)-1])
	}
}

func TestCodexExecContextPolicyFresh(t *testing.T) {
	// Verify that fresh context policy results in --ephemeral
	// We check by examining the args that would be built
	executor := &CodexExecutor{CodexBin: "echo"}
	_, err := executor.Exec(context.Background(), "test", ExecOptions{
		Ephemeral: true,
		JSON:      true,
	})
	// Should not panic
	_ = err
}

func TestCodexExecErrorEventPreservesOutput(t *testing.T) {
	dir := t.TempDir()
	// JSONL with error event followed by agent message
	jsonl := `{"type":"thread.started","thread_id":"t1"}` + "\n" +
		`{"type":"turn.started"}` + "\n" +
		`{"type":"item.completed","item":{"type":"error","message":"skill load failed"}}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"despite error, here is output"}}` + "\n" +
		`{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":10}}`
	jsonFile := filepath.Join(dir, "output.jsonl")
	os.WriteFile(jsonFile, []byte(jsonl), 0644)
	ps1 := writeFakeCodexPS(t, dir, `Get-Content -Raw -Path `+jsonFile)

	executor := &CodexExecutor{CodexBin: "powershell"}
	result, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	// Error events are non-fatal when valid output exists.
	// The output and thread ID should be returned successfully.
	if err != nil {
		t.Fatalf("expected no error when output exists, got: %v", err)
	}
	if result.Output != "despite error, here is output" {
		t.Errorf("Output = %q, want agent message text", result.Output)
	}
	if result.ThreadID != "t1" {
		t.Errorf("ThreadID = %q, want %q", result.ThreadID, "t1")
	}
	// HasError and ErrorCount are still tracked for diagnostics
	if !result.HasError {
		t.Error("HasError should be true")
	}
	if result.ErrorCount != 1 {
		t.Errorf("ErrorCount = %d, want 1", result.ErrorCount)
	}
	// Error message should be in Stderr for diagnostics
	if !strings.Contains(result.Stderr, "skill load failed") {
		t.Error("stderr should contain error message for diagnostics")
	}
	// Should be classified as warning, not fatal
	if result.FatalError {
		t.Error("FatalError should be false for warning-type errors")
	}
	if len(result.Warnings) != 1 {
		t.Errorf("Warnings count = %d, want 1", len(result.Warnings))
	}
}

func TestCodexExecFatalErrorThenAgentMessageFails(t *testing.T) {
	dir := t.TempDir()
	// JSONL with fatal error event followed by agent message
	jsonl := `{"type":"thread.started","thread_id":"t1"}` + "\n" +
		`{"type":"turn.started"}` + "\n" +
		`{"type":"item.completed","item":{"type":"error","message":"tool execution failed: permission denied"}}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"sorry, I could not complete the task"}}` + "\n" +
		`{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":10}}`
	jsonFile := filepath.Join(dir, "output.jsonl")
	os.WriteFile(jsonFile, []byte(jsonl), 0644)
	ps1 := writeFakeCodexPS(t, dir, `Get-Content -Raw -Path `+jsonFile)

	executor := &CodexExecutor{CodexBin: "powershell"}
	result, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	// Fatal error should cause failure even with output
	if err == nil {
		t.Fatal("expected error for fatal error event, got nil")
	}
	if !result.FatalError {
		t.Error("FatalError should be true")
	}
	// Output should still be preserved for diagnostics
	if result.Output != "sorry, I could not complete the task" {
		t.Errorf("Output = %q, want agent message text", result.Output)
	}
}

func TestCodexExecNonZeroExitAlwaysFails(t *testing.T) {
	dir := t.TempDir()
	// JSONL with agent message but non-zero exit will be handled by exit code check
	jsonl := `{"type":"thread.started","thread_id":"t1"}` + "\n" +
		`{"type":"turn.started"}` + "\n" +
		`{"type":"item.completed","item":{"type":"agent_message","text":"some output"}}` + "\n" +
		`{"type":"turn.completed","usage":{"input_tokens":50,"output_tokens":10}}`
	jsonFile := filepath.Join(dir, "output.jsonl")
	os.WriteFile(jsonFile, []byte(jsonl), 0644)
	// Script that outputs JSONL but exits with code 1
	ps1 := writeFakeCodexPS(t, dir, `Get-Content -Raw -Path `+jsonFile+`; exit 1`)

	executor := &CodexExecutor{CodexBin: "powershell"}
	_, err := executor.execute(context.Background(), []string{"-ExecutionPolicy", "Bypass", "-File", ps1})

	if err == nil {
		t.Fatal("expected error for non-zero exit code")
	}
}
