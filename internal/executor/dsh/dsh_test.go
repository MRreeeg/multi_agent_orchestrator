package dsh

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// writeFakeDSHCmd creates a fake `dsh` batch script that echoes its argv and
// optionally checks the patch overlay path (argv index 4) exists during the run.
func writeFakeDSHCmd(t *testing.T, dir, script string) string {
	t.Helper()
	bin := filepath.Join(dir, "fake-dsh.cmd")
	if err := os.WriteFile(bin, []byte(script), 0755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func TestExecReturnsOutput(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeDSHCmd(t, dir, "@echo off\r\necho hello from dsh\r\n")
	executor := &DshExecutor{DshBin: bin}
	result, err := executor.Exec(context.Background(), "do the thing", ExecOptions{})
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if result.Output != "hello from dsh" {
		t.Errorf("Output = %q, want %q", result.Output, "hello from dsh")
	}
	if result.ExitCode != 0 {
		t.Errorf("ExitCode = %d, want 0", result.ExitCode)
	}
}

func TestExecPassesModelPatchAndCleansUp(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeDSHCmd(t, dir, "@echo off\r\nif exist %4 echo PATCH_EXISTS\r\necho ARGS:%*\r\n")
	executor := &DshExecutor{DshBin: bin}
	result, err := executor.Exec(context.Background(), "task", ExecOptions{Model: "deepseek-v4-flash"})
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if !strings.Contains(result.Output, "PATCH_EXISTS") {
		t.Errorf("Output = %q, want PATCH_EXISTS (patch overlay must be on disk during the run)", result.Output)
	}
	if !strings.Contains(result.RawStdout, "--patch") {
		t.Errorf("RawStdout = %q, want --patch flag", result.RawStdout)
	}
	re := regexp.MustCompile(`--patch (\S+)`)
	m := re.FindStringSubmatch(result.RawStdout)
	if len(m) != 2 {
		t.Fatalf("could not extract patch path from %q", result.RawStdout)
	}
	if _, err := os.Stat(m[1]); !os.IsNotExist(err) {
		t.Errorf("patch file %q still exists after Exec", m[1])
	}
}

func TestExecNoModelOmitsPatch(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeDSHCmd(t, dir, "@echo off\r\necho ARGS:%*\r\n")
	executor := &DshExecutor{DshBin: bin}
	result, err := executor.Exec(context.Background(), "task", ExecOptions{})
	if err != nil {
		t.Fatalf("Exec() error = %v", err)
	}
	if strings.Contains(result.RawStdout, "--patch") {
		t.Errorf("RawStdout = %q, unexpected --patch", result.RawStdout)
	}
	if !strings.Contains(result.RawStdout, "--profile headless") {
		t.Errorf("RawStdout = %q, want --profile headless", result.RawStdout)
	}
}

func TestExecExitCodeNonZero(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeDSHCmd(t, dir, "@echo off\r\necho something\r\nexit /b 3\r\n")
	executor := &DshExecutor{DshBin: bin}
	result, err := executor.Exec(context.Background(), "task", ExecOptions{})
	if err == nil {
		t.Fatal("expected error for non-zero exit")
	}
	if !errors.Is(err, ErrExecutorExit) {
		t.Errorf("error = %v, want ErrExecutorExit", err)
	}
	if result.ExitCode != 3 {
		t.Errorf("ExitCode = %d, want 3", result.ExitCode)
	}
}

func TestExecEmptyOutput(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeDSHCmd(t, dir, "@echo off\r\n")
	executor := &DshExecutor{DshBin: bin}
	_, err := executor.Exec(context.Background(), "task", ExecOptions{})
	if !errors.Is(err, ErrExecutorEmptyOutput) {
		t.Errorf("error = %v, want ErrExecutorEmptyOutput", err)
	}
}

func TestExecStartFailure(t *testing.T) {
	executor := &DshExecutor{DshBin: `C:\nonexistent\dsh.exe`}
	_, err := executor.Exec(context.Background(), "task", ExecOptions{})
	if err == nil {
		t.Fatal("expected error for nonexistent binary")
	}
	if !errors.Is(err, ErrExecutorStart) {
		t.Errorf("error = %v, want ErrExecutorStart", err)
	}
}

func TestExecTimeout(t *testing.T) {
	dir := t.TempDir()
	bin := writeFakeDSHCmd(t, dir, "@echo off\r\nping -n 6 127.0.0.1 >nul\r\n")
	executor := &DshExecutor{DshBin: bin}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_, err := executor.Exec(ctx, "task", ExecOptions{})
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if !errors.Is(err, ErrExecutorTimeout) && !errors.Is(err, ErrExecutorCanceled) {
		t.Errorf("error = %v, want timeout/canceled", err)
	}
}

func TestBuildEnvPermissionMapping(t *testing.T) {
	joined := func(opts ExecOptions) string { return strings.Join(buildEnv(opts), "\n") }

	if s := joined(ExecOptions{PermissionMode: "danger-full-access"}); !strings.Contains(s, "DSH_PERMISSION_MODE=danger-full-access") {
		t.Errorf("env missing danger-full-access:\n%s", s)
	}
	if s := joined(ExecOptions{}); !strings.Contains(s, "DSH_PERMISSION_MODE=workspace-write") {
		t.Errorf("default env missing workspace-write:\n%s", s)
	}
	if s := joined(ExecOptions{DshHome: `D:\dsh-home`}); !strings.Contains(s, `DSH_HOME=D:\dsh-home`) {
		t.Errorf("env missing DSH_HOME override:\n%s", s)
	}
}

func TestWriteModelPatchContent(t *testing.T) {
	path, cleanup, err := writeModelPatch("deepseek-v4-flash")
	if err != nil {
		t.Fatalf("writeModelPatch() error = %v", err)
	}
	defer cleanup()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read patch: %v", err)
	}
	s := string(data)
	for _, want := range []string{"agent-default-model", "deepseek-v4-flash", "deepseek-official"} {
		if !strings.Contains(s, want) {
			t.Errorf("patch content missing %q:\n%s", want, s)
		}
	}
}

func TestResolveCommand(t *testing.T) {
	bin, prefix, err := resolveCommand("dsh")
	if err != nil || bin != "dsh" || len(prefix) != 0 {
		t.Errorf("resolveCommand(dsh) = %q %v %v, want dsh [] nil", bin, prefix, err)
	}
	js := `C:\x\bin.js`
	bin2, prefix2, err := resolveCommand(js)
	if err != nil || len(prefix2) != 1 || prefix2[0] != js {
		t.Errorf("resolveCommand(js) = %q %v %v, want node [%s] nil", bin2, prefix2, err, js)
	}
}
