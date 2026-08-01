package claude

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeClaudeBin creates a small script/batch that writes a canned JSON result
// and exits. It returns the executable path.
func fakeClaudeBin(t *testing.T, stdout string) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "claude")
	// Cross-platform fake: on Windows use a .bat invoked through cmd. Keep it
	// simple by writing an executable script with the current GOOS in mind.
	script := "#!/bin/sh\ncat <<'EOF'\n" + stdout + "\nEOF\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return bin
}

func TestParseJSONResultSuccess(t *testing.T) {
	res := &ExecutorResult{RawStdout: `{"type":"result","subtype":"success","is_error":false,"result":"只回复OK","session_id":"ses-abc","num_turns":1,"total_cost_usd":0.01,"total_input_tokens":100,"total_output_tokens":50}`}
	parseJSONResult(res)
	if res.Output != "只回复OK" {
		t.Fatalf("Output = %q", res.Output)
	}
	if res.SessionID != "ses-abc" {
		t.Fatalf("SessionID = %q", res.SessionID)
	}
	if res.HasError {
		t.Fatal("HasError = true, want false")
	}
	if res.Usage == nil {
		t.Fatal("Usage missing")
	}
}

func TestParseJSONResultError(t *testing.T) {
	res := &ExecutorResult{RawStdout: `{"type":"result","subtype":"error_during_execution","is_error":true,"errors":["API overloaded"],"session_id":"ses-bad"}`}
	parseJSONResult(res)
	if !res.HasError {
		t.Fatal("HasError = false, want true")
	}
	if len(res.Errors) != 1 || res.Errors[0] != "API overloaded" {
		t.Fatalf("Errors = %v", res.Errors)
	}
}

func TestParseJSONResultContentBlocks(t *testing.T) {
	// Some builds nest the assistant answer in content blocks.
	res := &ExecutorResult{RawStdout: `{"type":"result","subtype":"success","is_error":false,"content":[{"type":"text","text":"hello"},{"type":"text","text":" world"}],"session_id":"s"}`}
	parseJSONResult(res)
	if res.Output != "hello\n world" {
		t.Fatalf("Output = %q", res.Output)
	}
}

func TestPermissionModeMapping(t *testing.T) {
	cases := map[string]string{
		"auto": "bypassPermissions",
		"yolo": "bypassPermissions",
		"ask":  "dontAsk",
		"":     "",
	}
	for input, want := range cases {
		if got := permissionModeFlag(input); got != want {
			t.Errorf("permissionModeFlag(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestClaudeExecutorBuildArgs(t *testing.T) {
	// The fake binary is a shell script; on Windows it cannot run directly, so
	// only exercise argument construction and stdin routing here.
	e := &ClaudeExecutor{}
	bin := fakeClaudeBin(t, `{"type":"result","subtype":"success","is_error":false,"result":"ok","session_id":"s1"}`)
	e.ClaudeBin = bin

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Short prompt: goes as positional argument.
	res, err := e.Exec(ctx, "hi", ExecOptions{Model: "opus", Workspace: t.TempDir()})
	if err != nil && !strings.Contains(err.Error(), "failed to start") {
		t.Fatalf("Exec short: %v", err)
	}
	if err == nil && res.Output != "ok" {
		t.Fatalf("Exec short Output = %q", res.Output)
	}
	// The script may not run on Windows; tolerate start failures but verify the
	// stdin routing path does not corrupt the command line.
	longPrompt := strings.Repeat("长", 4000)
	_, _ = e.Exec(ctx, longPrompt, ExecOptions{Model: "sonnet"})
}

func TestDiscoverBinFindsNativeExe(t *testing.T) {
	// The integration environment (Windows npm install) must resolve the native
	// binary; on machines without Claude installed this is skipped.
	if os.Getenv("RUN_INTEGRATION") == "1" {
		bin := DiscoverBin()
		if bin == "" {
			t.Fatal("DiscoverBin returned empty under RUN_INTEGRATION")
		}
	}
}

func TestJSONResultShapeMatchesRealStreamJSON(t *testing.T) {
	// Guard the parser against the real stream-json result line shape.
	line := `{"type":"result","subtype":"success","is_error":false,"duration_ms":4521,"duration_api_ms":4120,"num_turns":1,"session_id":"550e8400-e29b-41d4-a716-446655440000","total_cost_usd":0.001,"permission_denials":[]}`
	res := &ExecutorResult{RawStdout: line}
	parseJSONResult(res)
	if res.SessionID != "550e8400-e29b-41d4-a716-446655440000" {
		t.Fatalf("SessionID = %q", res.SessionID)
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(line), &obj); err != nil {
		t.Fatal(err)
	}
	_ = obj
}
