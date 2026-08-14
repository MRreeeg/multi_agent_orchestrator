package dsh

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writePreset(t *testing.T, root, id, meta string) string {
	t.Helper()
	dir := filepath.Join(root, id)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if meta != "" {
		if err := os.WriteFile(filepath.Join(dir, "preset.yml"), []byte(meta), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestListAgentPresetsIn(t *testing.T) {
	root := t.TempDir()
	writePreset(t, root, "frontend-analyst", "name: 前端分析师 · 管家\ndescription: 管家式前端分析\n")
	writePreset(t, root, "reviewer", "name: 审查者\ndescription: 只读审查\n")
	// A preset without preset.yml still lists under its id.
	writePreset(t, root, "bare", "")
	// A stray file must not be listed as a preset.
	if err := os.WriteFile(filepath.Join(root, "readme.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	got := ListAgentPresetsIn(root)
	if len(got) != 3 {
		t.Fatalf("presets = %d, want 3: %+v", len(got), got)
	}
	byID := map[string]AgentPresetInfo{}
	for _, p := range got {
		byID[p.ID] = p
	}
	if byID["frontend-analyst"].Name != "前端分析师 · 管家" {
		t.Errorf("frontend-analyst name = %q", byID["frontend-analyst"].Name)
	}
	if byID["bare"].Name != "bare" {
		t.Errorf("bare name fallback = %q, want id", byID["bare"].Name)
	}
	if byID["frontend-analyst"].HasPatch {
		t.Error("frontend-analyst has no patch yet, HasPatch should be false")
	}
}

func TestListAgentPresetsInIncludesPatch(t *testing.T) {
	root := t.TempDir()
	dir := writePreset(t, root, "executor", "name: 执行者\n")
	if err := os.WriteFile(filepath.Join(dir, "headless.patch.yml"), []byte("- id: system-prompt\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	got := ListAgentPresetsIn(root)
	if len(got) != 1 || !got[0].HasPatch {
		t.Fatalf("presets = %+v, want one with HasPatch", got)
	}
	if got[0].PatchPath != filepath.Join(dir, "headless.patch.yml") {
		t.Errorf("PatchPath = %q", got[0].PatchPath)
	}
}

func TestListAgentPresetsEmptyRoot(t *testing.T) {
	if got := ListAgentPresetsIn(t.TempDir()); len(got) != 0 {
		t.Fatalf("empty root presets = %+v, want none", got)
	}
	if got := ListAgentPresetsIn(filepath.Join(t.TempDir(), "does-not-exist")); len(got) != 0 {
		t.Fatalf("missing root presets = %+v, want none", got)
	}
}

func TestResolvePresetPatch(t *testing.T) {
	// root must BE the .agent-presets directory so that its parent is the
	// harness home ResolvePresetPatch looks under.
	root := filepath.Join(t.TempDir(), ".agent-presets")
	dir := writePreset(t, root, "architect", "name: 架构师\n")
	writePreset(t, root, "reviewer", "name: 审查者\n")
	if err := os.WriteFile(filepath.Join(dir, "headless.patch.yml"), []byte("- id: system-prompt\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	home := filepath.Dir(root) // root == <home>/.agent-presets
	// Empty id resolves to nothing (stock persona).
	if p, err := ResolvePresetPatch("", home); err != nil || p != "" {
		t.Fatalf("empty id = (%q, %v), want empty", p, err)
	}
	p, err := ResolvePresetPatch("architect", home)
	if err != nil {
		t.Fatalf("architect: %v", err)
	}
	if p != filepath.Join(dir, "headless.patch.yml") {
		t.Errorf("architect patch = %q", p)
	}
	// Preset exists but has no patch → loud error.
	if _, err := ResolvePresetPatch("reviewer", home); err == nil {
		t.Fatal("reviewer without patch should error")
	} else if !strings.Contains(err.Error(), "headless.patch.yml") {
		t.Errorf("reviewer error = %v, want patch hint", err)
	}
	// Unknown preset → loud error naming the root.
	if _, err := ResolvePresetPatch("nope", home); err == nil {
		t.Fatal("unknown preset should error")
	} else if !strings.Contains(err.Error(), "not found") {
		t.Errorf("unknown error = %v, want not-found hint", err)
	}
}
