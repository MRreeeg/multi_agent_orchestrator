package dsh

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// makeFakePack builds a minimal dsh-agent-pack tree under dir and returns the
// pack root. presets carry agent.cordis.yml + preset.yml + headless.patch.yml,
// skills carry a SKILL.md, and a persona overlay is written for merge tests.
func makeFakePack(t *testing.T, dir string) string {
	t.Helper()
	pack := filepath.Join(dir, "docs", "deepseek-harness", "dsh-agent-pack")
	for _, id := range []string{"architect", "reviewer"} {
		pd := filepath.Join(pack, "presets", id)
		mustWrite(t, filepath.Join(pd, "preset.yml"), "name: "+id+"\n")
		mustWrite(t, filepath.Join(pd, "agent.cordis.yml"), "- id: system-prompt\n")
		mustWrite(t, filepath.Join(pd, "headless.patch.yml"), "- id: system-prompt\n")
	}
	mustWrite(t, filepath.Join(pack, "skills", "reasonix-architect", "SKILL.md"), "---\ndescription: architect\n---\n")
	mustWrite(t, filepath.Join(pack, "cordis.patch.yml"), "# dsh-agent-pack persona\n- id: system-prompt\n")
	return pack
}

func mustWrite(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestEnsureAgentPackInstalledNoPack(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("REASONIX_DSH_PACK_DIR", filepath.Join(t.TempDir(), "missing"))
	report := EnsureAgentPackInstalled()
	if report.Found {
		t.Fatalf("expected not found, got %+v", report)
	}
}

func TestEnsureAgentPackInstalledFreshHome(t *testing.T) {
	pack := makeFakePack(t, t.TempDir())
	t.Setenv("REASONIX_DSH_PACK_DIR", pack)
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)

	report := EnsureAgentPackInstalled()
	if !report.Found {
		t.Fatalf("expected pack found, got %+v", report)
	}
	if report.PackDir != pack {
		t.Errorf("packDir = %q, want %q", report.PackDir, pack)
	}
	if len(report.InstalledPresets) != 2 {
		t.Errorf("installedPresets = %v, want 2", report.InstalledPresets)
	}
	// Presets copied with headless patch resolvable.
	for _, id := range []string{"architect", "reviewer"} {
		meta := filepath.Join(home, ".agent-presets", id, "preset.yml")
		if _, err := os.Stat(meta); err != nil {
			t.Errorf("preset %s missing: %v", id, err)
		}
		if _, err := os.Stat(filepath.Join(home, ".agent-presets", id, "headless.patch.yml")); err != nil {
			t.Errorf("preset %s headless patch missing: %v", id, err)
		}
	}
	// Skills mirrored into DSH home and Reasonix root.
	if _, err := os.Stat(filepath.Join(home, "skills", "reasonix-architect", "SKILL.md")); err != nil {
		t.Errorf("dsh skill missing: %v", err)
	}
	if len(report.SkillsCopied) != 1 {
		t.Errorf("skillsCopied = %v, want 1", report.SkillsCopied)
	}
	// Persona overlay merged into cordis.patch.yml.
	overlay, err := os.ReadFile(filepath.Join(home, "cordis.patch.yml"))
	if err != nil || !strings.Contains(string(overlay), "dsh-agent-pack persona") {
		t.Errorf("persona overlay not merged: %v", err)
	}
	if !report.PersonaMerged {
		t.Error("personaMerged should be true")
	}
}

func TestEnsureAgentPackInstalledIdempotent(t *testing.T) {
	pack := makeFakePack(t, t.TempDir())
	t.Setenv("REASONIX_DSH_PACK_DIR", pack)
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)

	// User's own preset must not be overwritten.
	mustWrite(t, filepath.Join(home, ".agent-presets", "architect", "preset.yml"), "name: 我自己的\n")
	mustWrite(t, filepath.Join(home, ".agent-presets", "architect", "headless.patch.yml"), "- id: system-prompt\n")

	report := EnsureAgentPackInstalled()
	if len(report.InstalledPresets) != 1 {
		t.Errorf("installedPresets = %v, want only reviewer installed", report.InstalledPresets)
	}
	if len(report.SkippedPresets) != 1 || report.SkippedPresets[0] != "architect" {
		t.Errorf("skippedPresets = %v, want [architect]", report.SkippedPresets)
	}
	// The user's preset.yml is intact.
	got, _ := os.ReadFile(filepath.Join(home, ".agent-presets", "architect", "preset.yml"))
	if string(got) != "name: 我自己的\n" {
		t.Errorf("user preset overwritten: %q", got)
	}
	// Second run must be fully idempotent: nothing installed, no persona dup.
	report2 := EnsureAgentPackInstalled()
	if len(report2.InstalledPresets) != 0 {
		t.Errorf("second run installedPresets = %v, want none", report2.InstalledPresets)
	}
	overlay, _ := os.ReadFile(filepath.Join(home, "cordis.patch.yml"))
	if strings.Count(string(overlay), "dsh-agent-pack persona") != 1 {
		t.Errorf("persona overlay duplicated:\n%s", overlay)
	}
}

func TestEnsureAgentPackInstalledReasonixRootOverride(t *testing.T) {
	pack := makeFakePack(t, t.TempDir())
	t.Setenv("REASONIX_DSH_PACK_DIR", pack)
	home := t.TempDir()
	t.Setenv("DSH_HOME", home)
	skillRoot := t.TempDir()
	t.Setenv("REASONIX_SKILL_DIR", skillRoot)

	report := EnsureAgentPackInstalled()
	if report.ReasonixSkillRoot != skillRoot {
		t.Errorf("reasonixSkillRoot = %q, want %q", report.ReasonixSkillRoot, skillRoot)
	}
	if _, err := os.Stat(filepath.Join(skillRoot, "reasonix-architect", "SKILL.md")); err != nil {
		t.Errorf("reasonix skill missing: %v", err)
	}
}

func TestFindAgentPackDirNoPack(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("REASONIX_DSH_PACK_DIR", "")
	// Temp dirs have no docs/... layout.
	if dir := FindAgentPackDir(); dir != "" {
		t.Fatalf("unexpected pack found at %q", dir)
	}
}

func TestCopyTreeSymlink(t *testing.T) {
	src := t.TempDir()
	dest := filepath.Join(t.TempDir(), "dest")
	mustWrite(t, filepath.Join(src, "a", "f.txt"), "hi")
	if err := os.Symlink(filepath.Join(src, "a"), filepath.Join(src, "link")); err == nil {
		if err := copyTree(src, dest); err != nil {
			t.Fatalf("copyTree: %v", err)
		}
		got, err := os.ReadFile(filepath.Join(dest, "a", "f.txt"))
		if err != nil || string(got) != "hi" {
			t.Errorf("dest file = %q, err = %v", got, err)
		}
	}
}
