package dsh

import (
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// installMu serializes EnsureAgentPackInstalled: /selfcheck and /dsh-presets
// can fire concurrently at startup, and the copy steps must not race on the
// same destination directories.
var installMu sync.Mutex

// PackInstallReport describes the outcome of auto-installing the bundled
// dsh-agent-pack (the presets/skills/persona-overlay shipped under
// docs/deepseek-harness/dsh-agent-pack) at self-check time. It lets the
// frontend show "auto-installed from the bundled pack" instead of demanding a
// manual install.ps1 run.
type PackInstallReport struct {
	// Found is whether a bundled pack directory was located.
	Found bool `json:"found"`
	// PackDir is the resolved pack directory (empty when not found).
	PackDir string `json:"packDir,omitempty"`
	// DshHome is the harness home the presets/skills were installed into.
	DshHome string `json:"dshHome,omitempty"`
	// ReasonixSkillRoot is where the pack skills were mirrored for the
	// Reasonix analysis-agent persona enumeration (empty when skipped).
	ReasonixSkillRoot string `json:"reasonixSkillRoot,omitempty"`
	// InstalledPresets are preset ids freshly copied from the pack.
	InstalledPresets []string `json:"installedPresets,omitempty"`
	// SkippedPresets are preset ids already present locally (not overwritten).
	SkippedPresets []string `json:"skippedPresets,omitempty"`
	// SkillsCopied are skill names copied to the DSH skill root.
	SkillsCopied []string `json:"skillsCopied,omitempty"`
	// PersonaMerged reports whether the persona overlay was appended to
	// $DSH_HOME/cordis.patch.yml (false when it was already present).
	PersonaMerged bool `json:"personaMerged,omitempty"`
	// Errors accumulates non-fatal failures; self-check never 500s on them.
	Errors []string `json:"errors,omitempty"`
}

// FindAgentPackDir locates the bundled dsh-agent-pack directory. It honors an
// explicit REASONIX_DSH_PACK_DIR override, then searches a few plausible
// locations relative to the working directory and the executable (walking up
// to four levels), matching the repo layout `docs/deepseek-harness/
// dsh-agent-pack`. An empty result means no pack is available on this machine
// (e.g. a packaged install without the docs tree).
func FindAgentPackDir() string {
	if v := strings.TrimSpace(os.Getenv("REASONIX_DSH_PACK_DIR")); v != "" {
		if isAgentPackDir(v) {
			return filepath.Clean(v)
		}
	}
	var bases []string
	if cwd, err := os.Getwd(); err == nil {
		bases = append(bases, cwd)
	}
	if exe, err := os.Executable(); err == nil {
		bases = append(bases, filepath.Dir(exe))
	}
	seen := map[string]bool{}
	for _, base := range bases {
		for depth := 0; depth <= 4; depth++ {
			for _, rel := range []string{
				filepath.Join("docs", "deepseek-harness", "dsh-agent-pack"),
				filepath.Join("deepseek-harness", "dsh-agent-pack"),
				"dsh-agent-pack",
			} {
				cand := filepath.Clean(filepath.Join(base, rel))
				if seen[cand] {
					continue
				}
				seen[cand] = true
				if isAgentPackDir(cand) {
					return cand
				}
			}
			base = filepath.Dir(base)
		}
	}
	return ""
}

// isAgentPackDir reports whether dir looks like a dsh-agent-pack: a directory
// containing both `presets/` and `skills/` subdirectories.
func isAgentPackDir(dir string) bool {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return false
	}
	for _, sub := range []string{"presets", "skills"} {
		if fi, statErr := os.Stat(filepath.Join(dir, sub)); statErr != nil || !fi.IsDir() {
			return false
		}
	}
	return true
}

// EnsureAgentPackInstalled idempotently installs the bundled dsh-agent-pack
// into the local harness home, mirroring `install.ps1 -Mode user`:
//   - presets   → $DSH_HOME/.agent-presets/<id>/   (never overwrites existing)
//   - skills    → $DSH_HOME/skills/  +  Reasonix skill root
//   - persona   → merged into $DSH_HOME/cordis.patch.yml (marker-guarded)
//
// It is a no-op when no pack is found. Every failure is recorded in the
// report rather than raised, so self-check stays read-mostly and never 500s.
func EnsureAgentPackInstalled() PackInstallReport {
	installMu.Lock()
	defer installMu.Unlock()
	report := PackInstallReport{}
	packDir := FindAgentPackDir()
	if packDir == "" {
		return report
	}
	report.Found = true
	report.PackDir = packDir

	home := DshHome()
	if home == "" {
		report.Errors = append(report.Errors, "cannot resolve DSH home (DSH_HOME unset and no user home)")
		return report
	}
	report.DshHome = home

	report.InstalledPresets, report.SkippedPresets = installPresets(packDir, home, &report.Errors)
	report.SkillsCopied = installSkills(packDir, home, &report.Errors)
	report.ReasonixSkillRoot = installReasonixSkills(packDir, &report.Errors)
	report.PersonaMerged = mergePersonaOverlay(packDir, home, &report.Errors)

	sort.Strings(report.InstalledPresets)
	sort.Strings(report.SkippedPresets)
	sort.Strings(report.SkillsCopied)
	return report
}

// installPresets copies each pack presets/<id>/ directory into
// $DSH_HOME/.agent-presets/<id>/ when that preset id is not already present.
func installPresets(packDir, home string, errs *[]string) (installed, skipped []string) {
	src := filepath.Join(packDir, "presets")
	entries, err := os.ReadDir(src)
	if err != nil {
		*errs = append(*errs, "read pack presets: "+err.Error())
		return installed, skipped
	}
	dest := filepath.Join(home, ".agent-presets")
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		target := filepath.Join(dest, id)
		if _, statErr := os.Stat(filepath.Join(target, "preset.yml")); statErr == nil {
			skipped = append(skipped, id)
			continue
		}
		if err := copyTree(filepath.Join(src, id), target); err != nil {
			*errs = append(*errs, "install preset "+id+": "+err.Error())
			continue
		}
		installed = append(installed, id)
	}
	return installed, skipped
}

// installSkills copies each pack skills/<name>/ directory into
// $DSH_HOME/skills/<name>/ when the skill is not already present there.
func installSkills(packDir, home string, errs *[]string) (copied []string) {
	src := filepath.Join(packDir, "skills")
	entries, err := os.ReadDir(src)
	if err != nil {
		*errs = append(*errs, "read pack skills: "+err.Error())
		return copied
	}
	dest := filepath.Join(home, "skills")
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if _, statErr := os.Stat(filepath.Join(dest, name, "SKILL.md")); statErr == nil {
			continue
		}
		if err := copyTree(filepath.Join(src, name), filepath.Join(dest, name)); err != nil {
			*errs = append(*errs, "install skill "+name+": "+err.Error())
			continue
		}
		copied = append(copied, name)
	}
	return copied
}

// installReasonixSkills mirrors the pack skills into the Reasonix skill root so
// the analysis-persona enumeration (subagent profiles) sees them too. It
// follows the same resolution install.ps1 uses: REASONIX_SKILL_DIR override or
// <home>/.config/reasonix/skills. Returns the resolved root ("" when none).
func installReasonixSkills(packDir string, errs *[]string) string {
	root := reasonixSkillRoot()
	if root == "" {
		return ""
	}
	src := filepath.Join(packDir, "skills")
	entries, err := os.ReadDir(src)
	if err != nil {
		*errs = append(*errs, "read pack skills for reasonix: "+err.Error())
		return root
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		if _, statErr := os.Stat(filepath.Join(root, name, "SKILL.md")); statErr == nil {
			continue
		}
		if err := copyTree(filepath.Join(src, name), filepath.Join(root, name)); err != nil {
			*errs = append(*errs, "install reasonix skill "+name+": "+err.Error())
		}
	}
	return root
}

// reasonixSkillRoot resolves the Reasonix skill root, mirroring install.ps1.
func reasonixSkillRoot() string {
	if v := strings.TrimSpace(os.Getenv("REASONIX_SKILL_DIR")); v != "" {
		return filepath.Clean(v)
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".config", "reasonix", "skills")
	}
	return ""
}

// mergePersonaOverlay appends the pack's persona overlay
// (cordis.patch.yml) to $DSH_HOME/cordis.patch.yml once. It is guarded by the
// pack's own `# dsh-agent-pack` header comment so repeated self-checks do not
// stack duplicate persona rows. A missing overlay file is not an error.
func mergePersonaOverlay(packDir, home string, errs *[]string) bool {
	overlay := filepath.Join(packDir, "cordis.patch.yml")
	data, err := os.ReadFile(overlay)
	if err != nil {
		return false
	}
	target := filepath.Join(home, "cordis.patch.yml")
	if existing, readErr := os.ReadFile(target); readErr == nil && strings.Contains(string(existing), "# dsh-agent-pack") {
		return false
	}
	if err := os.MkdirAll(home, 0o755); err != nil {
		*errs = append(*errs, "mkdir dsh home: "+err.Error())
		return false
	}
	f, err := os.OpenFile(target, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		*errs = append(*errs, "open persona overlay target: "+err.Error())
		return false
	}
	defer f.Close()
	if _, err := f.Write(data); err != nil {
		*errs = append(*errs, "write persona overlay: "+err.Error())
		return false
	}
	return true
}

// copyTree copies a source directory tree to a destination, creating parents
// as needed. It returns the first error encountered.
func copyTree(src, dest string) error {
	return filepath.WalkDir(src, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return os.MkdirAll(dest, 0o755)
		}
		target := filepath.Join(dest, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		if d.Type()&os.ModeSymlink != 0 {
			return copySymlink(path, target)
		}
		return copyFile(path, target)
	})
}

// copyFile copies one regular file, creating its parent directory.
func copyFile(src, dest string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dest, data, 0o644)
}

// copySymlink re-creates a symlink at dest pointing at the same target as src.
func copySymlink(src, dest string) error {
	target, err := os.Readlink(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	return os.Symlink(target, dest)
}
