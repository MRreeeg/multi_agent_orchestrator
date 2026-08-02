package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
)

// codexProfileOverrides reads $CODEX_HOME/<profile>.config.toml (the codex
// --profile overlay) and converts it into `-c key=value` overrides suitable
// for `codex app-server`, which does not accept --profile but does accept
// nested -c overrides such as `model_providers.custom.base_url=...`.
//
// The profile files are machine-local (they carry API keys) and are generated
// from cc-switch providers by scripts/sync-codex-profiles.ps1; keys never live
// in the repository. serve mode therefore uses exactly the same configuration
// source as run mode (`codex --profile`).
func codexProfileOverrides(profile string) []string {
	if profile == "" {
		return nil
	}
	dir := codexHomeDir()
	if dir == "" {
		return nil
	}
	data, err := os.ReadFile(filepath.Join(dir, profile+".config.toml"))
	if err != nil {
		return nil
	}
	section := ""
	var out []string
	for _, raw := range strings.Split(string(data), "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			section = strings.TrimSpace(strings.Trim(line, "[]"))
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" {
			continue
		}
		// Strip TOML quotes from string values ("sk-..." -> sk-...).
		value = strings.Trim(value, `"'`)
		if section != "" {
			key = section + "." + key
		}
		out = append(out, key+"="+value)
	}
	return out
}

// codexHomeDir returns $CODEX_HOME or the default ~/.codex.
func codexHomeDir() string {
	if dir := strings.TrimSpace(os.Getenv("CODEX_HOME")); dir != "" {
		return dir
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".codex")
}
