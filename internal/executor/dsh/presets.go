package dsh

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// AgentPresetInfo is the public projection of one locally authored DSH agent
// preset (a directory under $DSH_HOME/.agent-presets with a preset.yml and an
// agent.cordis.yml). Reasonix imports these at self-check time so dsh nodes
// can pick a customized agent instead of the stock persona.
type AgentPresetInfo struct {
	// ID is the preset id, which is also its directory name.
	ID string `json:"id"`
	// Name is the display name from preset.yml (falls back to ID).
	Name string `json:"name"`
	// Description is the one-line description from preset.yml.
	Description string `json:"description,omitempty"`
	// Dir is the preset directory's absolute path.
	Dir string `json:"dir"`
	// PatchPath is the absolute path of the headless.patch.yml that flattens
	// this preset onto a one-shot `dsh --profile headless` invocation
	// (persona override plus tool-catalog pruning). Empty when the file is
	// missing — such a preset still runs in DSH Web but is not selectable
	// for orchestration nodes.
	PatchPath string `json:"patchPath,omitempty"`
	// HasPatch reports whether the headless patch exists.
	HasPatch bool `json:"hasPatch"`
}

// presetMeta is the subset of preset.yml this package reads.
type presetMeta struct {
	Name        string `yaml:"name"`
	Description string `yaml:"description"`
}

// DshHome returns the harness home directory: the DSH_HOME environment
// override when set, otherwise <user home>/.dsh (the harness default).
func DshHome() string {
	if env := strings.TrimSpace(os.Getenv("DSH_HOME")); env != "" {
		return env
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".dsh")
	}
	return ""
}

// UserPresetRoot returns the locally authored agent-preset root
// ($DSH_HOME/.agent-presets), the directory Reasonix imports at self-check.
func UserPresetRoot() string {
	home := DshHome()
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".agent-presets")
}

// ListAgentPresets scans the user preset root and returns every locally
// authored preset with its display metadata and headless patch path.
func ListAgentPresets() []AgentPresetInfo {
	return ListAgentPresetsIn(UserPresetRoot())
}

// ListAgentPresetsIn scans one preset root. Exported as a seam for tests.
func ListAgentPresetsIn(root string) []AgentPresetInfo {
	if root == "" {
		return []AgentPresetInfo{}
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return []AgentPresetInfo{}
	}
	out := make([]AgentPresetInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		id := entry.Name()
		dir := filepath.Join(root, id)
		info := AgentPresetInfo{ID: id, Name: id, Dir: dir}
		if meta, err := readPresetMeta(filepath.Join(dir, "preset.yml")); err == nil {
			if strings.TrimSpace(meta.Name) != "" {
				info.Name = strings.TrimSpace(meta.Name)
			}
			info.Description = strings.TrimSpace(meta.Description)
		}
		patch := filepath.Join(dir, "headless.patch.yml")
		if stat, err := os.Stat(patch); err == nil && !stat.IsDir() {
			info.PatchPath = patch
			info.HasPatch = true
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ResolvePresetPatch returns the headless.patch.yml of a preset id, looked up
// in the user preset root (optionally under an explicit harness home, which
// mirrors the DSH_HOME the child process will run with). It errors when the
// preset is unknown or has no headless patch, so a misconfigured node fails
// loudly instead of silently running the stock persona.
func ResolvePresetPatch(id string, dshHome string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", nil
	}
	root := UserPresetRoot()
	if strings.TrimSpace(dshHome) != "" {
		root = filepath.Join(strings.TrimSpace(dshHome), ".agent-presets")
	}
	patch := filepath.Join(root, id, "headless.patch.yml")
	if stat, err := os.Stat(patch); err != nil || stat.IsDir() {
		// Distinguish "unknown preset" from "preset without a patch" for a
		// clearer node error message.
		dir := filepath.Join(root, id)
		if info, dirErr := os.Stat(dir); dirErr != nil || !info.IsDir() {
			return "", &PresetNotFoundError{ID: id, Root: root}
		}
		return "", &PresetPatchMissingError{ID: id, Dir: dir}
	}
	return patch, nil
}

// PresetNotFoundError reports a dsh node naming a preset that is not
// installed under the harness home.
type PresetNotFoundError struct {
	ID   string
	Root string
}

func (e *PresetNotFoundError) Error() string {
	return "dsh agent preset " + strconvQuote(e.ID) + " not found under " + e.Root + "; install the preset into $DSH_HOME/.agent-presets or clear the node's 客制化 Agent selection"
}

// PresetPatchMissingError reports a preset that exists but cannot drive a
// headless node (no headless.patch.yml).
type PresetPatchMissingError struct {
	ID  string
	Dir string
}

func (e *PresetPatchMissingError) Error() string {
	return "dsh agent preset " + strconvQuote(e.ID) + " has no headless.patch.yml under " + e.Dir + "; the preset cannot be used by an orchestration node"
}

func strconvQuote(s string) string {
	return "\"" + s + "\""
}

// readPresetMeta reads the name/description of one preset.yml. A missing or
// malformed file is not an error for the catalog: the preset still exists and
// the ID serves as its display name.
func readPresetMeta(path string) (presetMeta, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return presetMeta{}, err
	}
	var meta presetMeta
	if err := yaml.Unmarshal(raw, &meta); err != nil {
		return presetMeta{}, err
	}
	return meta, nil
}
