package orchestrator

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

type SkillInfo struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Path        string `json:"path,omitempty"`
}

const maxSkillPromptBytes = 32 * 1024

func skillSearchRoots() []string {
	home, _ := os.UserHomeDir()
	roots := []string{
		filepath.Join(home, ".codex", "skills"),
		filepath.Join(home, ".local", "share", "mimocode", "builtin_skills"),
	}
	for _, envName := range []string{"REASONIX_SKILL_DIR", "REASONIX_SKILLPACK"} {
		if v := strings.TrimSpace(os.Getenv(envName)); v != "" {
			roots = append(roots, v)
		}
	}
	// Standard Reasonix skill path under user config dir
	roots = append(roots, filepath.Join(home, ".config", "reasonix", "skills"))
	return uniqueStrings(roots)
}

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = filepath.Clean(v)
		if v == "." || v == "" {
			continue
		}
		key := strings.ToLower(v)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, v)
	}
	return out
}

func skillFileCandidates(root string) []string {
	candidates := []string{root}
	if entries, err := os.ReadDir(root); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				candidates = append(candidates, filepath.Join(root, entry.Name(), "skills"))
			}
		}
	}
	return candidates
}

func scanSkillRoot(root string, seen map[string]SkillInfo) {
	for _, dir := range skillFileCandidates(root) {
		scanSkillDirectory(dir, seen)
	}

	// Codex keeps a small set of platform-owned skills below .system rather
	// than alongside user/community skills. Do not expose every internal
	// implementation skill to generated pipelines, but explicitly include the
	// read-only reviewer contract because reviewer nodes depend on it.
	scanSkillDirectory(filepath.Join(root, ".system"), seen, "review-agent")
}

func scanSkillDirectory(dir string, seen map[string]SkillInfo, allowedSystemNames ...string) {
	allowed := make(map[string]struct{}, len(allowedSystemNames))
	for _, name := range allowedSystemNames {
		allowed[strings.ToLower(strings.TrimSpace(name))] = struct{}{}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if len(allowed) > 0 {
			if _, ok := allowed[strings.ToLower(entry.Name())]; !ok {
				continue
			}
		}
		path := filepath.Join(dir, entry.Name(), "SKILL.md")
		if _, err := os.Stat(path); err != nil {
			continue
		}
		info := SkillInfo{Name: entry.Name(), Path: path, Description: readSkillDescription(path)}
		key := strings.ToLower(info.Name)
		if _, exists := seen[key]; !exists {
			seen[key] = info
		}
	}
}

func readSkillDescription(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	inFrontmatter := false
	var heading string
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "---" {
			inFrontmatter = !inFrontmatter
			continue
		}
		if inFrontmatter && strings.HasPrefix(strings.ToLower(line), "description:") {
			return strings.Trim(strings.TrimSpace(line[len("description:"):]), "\"'")
		}
		if heading == "" && strings.HasPrefix(line, "#") {
			heading = strings.TrimSpace(strings.TrimLeft(line, "#"))
		}
	}
	return heading
}

func ListAvailableSkills() []SkillInfo {
	seen := make(map[string]SkillInfo)
	for _, root := range skillSearchRoots() {
		scanSkillRoot(root, seen)
	}
	out := make([]SkillInfo, 0, len(seen))
	for _, info := range seen {
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func LoadSkill(name string) (SkillInfo, string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return SkillInfo{}, "", fmt.Errorf("skill name is empty")
	}
	for _, info := range ListAvailableSkills() {
		if strings.EqualFold(info.Name, name) {
			data, err := os.ReadFile(info.Path)
			if err != nil {
				return SkillInfo{}, "", err
			}
			content := string(data)
			if len(content) > maxSkillPromptBytes {
				content = content[:maxSkillPromptBytes] + "\n\n[Skill 内容已截断，避免占用过多上下文]"
			}
			return info, content, nil
		}
	}
	return SkillInfo{}, "", fmt.Errorf("skill %q not found", name)
}

func skillMatches(info SkillInfo, terms ...string) bool {
	hay := strings.ToLower(info.Name + " " + info.Description)
	for _, term := range terms {
		if strings.Contains(hay, strings.ToLower(term)) {
			return true
		}
	}
	return false
}

func SelectSkill(task string, nodeType NodeType, phase, explicit string) string {
	catalog := ListAvailableSkills()
	byName := make(map[string]SkillInfo, len(catalog))
	for _, info := range catalog {
		byName[strings.ToLower(info.Name)] = info
	}
	if info, ok := byName[strings.ToLower(strings.TrimSpace(explicit))]; ok {
		return info.Name
	}
	text := strings.ToLower(task + " " + phase)
	preferred := make([]string, 0, 8)
	switch nodeType {
	case NodeReviewer:
		// /loop is a scheduler skill, not a code-review skill. Never route a
		// reviewer to it: doing so can make the reviewer schedule work or
		// behave like an autonomous loop instead of returning one judgment.
		preferred = append(preferred, "review-agent", "code-review", "code-reviewer")
	case NodeArchitect:
		preferred = append(preferred, "brainstorming", "design-blueprint", "json-canvas", "executing-plans")
	default:
		preferred = append(preferred, "executing-plans", "playwright", "frontend-design", "pdf", "modern-python-toolchain")
	}
	if strings.Contains(text, "论文") || strings.Contains(text, "学术") || strings.Contains(text, "文献") || strings.Contains(text, "research") {
		preferred = append([]string{"deep-research", "nature-academic-search", "academic-research-suite"}, preferred...)
	}
	for _, name := range preferred {
		if info, ok := byName[strings.ToLower(name)]; ok {
			return info.Name
		}
	}
	for _, info := range catalog {
		if nodeType == NodeReviewer && strings.Contains(text, "审") && skillMatches(info, "review-agent", "code review", "code-reviewer") {
			return info.Name
		}
	}
	return ""
}

func SkillCatalogSummary() string {
	catalog := ListAvailableSkills()
	if len(catalog) == 0 {
		return "（当前未发现可用 Skill；skill 字段留空）"
	}
	var b strings.Builder
	for _, info := range catalog {
		desc := info.Description
		if len(desc) > 180 {
			desc = desc[:180]
		}
		fmt.Fprintf(&b, "- %s: %s\n", info.Name, desc)
	}
	return b.String()
}
