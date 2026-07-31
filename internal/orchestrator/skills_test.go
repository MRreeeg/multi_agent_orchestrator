package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

func TestScanSkillRootIncludesWhitelistedSystemReviewer(t *testing.T) {
	root := t.TempDir()
	mustWrite := func(path, content string) {
		t.Helper()
		if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content), 0644); err != nil {
			t.Fatal(err)
		}
	}
	mustWrite(filepath.Join(root, ".system", "review-agent", "SKILL.md"), "---\ndescription: read only review\n---\n# Review")
	mustWrite(filepath.Join(root, ".system", "private-internal", "SKILL.md"), "# Private")
	mustWrite(filepath.Join(root, "executing-plans", "SKILL.md"), "# Plans")

	seen := map[string]SkillInfo{}
	scanSkillRoot(root, seen)
	if got, ok := seen["review-agent"]; !ok || got.Path == "" {
		t.Fatalf("review-agent was not discovered: %+v", seen)
	}
	if _, ok := seen["private-internal"]; ok {
		t.Fatalf("private system skill leaked into catalog: %+v", seen)
	}
	if _, ok := seen["executing-plans"]; !ok {
		t.Fatalf("regular skill was not discovered: %+v", seen)
	}
}

func TestSelectSkillCanonicalizesReviewerAlias(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, ".system", "review-agent", "SKILL.md")
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("# Review"), 0644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("REASONIX_SKILL_DIR", root)

	got := SelectSkill("检查代码", NodeReviewer, "loop-review", "review")
	if got != "review-agent" {
		t.Fatalf("SelectSkill reviewer alias = %q, want review-agent", got)
	}
}
