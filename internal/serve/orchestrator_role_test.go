package serve

import (
	"strings"
	"testing"
)

func TestEnforceGeneratedRoleBoundary(t *testing.T) {
	cases := []struct {
		name, agent, want string
	}{
		{"architect", "架构师1", "禁止写代码"},
		{"executor", "executor2", "只执行当前轮"},
		{"reviewer", "审查者3", "只审查当前轮"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := enforceGeneratedRoleBoundary(""+tc.agent, "原始职责")
			if !containsText(got, tc.want) {
				t.Fatalf("boundary for %s missing %q: %s", tc.agent, tc.want, got)
			}
			if !containsText(got, "原始职责") {
				t.Fatalf("original role description was lost: %s", got)
			}
		})
	}
}

func containsText(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && strings.Contains(s, sub))
}
