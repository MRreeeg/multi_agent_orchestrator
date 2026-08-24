package dsh

import (
	"strings"
	"testing"
)

func TestUtf16Len(t *testing.T) {
	tests := []struct {
		in   string
		want int
	}{
		{"", 0},
		{"ascii", 5},
		// CJK BMP runes cost exactly one UTF-16 unit each.
		{"需求文档", 4},
		// Astral runes (emoji) cost two units (surrogate pair).
		{"a🙂b", 4},
	}
	for _, tt := range tests {
		if got := utf16Len(tt.in); got != tt.want {
			t.Errorf("utf16Len(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestTruncateToUTF16Units(t *testing.T) {
	s := "需求" + strings.Repeat("x", 100)
	got := truncateToUTF16Units(s, 10)
	if utf16Len(got) > 10 {
		t.Fatalf("truncated length %d exceeds budget 10", utf16Len(got))
	}
	if !strings.HasPrefix(s, got) {
		t.Fatal("head truncation must preserve a prefix")
	}
	if full := truncateToUTF16Units("短文本", 100); full != "短文本" {
		t.Fatal("short input must be returned unchanged")
	}
}

func TestTruncateTailToUTF16Units(t *testing.T) {
	s := strings.Repeat("x", 100) + "结尾标记"
	got := truncateTailToUTF16Units(s, 10)
	if utf16Len(got) > 10 {
		t.Fatalf("truncated length %d exceeds budget 10", utf16Len(got))
	}
	if !strings.HasSuffix(s, got) {
		t.Fatal("tail truncation must preserve a suffix")
	}
}

func TestLongPromptIsTruncatedToCommandLineBudget(t *testing.T) {
	// Simulate the analysis path: a prompt far beyond the 32K command line.
	prompt := "你是分析助手。请只输出 JSON。\n" + strings.Repeat("里程碑一收尾与阶段五画线轨迹跟随。", 3000)
	if utf16Len(prompt) <= maxDshPromptUnits {
		t.Fatalf("test prompt too short: %d units", utf16Len(prompt))
	}
	head := truncateToUTF16Units(prompt, maxDshPromptUnits*40/100)
	tail := truncateTailToUTF16Units(prompt, maxDshPromptUnits*55/100)
	dropped := utf16Len(prompt) - utf16Len(head) - utf16Len(tail)
	rebuilt := head + "[截断]" + tail
	if utf16Len(rebuilt) > maxDshPromptUnits+utf16Len("[截断]") {
		t.Fatalf("rebuilt prompt %d units must stay within budget", utf16Len(rebuilt))
	}
	if dropped <= 0 {
		t.Fatal("expected a positive dropped-character count")
	}
	if !strings.HasPrefix(rebuilt, "你是分析助手") {
		t.Fatal("head (instructions/schema) must be preserved")
	}
}
