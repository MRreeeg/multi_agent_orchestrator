package serve

import (
	"strings"
	"testing"
)

// 管家分析提示词三模式测试：
// - 默认/requirement 模式必须包含动态选型决策树，且不再出现写死的 "reasonix or mimo"
// - chat 模式必须是纯对话提示词（禁生成流水线），且不注入能力清单（提前返回）
func TestBuildAnalysisPromptRequirementMode(t *testing.T) {
	p := buildAnalysisPrompt("zh", "[skill catalog]", "[capability]", "[history]", "requirement")
	for _, want := range []string{
		"执行器与模型选型",
		"禁止所有步骤固定同一个执行器",
		"codex 或 claude 执行器",
		"dsh 执行器",
		"[skill catalog]",
		"[capability]",
		"[history]",
	} {
		if !strings.Contains(p, want) {
			t.Fatalf("requirement prompt missing %q", want)
		}
	}
	if strings.Contains(p, `"executor": "reasonix or mimo"`) {
		t.Fatal("requirement prompt still hardcodes executor choices")
	}
}

func TestBuildAnalysisPromptDefaultIsRequirement(t *testing.T) {
	def := buildAnalysisPrompt("zh", "", "", "", "")
	req := buildAnalysisPrompt("zh", "", "", "", "requirement")
	if def != req {
		t.Fatal("empty mode must behave as requirement")
	}
}

func TestBuildAnalysisPromptChatMode(t *testing.T) {
	p := buildAnalysisPrompt("zh", "[skill catalog]", "[capability]", "[history]", "chat")
	for _, want := range []string{"闲聊", `"steps":[]`, "[history]"} {
		if !strings.Contains(p, want) {
			t.Fatalf("chat prompt missing %q", want)
		}
	}
	// chat 提前返回：不拼能力清单与技能目录。
	if strings.Contains(p, "[capability]") || strings.Contains(p, "[skill catalog]") {
		t.Fatal("chat prompt should not embed capability/skill catalog")
	}
	if !strings.Contains(p, `"loopConfig"`) {
		t.Fatal("chat prompt JSON schema must keep loopConfig key for parser compatibility")
	}
}

func TestBuildAnalysisPromptChatModeEnglish(t *testing.T) {
	p := buildAnalysisPrompt("en", "", "", "[history]", "chat")
	if !strings.Contains(p, "CHAT mode") || !strings.Contains(p, "[history]") {
		t.Fatal("english chat prompt malformed")
	}
}
