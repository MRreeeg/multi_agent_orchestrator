package semantic

import (
	"context"
	"strings"
	"testing"

	"reasonix/internal/designpattern/matcher"
)

// mockClient returns a fixed response for testing.
type mockClient struct {
	response string
	err      error
}

func (m *mockClient) Complete(_ context.Context, _ string) (string, error) {
	return m.response, m.err
}

func TestBuildValidationPrompt(t *testing.T) {
	p := matcher.Pattern{English: "Singleton", Chinese: "单例模式", Category: "creational"}
	prompt := BuildValidationPrompt(p, "type singleton struct{}", PromptConfig{Language: "zh"})
	if len(prompt) < 50 {
		t.Errorf("prompt too short: %d chars", len(prompt))
	}
	if !strings.Contains(prompt, "单例模式") {
		t.Error("prompt should contain Chinese pattern name")
	}
	if !strings.Contains(prompt, "Singleton") {
		t.Error("prompt should contain English pattern name")
	}
}

func TestBuildValidationPromptEnglish(t *testing.T) {
	p := matcher.Pattern{English: "Observer", Chinese: "观察者模式", Category: "behavioural"}
	prompt := BuildValidationPrompt(p, "type subject struct{}", PromptConfig{Language: "en"})
	if !strings.Contains(prompt, "Observer") {
		t.Error("prompt should contain pattern name")
	}
	if strings.Contains(prompt, "请分析") {
		t.Error("English prompt should not contain Chinese")
	}
}

func TestParseResultCorrect(t *testing.T) {
	raw := `{"verdict":"correct","confidence":0.95,"issues":[],"summary":"Correct implementation."}`
	p := matcher.Pattern{English: "Singleton", Chinese: "单例模式"}
	result, err := parseResult(raw, p)
	if err != nil {
		t.Fatalf("parseResult error: %v", err)
	}
	if result.Verdict != VerdictCorrect {
		t.Errorf("got verdict=%q, want correct", result.Verdict)
	}
	if result.Confidence != 0.95 {
		t.Errorf("got confidence=%f, want 0.95", result.Confidence)
	}
	if result.Pattern.English != "Singleton" {
		t.Errorf("got pattern=%q, want Singleton", result.Pattern.English)
	}
}

func TestParseResultIncorrect(t *testing.T) {
	raw := `{"verdict":"incorrect","confidence":0.8,"issues":[{"line":42,"message":"missing private constructor","suggestion":"make constructor private"}],"summary":"Not truly singleton."}`
	p := matcher.Pattern{English: "Singleton"}
	result, err := parseResult(raw, p)
	if err != nil {
		t.Fatalf("parseResult error: %v", err)
	}
	if result.Verdict != VerdictIncorrect {
		t.Errorf("got verdict=%q, want incorrect", result.Verdict)
	}
	if len(result.Issues) != 1 {
		t.Fatalf("expected 1 issue, got %d", len(result.Issues))
	}
	if result.Issues[0].Line != 42 {
		t.Errorf("got line=%d, want 42", result.Issues[0].Line)
	}
}

func TestParseResultInvalidJSON(t *testing.T) {
	raw := `This is not JSON at all.`
	p := matcher.Pattern{English: "Test"}
	result, err := parseResult(raw, p)
	if err != nil {
		t.Fatalf("parseResult error: %v", err)
	}
	// Should return uncertain, not fail.
	if result.Verdict != VerdictUncertain {
		t.Errorf("got verdict=%q, want uncertain", result.Verdict)
	}
}

func TestExtractJSONFromMarkdown(t *testing.T) {
	raw := "Here's my analysis:\n```json\n{\"verdict\":\"correct\",\"confidence\":0.9,\"issues\":[],\"summary\":\"ok\"}\n```\n"
	result, err := parseResult(raw, matcher.Pattern{English: "Test"})
	if err != nil {
		t.Fatalf("parseResult error: %v", err)
	}
	if result.Verdict != VerdictCorrect {
		t.Errorf("got verdict=%q, want correct", result.Verdict)
	}
	if result.Confidence != 0.9 {
		t.Errorf("got confidence=%f, want 0.9", result.Confidence)
	}
}

func TestAnalyseWithMock(t *testing.T) {
	client := &mockClient{
		response: `{"verdict":"correct","confidence":0.85,"issues":[],"summary":"Looks good."}`,
	}
	p := matcher.Pattern{English: "Strategy", Chinese: "策略模式", Category: "behavioural"}
	result, err := Analyse(context.Background(), client, p, "type context struct{}", PromptConfig{Language: "en"})
	if err != nil {
		t.Fatalf("Analyse error: %v", err)
	}
	if result.Verdict != VerdictCorrect {
		t.Errorf("got verdict=%q, want correct", result.Verdict)
	}
	if result.Confidence != 0.85 {
		t.Errorf("got confidence=%f, want 0.85", result.Confidence)
	}
}

func TestBuildIdentifyPrompt(t *testing.T) {
	candidates := []matcher.Pattern{
		{English: "Observer", Chinese: "观察者模式"},
		{English: "Mediator", Chinese: "中介者模式"},
	}
	prompt := BuildIdentifyPrompt("type event struct{}", candidates, PromptConfig{Language: "zh"})
	if !strings.Contains(prompt, "观察者模式") {
		t.Error("prompt should contain candidate names")
	}
	if !strings.Contains(prompt, "中介者模式") {
		t.Error("prompt should contain candidate names")
	}
}

func TestIdentifyWithMock(t *testing.T) {
	client := &mockClient{
		response: `{"pattern":"Observer","confidence":0.92,"reasoning":"The code implements a publish-subscribe mechanism."}`,
	}
	pattern, confidence, err := Identify(context.Background(), client, "type publisher struct{}", nil, PromptConfig{Language: "en"})
	if err != nil {
		t.Fatalf("Identify error: %v", err)
	}
	if pattern != "Observer" {
		t.Errorf("got pattern=%q, want Observer", pattern)
	}
	if confidence != 0.92 {
		t.Errorf("got confidence=%f, want 0.92", confidence)
	}
}

func TestInvalidVerdictDefault(t *testing.T) {
	raw := `{"verdict":"unknown_verdict","confidence":0.5,"issues":[],"summary":"test"}`
	result, err := parseResult(raw, matcher.Pattern{English: "Test"})
	if err != nil {
		t.Fatalf("parseResult error: %v", err)
	}
	if result.Verdict != VerdictUncertain {
		t.Errorf("got verdict=%q, want uncertain for unknown value", result.Verdict)
	}
}
