// Package semantic provides an LLM-based semantic analysis layer for design
// pattern validation. It defines a Client interface that callers implement
// (e.g. using the Reasonix provider system), prompt-building helpers, and a
// typed AnalysisResult.
package semantic

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"reasonix/internal/designpattern/errorcode"
	"reasonix/internal/designpattern/matcher"
)

// ---------- Client interface ----------

// Client is the LLM interface the semantic layer needs. Callers wire their
// preferred provider/model behind this.
type Client interface {
	// Complete sends a prompt and returns the raw text response.
	Complete(ctx context.Context, prompt string) (string, error)
}

// ---------- Analysis types ----------

// Verdict is the conclusion of a semantic analysis.
type Verdict string

const (
	VerdictCorrect   Verdict = "correct"    // pattern is used correctly
	VerdictIncorrect Verdict = "incorrect"  // pattern is used incorrectly
	VerdictUncertain Verdict = "uncertain"  // cannot determine with confidence
	VerdictNoPattern Verdict = "no_pattern" // no known design pattern detected
)

// Issue describes a specific problem found in the pattern usage.
type Issue struct {
	Line       int    `json:"line,omitempty"`       // source line number (0 = N/A)
	Message    string `json:"message"`              // human-readable description
	Suggestion string `json:"suggestion,omitempty"` // how to fix
}

// AnalysisResult holds the full output of a semantic analysis.
type AnalysisResult struct {
	Pattern    matcher.Pattern `json:"pattern"`
	Verdict    Verdict         `json:"verdict"`
	Confidence float64         `json:"confidence"` // 0.0 – 1.0
	Issues     []Issue         `json:"issues,omitempty"`
	Summary    string          `json:"summary"`
}

// ---------- Prompt building ----------

// PromptConfig controls the verbosity and focus of the LLM prompt.
type PromptConfig struct {
	Language string // "zh" or "en" (default "zh")
	Detailed bool   // include full code context vs. summary
}

// BuildValidationPrompt constructs a prompt for validating whether code
// correctly implements a given design pattern.
func BuildValidationPrompt(pattern matcher.Pattern, codeSnippet string, cfg PromptConfig) string {
	lang := cfg.Language
	if lang == "" {
		lang = "zh"
	}

	var b strings.Builder
	if lang == "zh" {
		fmt.Fprintf(&b, "请分析以下代码是否正确地实现了「%s」（%s）设计模式。\n\n", pattern.Chinese, pattern.English)
		b.WriteString("请从以下方面进行评估：\n")
		b.WriteString("1. 结构是否符合该模式的经典定义（参与者、协作关系）\n")
		b.WriteString("2. 是否引入了该模式的核心意图（解决什么问题）\n")
		b.WriteString("3. 是否存在常见的误用或反模式\n")
		b.WriteString("4. 如果实现有误，请指出具体位置和改进建议\n\n")
		b.WriteString("请以JSON格式回复，包含以下字段：\n")
		b.WriteString("  - \"verdict\": \"correct\" | \"incorrect\" | \"uncertain\" | \"no_pattern\"\n")
		b.WriteString("  - \"confidence\": 0.0-1.0 的置信度分数\n")
		b.WriteString("  - \"issues\": 问题列表（每项含 line, message, suggestion）\n")
		b.WriteString("  - \"summary\": 简要分析总结\n\n")
	} else {
		fmt.Fprintf(&b, "Analyse whether the following code correctly implements the %q design pattern.\n\n", pattern.English)
		b.WriteString("Evaluate:\n")
		b.WriteString("1. Structure matches the classic definition (participants, collaborations)\n")
		b.WriteString("2. The code fulfils the pattern's core intent\n")
		b.WriteString("3. Common misuses or anti-patterns\n")
		b.WriteString("4. Specific issues and improvement suggestions\n\n")
		b.WriteString("Reply in JSON with: verdict, confidence (0.0-1.0), issues (line, message, suggestion), summary.\n\n")
	}

	b.WriteString("### 设计模式 / Design Pattern ###\n")
	fmt.Fprintf(&b, "名称: %s (%s)\n分类: %s\n\n", pattern.English, pattern.Chinese, pattern.Category)
	b.WriteString("### 代码 / Code ###\n")
	b.WriteString("```\n")
	b.WriteString(codeSnippet)
	if !strings.HasSuffix(codeSnippet, "\n") {
		b.WriteString("\n")
	}
	b.WriteString("```\n")
	return b.String()
}

// BuildIdentifyPrompt constructs a prompt for identifying which design pattern(s)
// a code snippet implements.
func BuildIdentifyPrompt(codeSnippet string, candidates []matcher.Pattern, cfg PromptConfig) string {
	lang := cfg.Language
	if lang == "" {
		lang = "zh"
	}

	var b strings.Builder
	if lang == "zh" {
		b.WriteString("请分析以下代码实现了哪种设计模式，并说明理由。\n\n")
	} else {
		b.WriteString("Identify which design pattern(s) the following code implements, with reasoning.\n\n")
	}

	if len(candidates) > 0 {
		if lang == "zh" {
			b.WriteString("候选模式：\n")
		} else {
			b.WriteString("Candidates:\n")
		}
		for _, p := range candidates {
			fmt.Fprintf(&b, "  - %s (%s)\n", p.English, p.Chinese)
		}
		b.WriteString("\n")
	}

	b.WriteString("### 代码 ###\n")
	b.WriteString("```\n")
	b.WriteString(codeSnippet)
	if !strings.HasSuffix(codeSnippet, "\n") {
		b.WriteString("\n")
	}
	b.WriteString("```\n")

	if lang == "zh" {
		b.WriteString("\n请回复JSON格式：pattern（模式英文名）, confidence（置信度）, reasoning（分析理由）\n")
	} else {
		b.WriteString("\nReply in JSON: pattern (English name), confidence (0-1), reasoning.\n")
	}
	return b.String()
}

// ---------- Analysis runner ----------

// Analyse sends a validation prompt to the LLM client and parses the structured
// response. It returns an *errorcode.Error with Kind Semantic if the LLM output
// cannot be parsed.
func Analyse(ctx context.Context, client Client, pattern matcher.Pattern, codeSnippet string, cfg PromptConfig) (AnalysisResult, error) {
	prompt := BuildValidationPrompt(pattern, codeSnippet, cfg)
	raw, err := client.Complete(ctx, prompt)
	if err != nil {
		return AnalysisResult{}, errorcode.New(errorcode.Semantic,
			"LLM completion failed: %v", err)
	}

	return parseResult(raw, pattern)
}

// Identify sends an identification prompt to the LLM client and returns the
// identified pattern name and confidence.
func Identify(ctx context.Context, client Client, codeSnippet string, candidates []matcher.Pattern, cfg PromptConfig) (string, float64, error) {
	prompt := BuildIdentifyPrompt(codeSnippet, candidates, cfg)
	raw, err := client.Complete(ctx, prompt)
	if err != nil {
		return "", 0, errorcode.New(errorcode.Semantic,
			"LLM completion failed: %v", err)
	}

	return parseIdentifyResult(raw)
}

// ---------- Response parsing ----------

type rawAnalysisResponse struct {
	Verdict    Verdict `json:"verdict"`
	Confidence float64 `json:"confidence"`
	Issues     []Issue `json:"issues,omitempty"`
	Summary    string  `json:"summary"`
}

type rawIdentifyResponse struct {
	Pattern    string  `json:"pattern"`
	Confidence float64 `json:"confidence"`
	Reasoning  string  `json:"reasoning"`
}

func parseResult(raw string, pattern matcher.Pattern) (AnalysisResult, error) {
	cleaned := extractJSON(raw)
	var resp rawAnalysisResponse
	if err := json.Unmarshal([]byte(cleaned), &resp); err != nil {
		// If JSON parsing fails, return an uncertain result instead of failing.
		return AnalysisResult{
			Pattern:    pattern,
			Verdict:    VerdictUncertain,
			Confidence: 0,
			Summary:    "failed to parse LLM response: " + err.Error(),
		}, nil
	}

	// Validate verdict.
	switch resp.Verdict {
	case VerdictCorrect, VerdictIncorrect, VerdictUncertain, VerdictNoPattern:
	default:
		resp.Verdict = VerdictUncertain
	}

	if resp.Confidence < 0 || resp.Confidence > 1 {
		resp.Confidence = 0
	}
	if resp.Issues == nil {
		resp.Issues = []Issue{}
	}

	return AnalysisResult{
		Pattern:    pattern,
		Verdict:    resp.Verdict,
		Confidence: resp.Confidence,
		Issues:     resp.Issues,
		Summary:    resp.Summary,
	}, nil
}

func parseIdentifyResult(raw string) (string, float64, error) {
	cleaned := extractJSON(raw)
	var resp rawIdentifyResponse
	if err := json.Unmarshal([]byte(cleaned), &resp); err != nil {
		return "", 0, errorcode.New(errorcode.Semantic,
			"failed to parse LLM identification response: %v", err)
	}
	if resp.Confidence < 0 || resp.Confidence > 1 {
		resp.Confidence = 0
	}
	return resp.Pattern, resp.Confidence, nil
}

// extractJSON finds the first JSON object ({...}) in raw text, handling
// markdown code fences.
func extractJSON(raw string) string {
	// Strip markdown code fences.
	raw = strings.TrimSpace(raw)
	if idx := strings.Index(raw, "```"); idx >= 0 {
		rest := raw[idx+3:]
		if idx2 := strings.Index(rest, "```"); idx2 >= 0 {
			rest = rest[:idx2]
		}
		// Remove optional language tag.
		if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
			rest = rest[nl+1:]
		}
		raw = strings.TrimSpace(rest)
	}

	start := strings.IndexByte(raw, '{')
	if start < 0 {
		return `{"verdict":"uncertain","confidence":0,"issues":[],"summary":"no JSON found in LLM response"}`
	}
	raw = raw[start:]

	end := strings.LastIndexByte(raw, '}')
	if end < 0 {
		return `{"verdict":"uncertain","confidence":0,"issues":[],"summary":"no closing brace in LLM response"}`
	}
	return raw[:end+1]
}
