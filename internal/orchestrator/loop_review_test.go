package orchestrator

import (
	"testing"
)

func TestParseReviewDecisionPass(t *testing.T) {
	output := `{
  "schemaVersion": "loop-review-v1",
  "decision": "pass",
  "confidence": 0.95,
  "summary": "报告满足验收标准",
  "blockingIssues": [],
  "requiredChanges": [],
  "nextTask": "",
  "evidence": ["包含文件基本信息", "包含质量评估"]
}`
	d, err := ParseReviewDecision(output)
	if err != nil {
		t.Fatal(err)
	}
	if d.Decision != "pass" {
		t.Errorf("decision = %q, want pass", d.Decision)
	}
	if d.Confidence != 0.95 {
		t.Errorf("confidence = %f, want 0.95", d.Confidence)
	}
}

func TestParseReviewDecisionRevise(t *testing.T) {
	output := `{
  "schemaVersion": "loop-review-v1",
  "decision": "revise",
  "confidence": 0.87,
  "summary": "需要补充验证",
  "blockingIssues": [],
  "requiredChanges": ["补充文件写入验证", "增加错误处理"],
  "nextTask": "补充文件写入验证和错误处理",
  "evidence": ["缺少写入验证"]
}`
	d, err := ParseReviewDecision(output)
	if err != nil {
		t.Fatal(err)
	}
	if d.Decision != "revise" {
		t.Errorf("decision = %q, want revise", d.Decision)
	}
	if len(d.RequiredChanges) != 2 {
		t.Errorf("requiredChanges count = %d, want 2", len(d.RequiredChanges))
	}
	if d.NextTask == "" {
		t.Error("nextTask is empty")
	}
}

func TestParseReviewDecisionBlocked(t *testing.T) {
	output := `{
  "schemaVersion": "loop-review-v1",
  "decision": "blocked",
  "confidence": 0.9,
  "summary": "存在安全阻塞",
  "blockingIssues": ["权限不足"],
  "requiredChanges": [],
  "nextTask": "",
  "evidence": ["权限检查失败"]
}`
	d, err := ParseReviewDecision(output)
	if err != nil {
		t.Fatal(err)
	}
	if d.Decision != "blocked" {
		t.Errorf("decision = %q, want blocked", d.Decision)
	}
	if len(d.BlockingIssues) != 1 {
		t.Errorf("blockingIssues count = %d, want 1", len(d.BlockingIssues))
	}
}

func TestParseReviewDecisionWithMarkdown(t *testing.T) {
	output := `以下是审查结果：

` + "```json" + `
{
  "schemaVersion": "loop-review-v1",
  "decision": "pass",
  "confidence": 0.92,
  "summary": "通过",
  "blockingIssues": [],
  "requiredChanges": [],
  "nextTask": "",
  "evidence": []
}
` + "```" + `

审查完成。`
	d, err := ParseReviewDecision(output)
	if err != nil {
		t.Fatal(err)
	}
	if d.Decision != "pass" {
		t.Errorf("decision = %q, want pass", d.Decision)
	}
}

func TestParseReviewDecisionEmpty(t *testing.T) {
	_, err := ParseReviewDecision("")
	if err == nil {
		t.Error("expected error for empty output")
	}
}

func TestParseReviewDecisionNoJSON(t *testing.T) {
	_, err := ParseReviewDecision("this is just text without any JSON")
	if err == nil {
		t.Error("expected error for no JSON")
	}
}

func TestValidateReviewDecisionPass(t *testing.T) {
	d := ReviewDecision{SchemaVersion: "loop-review-v1", Decision: "pass", Confidence: 0.9}
	if err := ValidateReviewDecision(d); err != nil {
		t.Error(err)
	}
}

func TestValidateReviewDecisionReviseValid(t *testing.T) {
	d := ReviewDecision{
		SchemaVersion:   "loop-review-v1",
		Decision:        "revise",
		Confidence:      0.8,
		NextTask:        "fix the bug",
		RequiredChanges: []string{"fix bug"},
	}
	if err := ValidateReviewDecision(d); err != nil {
		t.Error(err)
	}
}

func TestValidateReviewDecisionReviseNoNextTask(t *testing.T) {
	d := ReviewDecision{
		SchemaVersion:   "loop-review-v1",
		Decision:        "revise",
		Confidence:      0.8,
		RequiredChanges: []string{"fix bug"},
	}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for revise without nextTask")
	}
}

func TestValidateReviewDecisionReviseNoChanges(t *testing.T) {
	d := ReviewDecision{
		SchemaVersion: "loop-review-v1",
		Decision:      "revise",
		Confidence:    0.8,
		NextTask:      "fix the bug",
	}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for revise without requiredChanges")
	}
}

func TestValidateReviewDecisionPassWithBlocking(t *testing.T) {
	d := ReviewDecision{
		SchemaVersion:  "loop-review-v1",
		Decision:       "pass",
		Confidence:     0.9,
		BlockingIssues: []string{"issue"},
	}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for pass with blockingIssues")
	}
}

func TestValidateReviewDecisionConfidenceOutOfRange(t *testing.T) {
	d := ReviewDecision{SchemaVersion: "loop-review-v1", Decision: "pass", Confidence: 1.5}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for confidence > 1")
	}
}

func TestValidateReviewDecisionInvalidDecision(t *testing.T) {
	d := ReviewDecision{SchemaVersion: "loop-review-v1", Decision: "maybe", Confidence: 0.5}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for invalid decision")
	}
}

func TestValidateReviewDecisionInvalidSchema(t *testing.T) {
	d := ReviewDecision{SchemaVersion: "wrong", Decision: "pass", Confidence: 0.9}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for invalid schemaVersion")
	}
}

func TestValidateReviewDecisionBlockedRequiresBlockingIssues(t *testing.T) {
	d := ReviewDecision{
		SchemaVersion: "loop-review-v1",
		Decision:      "blocked",
		Confidence:    0.9,
	}
	if err := ValidateReviewDecision(d); err == nil {
		t.Error("expected error for blocked without blockingIssues")
	}
}

func TestParseReviewDecisionIgnoresBracesInsideStringsAndToolJSON(t *testing.T) {
	output := `{"command":"Get-ChildItem -Path C:\\work\\{draft}"}
审查结果：{"schemaVersion":"loop-review-v1","decision":"pass","confidence":0.9,"summary":"结果包含 {} 文本","blockingIssues":[],"requiredChanges":[],"nextTask":"","evidence":["检查了 {draft}"]}`
	d, err := ParseReviewDecision(output)
	if err != nil {
		t.Fatal(err)
	}
	if d.Decision != "pass" {
		t.Fatalf("decision = %q, want pass", d.Decision)
	}
}

func TestParseReviewDecisionRejectsToolCommandJSON(t *testing.T) {
	_, err := ParseReviewDecision(`{"command":"if (Test-Path C:\\work) { Write-Host ok }"}`)
	if err == nil {
		t.Fatal("expected tool command JSON to be rejected")
	}
}
