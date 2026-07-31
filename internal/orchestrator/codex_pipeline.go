package orchestrator

import (
	"context"
	"fmt"

	"reasonix/internal/executor/codex"
)

// CodexPipelineExecutor wraps codex.CodexExecutor to implement PipelineExecutor.
type CodexPipelineExecutor struct {
	Client *codex.CodexExecutor
}

// Name returns the executor identifier.
func (e *CodexPipelineExecutor) Name() string { return "codex" }

// Execute runs a Codex node via `codex exec` or `codex exec resume`.
func (e *CodexPipelineExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(endpoint string, port int)) (*ExecResult, error) {
	client := e.Client
	if client == nil {
		client = codex.New()
	}

	// Validate context policy
	switch spec.ContextPolicy {
	case "", "reuse", "fresh", "fresh_per_run":
		// valid
	default:
		return nil, fmt.Errorf("codex: unknown context policy %q", spec.ContextPolicy)
	}

	// Determine ephemeral and resume based on ContextPolicy
	ephemeral := spec.ContextPolicy == "fresh" || spec.ContextPolicy == "fresh_per_run"

	// Only pass ResumeSessionID for "reuse" mode with a valid ID
	var resumeID string
	if spec.ContextPolicy == "reuse" || spec.ContextPolicy == "" {
		resumeID = spec.ExternalSessionID
	}
	// fresh/fresh_per_run: never pass ResumeSessionID (start new session)

	opts := codex.ExecOptions{
		Model:           spec.ModelRef,
		ReasoningEffort: spec.ReasoningEffort,
		Workspace:       spec.Workspace,
		BypassAll:       spec.Trust,
		Ephemeral:       ephemeral,
		JSON:            true,
		ResumeSessionID: resumeID,
	}

	// Inject Skill content into prompt if present
	prompt := spec.Prompt
	if spec.SkillContent != "" {
		prompt = fmt.Sprintf("# SYSTEM-LEVEL SKILL INSTRUCTIONS\n\n以下是本节点必须遵守的 Skill 指令。\nSkill 名称：%s\n\n<skill>\n%s\n</skill>\n\n# TASK\n\n%s",
			spec.Skill, spec.SkillContent, spec.Prompt)
	}

	codexResult, err := client.Exec(ctx, prompt, opts)

	// Nil result protection
	if codexResult == nil {
		if err != nil {
			return &ExecResult{RawStderr: err.Error(), ExitCode: -1}, fmt.Errorf("codex exec returned nil result: %w", err)
		}
		return nil, codex.ErrExecutorProtocol
	}

	// Always build result from partial data, even on error
	result := &ExecResult{
		FinalText:         codexResult.Output,
		RawStdout:         codexResult.RawStdout,
		RawStderr:         codexResult.Stderr,
		ExternalSessionID: codexResult.ThreadID,
	}

	if err != nil {
		// Return partial result with error — caller can still save session ID, stderr, etc.
		return result, fmt.Errorf("codex exec: %w", err)
	}

	return result, nil
}
