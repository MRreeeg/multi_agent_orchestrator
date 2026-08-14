package orchestrator

import (
	"context"
	"fmt"

	claudeclient "reasonix/internal/executor/claude"
)

// ClaudePipelineExecutor wraps claude.ClaudeExecutor to implement
// PipelineExecutor.
type ClaudePipelineExecutor struct {
	Client *claudeclient.ClaudeExecutor
}

// Name returns the executor identifier.
func (e *ClaudePipelineExecutor) Name() string { return "claude" }

// Execute runs a Claude node through one-shot `claude -p --output-format json`
// or the retained stream-json Runtime Manager, depending on the node mode.
func (e *ClaudePipelineExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(endpoint string, port int)) (*ExecResult, error) {
	client := e.Client
	if client == nil {
		client = claudeclient.New()
	}

	// Validate context policy
	switch spec.ContextPolicy {
	case "", "reuse", "fresh", "fresh_per_run":
		// valid
	default:
		return nil, fmt.Errorf("claude: unknown context policy %q", spec.ContextPolicy)
	}

	// Inject Skill content into the prompt. Both one-shot and the retained
	// runtime receive exactly the same task contract.
	prompt := spec.Prompt
	if spec.SkillContent != "" {
		prompt = fmt.Sprintf("# SYSTEM-LEVEL SKILL INSTRUCTIONS\n\n以下是本节点必须遵守的 Skill 指令。\nSkill 名称：%s\n\n<skill>\n%s\n</skill>\n\n# TASK\n\n%s",
			spec.Skill, spec.SkillContent, spec.Prompt)
	}

	// `serve` is a retained Claude stream-json runtime owned by
	// ClaudeRuntimeManager; Loop ownership remains in the orchestrator.
	if spec.Mode == "serve" {
		serveSpec := spec
		serveSpec.Prompt = prompt
		return claudeRuntimeMgr.Execute(ctx, serveSpec, onStart)
	}

	// run: one-shot claude -p --output-format json with optional --resume.
	var resumeID string
	if spec.ContextPolicy == "reuse" || spec.ContextPolicy == "" {
		resumeID = spec.ExternalSessionID
	}
	opts := claudeclient.ExecOptions{
		Model:              claudeRuntimeModel(spec),
		ConfigDir:          claudeConfigDir(spec),
		Workspace:          spec.Workspace,
		ResumeSessionID:    resumeID,
		PermissionMode:     claudeclient.PermissionModeForApproval(spec.ApprovalMode),
		AppendSystemPrompt: spec.SkillContent,
		Effort:             spec.ReasoningEffort,
	}
	result, err := client.Exec(ctx, prompt, opts)
	if result == nil {
		if err != nil {
			return &ExecResult{RawStderr: err.Error(), ExitCode: -1}, fmt.Errorf("claude exec returned nil result: %w", err)
		}
		return nil, claudeclient.ErrExecutorProtocol
	}
	execResult := &ExecResult{
		FinalText:         result.Output,
		RawStdout:         result.RawStdout,
		RawStderr:         result.Stderr,
		ExternalSessionID: result.SessionID,
	}
	if err != nil {
		return execResult, fmt.Errorf("claude exec: %w", err)
	}
	return execResult, nil
}
