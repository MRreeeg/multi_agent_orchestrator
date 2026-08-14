package orchestrator

import (
	"context"
	"fmt"
	"strings"

	dshclient "reasonix/internal/executor/dsh"
)

// DshPipelineExecutor wraps dsh.DshExecutor to implement PipelineExecutor.
//
// DSH is run-only: every node attempt is one `dsh --profile headless "<task>"`
// invocation that creates a fresh persisted session, prints the final
// assistant message, and exits. There is no retained serve protocol (no
// app-server / stream-json equivalent), so the node Mode must be "run".
type DshPipelineExecutor struct {
	Client *dshclient.DshExecutor
}

// Name returns the executor identifier.
func (e *DshPipelineExecutor) Name() string { return "dsh" }

// Execute runs a DSH node through the one-shot headless profile.
func (e *DshPipelineExecutor) Execute(ctx context.Context, spec ExecSpec, onStart func(endpoint string, port int)) (*ExecResult, error) {
	client := e.Client
	if client == nil {
		client = dshclient.New()
	}

	// Validate context policy. DSH headless always starts a fresh session and
	// exposes no CLI resume seam, so the policy is accepted but advisory.
	switch spec.ContextPolicy {
	case "", "reuse", "fresh", "fresh_per_run":
		// valid
	default:
		return nil, fmt.Errorf("dsh: unknown context policy %q", spec.ContextPolicy)
	}

	if strings.EqualFold(strings.TrimSpace(spec.Mode), "serve") {
		return nil, fmt.Errorf("dsh: serve mode is not supported; DSH headless is one-shot, use mode=run")
	}

	// Inject Skill content into the prompt, mirroring codex/claude so the node
	// receives the same task contract regardless of executor.
	prompt := spec.Prompt
	if spec.SkillContent != "" {
		prompt = fmt.Sprintf("# SYSTEM-LEVEL SKILL INSTRUCTIONS\n\n以下是本节点必须遵守的 Skill 指令。\nSkill 名称：%s\n\n<skill>\n%s\n</skill>\n\n# TASK\n\n%s",
			spec.Skill, spec.SkillContent, spec.Prompt)
	}

	opts := dshclient.ExecOptions{
		Model:          spec.ModelRef,
		Workspace:      spec.Workspace,
		PermissionMode: dshPermissionMode(spec),
		AgentPreset:    spec.DshPreset,
	}
	result, err := client.Exec(ctx, prompt, opts)
	if result == nil {
		if err != nil {
			return &ExecResult{RawStderr: err.Error(), ExitCode: -1}, fmt.Errorf("dsh exec returned nil result: %w", err)
		}
		return nil, dshclient.ErrExecutorProtocol
	}
	execResult := &ExecResult{
		FinalText: result.Output,
		RawStdout: result.RawStdout,
		RawStderr: result.Stderr,
		ExitCode:  result.ExitCode,
	}
	if err != nil {
		// Return partial result with error — the caller still surfaces stderr.
		return execResult, fmt.Errorf("dsh exec: %w", err)
	}
	return execResult, nil
}

// dshPermissionMode maps the orchestrator trust / read-only policy onto the
// DSH sandbox boundary (DSH_PERMISSION_MODE). Headless cannot answer
// interactive approvals, so trusted nodes run danger-full-access (approval
// never) and read-only nodes run a read-only sandbox.
func dshPermissionMode(spec ExecSpec) string {
	if spec.ToolsReadOnly {
		return "read-only"
	}
	if spec.Trust {
		return "danger-full-access"
	}
	switch strings.ToLower(strings.TrimSpace(spec.ApprovalMode)) {
	case "auto", "yolo":
		return "danger-full-access"
	}
	return "workspace-write"
}
