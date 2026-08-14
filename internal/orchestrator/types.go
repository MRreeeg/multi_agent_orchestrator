// Package orchestrator manages multi-agent pipeline orchestration: it defines
// Pipeline DAGs (nodes + edges), spawns reasonix serve child processes, sends
// tasks to them via HTTP, collects results, and tracks token usage. The
// HTTP/SSE frontend in internal/serve drives it through API endpoints.
package orchestrator

import (
	"fmt"
	"time"
)

// NodeType identifies the kind of agent for an orchestration node.
type NodeType string

const (
	NodeArchitect NodeType = "architect"
	NodeExecutor  NodeType = "executor"
	NodeReviewer  NodeType = "reviewer"
)

// ExecutorType identifies which runtime executes a node.
type ExecutorType string

const (
	ExecutorReasonix ExecutorType = "reasonix"
	ExecutorMimo     ExecutorType = "mimo"
	ExecutorCodex    ExecutorType = "codex"
	ExecutorClaude   ExecutorType = "claude"
	ExecutorOpencode ExecutorType = "opencode"
	ExecutorDsh      ExecutorType = "dsh"
)

// validateContextPolicy checks that a context policy value is valid.
// Valid values: "" (empty, treated as reuse), "reuse", "fresh", "fresh_per_run".
func validateContextPolicy(policy string) error {
	switch policy {
	case "", "reuse", "fresh", "fresh_per_run":
		return nil
	default:
		return fmt.Errorf("invalid context policy %q: must be reuse, fresh, or fresh_per_run", policy)
	}
}

// ExecSpec is the unified execution specification for any executor.
type ExecSpec struct {
	Workspace string
	Prompt    string
	ModelRef  string
	// DisplayModel preserves the user-facing model label for runtime metadata.
	// It may differ from ModelRef when a local route (for example CCSwitch)
	// intentionally omits --model on the provider command.
	DisplayModel string
	// ProviderRoute selects a local/provider routing profile. For Codex,
	// "ccswitch" means use the Codex CLI's current CCSwitch configuration and
	// intentionally omit --model.
	ProviderRoute   string
	Agent           string // mimo agent profile
	Skill           string // reasonix skill
	SkillContent    string // loaded SKILL.md content for prompt injection
	SkillPolicy     string // "automated" | "advisory" | "strict"
	ReasoningEffort string // "high" | "medium" | "low"
	Mode            string // run | serve
	Executor        string // reasonix | mimo | codex
	ApprovalMode    string // ask | auto
	ExecutionMode   string // task | goal
	Trust           bool
	NeverAsk        bool
	NodeID          string
	NodeLabel       string
	// ContextPolicy controls session persistence: "reuse" | "fresh_per_run" | "fresh"
	ContextPolicy string
	// ExternalSessionID is the provider's session ID for resume (e.g., Codex thread_id)
	ExternalSessionID string
	// MaxSteps limits provider tool-call rounds for bounded orchestration nodes.
	// Zero keeps the provider default.
	MaxSteps int
	// ToolsReadOnly keeps read-only exploration (read/grep/glob) but denies
	// everything that can mutate state or run commands (bash/edit/write/
	// task/web/*). Used when the node must inspect the codebase before
	// producing its deliverable, without the risk of a runaway tool loop.
	ToolsReadOnly bool
	// TurnTimeout is the hard per-turn budget for the opencode serve turn.
	// Zero uses the runtime default (5 minutes).
	TurnTimeout time.Duration
	// DshPreset names a locally authored DSH agent preset for dsh executor
	// nodes (mirrors AgentNode.DshPreset).
	DshPreset string
}

// ExecResult is the unified execution result from any executor.
type ExecResult struct {
	FinalText         string
	RawStdout         string
	RawStderr         string
	ExitCode          int
	DurationMs        int64
	RuntimeID         string
	Endpoint          string
	TokenUsage        *TokenUsage
	ExternalSessionID string // provider's session ID for resume (e.g., Codex thread_id)
	// MaxSteps limits provider tool-call rounds for bounded orchestration nodes.
	// Zero keeps the provider default.
	MaxSteps int
}

// NodeStatus tracks the execution state of a single pipeline node.
type NodeStatus string

const (
	NodePending     NodeStatus = "pending"
	NodeRunning     NodeStatus = "running"
	NodeComplete    NodeStatus = "complete"
	NodeFailed      NodeStatus = "failed"
	NodeCanceled    NodeStatus = "canceled"
	NodeInterrupted NodeStatus = "interrupted"
)

// Pipeline is a named DAG of agent nodes and data edges.
type Pipeline struct {
	ID        string      `json:"id"`
	Name      string      `json:"name"`
	Nodes     []AgentNode `json:"nodes"`
	Edges     []Edge      `json:"edges"`
	CreatedAt string      `json:"createdAt"`
	UpdatedAt string      `json:"updatedAt"`
}

// AgentNode represents one agent in the pipeline.
type AgentNode struct {
	ID              string       `json:"id"`
	Type            NodeType     `json:"type"`
	Label           string       `json:"name"`                      // frontend sends "name"
	Model           string       `json:"model"`                     // provider/model ref
	ProviderRoute   string       `json:"providerRoute,omitempty"`   // codex route profile, e.g. ccswitch
	Agent           string       `json:"agent,omitempty"`           // mimo agent profile
	Skill           string       `json:"skill"`                     // optional skill name
	SkillPolicy     string       `json:"skillPolicy,omitempty"`     // automated | advisory | strict
	ReasoningEffort string       `json:"reasoningEffort,omitempty"` // high | medium | low
	Mode            string       `json:"mode,omitempty"`
	RoleDesc        string       `json:"role"`          // frontend sends "role"
	Executor        ExecutorType `json:"executor"`      // reasonix | mimo | codex
	ExecutionMode   string       `json:"executionMode"` // task | goal (default: task)
	ApprovalMode    string       `json:"approvalMode"`  // ask | auto (default: ask)
	// DshPreset names a locally authored DSH agent preset
	// ($DSH_HOME/.agent-presets/<id>) for dsh executor nodes. When set, the
	// node runs under that customized agent (persona + pruned tool catalog)
	// via its headless.patch.yml; when empty, the node uses DSH's stock
	// headless persona plus prompt/skill injection.
	DshPreset string  `json:"dshPreset,omitempty"`
	InputMap  string  `json:"inputMap"`  // how to map upstream output to this node's input
	OutputMap string  `json:"outputMap"` // how to map this node's output for downstream
	X         float64 `json:"x"`         // editor canvas X
	Y         float64 `json:"y"`         // editor canvas Y
}

// Edge defines data flow between two nodes.
type Edge struct {
	ID     string `json:"id"`
	FromID string `json:"from"`
	ToID   string `json:"to"`
	Label  string `json:"label,omitempty"`
}

// PipelineRun tracks one execution of a Pipeline.
type PipelineRun struct {
	ID                 string              `json:"id"`
	PipelineID         string              `json:"pipelineId"`
	PipelineRevisionID string              `json:"pipelineRevisionID,omitempty"`
	SessionID          string              `json:"sessionId,omitempty"`
	Task               string              `json:"task,omitempty"`
	RewrittenTask      string              `json:"rewrittenTask,omitempty"`
	Status             string              `json:"status"`
	Trigger            string              `json:"trigger,omitempty"`
	ParentRunID        string              `json:"parentRunID,omitempty"`
	LoopIteration      int                 `json:"loopIteration,omitempty"`
	ExecOptions        ExecutionOptions    `json:"execOptions,omitempty"`
	LoopConfig         LoopConfig          `json:"loopConfig,omitempty"`
	CurrentIteration   int                 `json:"currentIteration"`
	IterationIDs       []string            `json:"iterationIDs,omitempty"`
	FinalReview        *ReviewDecision     `json:"finalReview,omitempty"`
	TerminationReason  string              `json:"terminationReason,omitempty"` // review_pass | fixed_limit | max_iterations | blocked | failed | canceled
	NodeStates         map[string]RunState `json:"nodeStates"`
	NodeAttemptIDs     []string            `json:"nodeAttemptIDs,omitempty"`
	CurrentNodeID      string              `json:"currentNodeID,omitempty"`
	CreatedAt          string              `json:"createdAt"`
	StartedAt          string              `json:"startedAt,omitempty"`
	FinishedAt         string              `json:"finishedAt,omitempty"`
	UpdatedAt          string              `json:"updatedAt"`
	Error              string              `json:"error,omitempty"`
	Cancel             func()              `json:"-"`
}

// RunState captures a node's execution state during a run.
type RunState struct {
	Status     NodeStatus `json:"status"`
	Input      string     `json:"input,omitempty"`
	Output     string     `json:"output,omitempty"`
	Stderr     string     `json:"stderr,omitempty"` // full stderr (thinking, tool calls)
	Error      string     `json:"error,omitempty"`
	TokenUsage TokenUsage `json:"tokenUsage"`
	StartedAt  string     `json:"startedAt,omitempty"`
	DoneAt     string     `json:"doneAt,omitempty"`
}

// TokenUsage records token consumption for a node or the whole run.
type TokenUsage struct {
	InputTokens  int     `json:"inputTokens"`
	OutputTokens int     `json:"outputTokens"`
	TotalTokens  int     `json:"totalTokens"`
	CostYuan     float64 `json:"costYuan"`
	DurationMs   int64   `json:"durationMs"`
	Source       string  `json:"source,omitempty"`
}

// AgentInstance describes a running agent subprocess managed by the orchestrator.
type AgentInstance struct {
	ID       string       `json:"id"`
	Type     NodeType     `json:"type"`
	Port     int          `json:"port"`
	Model    string       `json:"model"`
	Status   string       `json:"status"` // running | stopped | error
	Label    string       `json:"label"`
	Error    string       `json:"error,omitempty"`
	Executor ExecutorType `json:"executor,omitempty"`
	Endpoint string       `json:"endpoint,omitempty"`
}

// NodeTypeInfo describes an available node type for the frontend config panel.
type NodeTypeInfo struct {
	Type             NodeType                  `json:"type"`
	Label            string                    `json:"label"`
	Models           []string                  `json:"models"`
	ModelsByExecutor map[ExecutorType][]string `json:"modelsByExecutor,omitempty"`
	Skills           []string                  `json:"skills"`
	Executors        []ExecutorType            `json:"executors"`
}

// PipelinePreset is a pre-built pipeline template.
type PipelinePreset struct {
	ID       string   `json:"id"`
	Name     string   `json:"name"`
	Desc     string   `json:"desc"`
	Pipeline Pipeline `json:"pipeline"`
	// LoopConfig is an optional loop setup applied when the preset loads.
	LoopConfig *LoopConfig `json:"loopConfig,omitempty"`
}

// Stats aggregates token/cost data across all runs.
type Stats struct {
	TotalRuns       int        `json:"totalRuns"`
	TotalTokens     int        `json:"totalTokens"`
	TotalCostYuan   float64    `json:"totalCostYuan"`
	TotalDurationMs int64      `json:"totalDurationMs"`
	PerNode         []NodeStat `json:"perNode,omitempty"`
}

// NodeStat is per-node-type statistics.
type NodeStat struct {
	Type     NodeType `json:"type"`
	RunCount int      `json:"runCount"`
	TokenSum int      `json:"tokenSum"`
	CostYuan float64  `json:"costYuan"`
}

// PipelinePayload is the JSON body for creating/updating a pipeline.
type PipelinePayload struct {
	Name       string      `json:"name"`
	Nodes      []AgentNode `json:"nodes"`
	Edges      []Edge      `json:"edges"`
	LoopConfig *LoopConfig `json:"loopConfig,omitempty"`
}

// Session groups multiple PipelineRuns under one user task.
type Session struct {
	ID        string       `json:"id"`
	Task      string       `json:"task"`   // original user requirement
	RunIDs    []string     `json:"runIds"` // all run IDs in this session
	Stats     SessionStats `json:"stats"`  // aggregated stats
	CreatedAt string       `json:"createdAt"`
	UpdatedAt string       `json:"updatedAt"`
}

// SessionStats aggregates token/cost data across all runs in a session.
type SessionStats struct {
	TotalRuns     int     `json:"totalRuns"`
	TotalTokens   int     `json:"totalTokens"`
	TotalCostYuan float64 `json:"totalCostYuan"`
	TotalDuration int64   `json:"totalDurationMs"`
}

// RuntimeStatus tracks the lifecycle of a serve runtime instance.
type RuntimeStatus string

const (
	RuntimeStarting RuntimeStatus = "starting"
	RuntimeReady    RuntimeStatus = "ready"
	RuntimeBusy     RuntimeStatus = "busy"
	RuntimeIdle     RuntimeStatus = "idle"
	RuntimeStopped  RuntimeStatus = "stopped"
	RuntimeError    RuntimeStatus = "error"
)

// CleanupPolicy determines when a runtime is destroyed.
type CleanupPolicy string

const (
	CleanupEphemeral CleanupPolicy = "ephemeral" // destroyed when pipeline ends
	CleanupRetained  CleanupPolicy = "retained"  // kept alive until explicitly stopped
	CleanupTTL       CleanupPolicy = "ttl"       // destroyed after idle timeout
)

// RuntimeState is the persistent state of a serve runtime instance.
type RuntimeState struct {
	RuntimeID      string        `json:"runtimeID"`
	SessionID      string        `json:"sessionID,omitempty"`
	AgentBindingID string        `json:"agentBindingID,omitempty"`
	NodeID         string        `json:"nodeID"`
	RunID          string        `json:"runID,omitempty"`
	Executor       string        `json:"executor"`
	Model          string        `json:"model"`
	Endpoint       string        `json:"endpoint"`
	Port           int           `json:"port"`
	PID            int           `json:"pid"`
	Status         RuntimeStatus `json:"status"`
	Error          string        `json:"error,omitempty"`
	CreatedAt      time.Time     `json:"createdAt"`
	LastActiveAt   time.Time     `json:"lastActiveAt"`
	CleanupPolicy  CleanupPolicy `json:"cleanupPolicy"`
	AccessMode     string        `json:"accessMode,omitempty"`    // browser | local_history | runtime_console
	ApprovalMode   string        `json:"approvalMode,omitempty"`  // ask | auto
	ExecutionMode  string        `json:"executionMode,omitempty"` // task | goal
	// ThreadID and TurnID expose retained WebSocket-provider context to the
	// orchestrator Runtime Console. They are provider identifiers, never tokens.
	ThreadID string `json:"threadID,omitempty"`
	TurnID   string `json:"turnID,omitempty"`
	// PermissionRequests lists tool-approval prompts parked because the node's
	// approval mode is "ask" and no human has decided yet. The Runtime Console
	// renders one card per entry; answering calls
	// AnswerMimoRuntimePermission / AnswerClaudeRuntimePermission.
	PermissionRequests []PermissionRequestInfo `json:"permissionRequests,omitempty"`
}

// PermissionRequestInfo is the public projection of one parked tool-approval
// prompt (mimo ACP request_permission or claude SDK can_use_tool).
type PermissionRequestInfo struct {
	RequestID string    `json:"requestId"`
	ToolName  string    `json:"toolName"`
	ToolInput string    `json:"toolInput,omitempty"` // trimmed JSON of the tool call
	SessionID string    `json:"sessionID,omitempty"`
	AskedAt   time.Time `json:"askedAt"`
}

// SessionState captures the full orchestrator state for a session (for persistence).
type SessionState struct {
	Nodes         []AgentNode             `json:"nodes"`
	Edges         []Edge                  `json:"edges"`
	ChatMessages  []ChatMsg               `json:"chatMessages"`
	RewrittenTask string                  `json:"rewrittenTask"`
	RuntimeStates map[string]RuntimeState `json:"runtimeStates,omitempty"`
}

// ChatMsg is a single chat message for persistence.
type ChatMsg struct {
	Role string `json:"role"`
	Text string `json:"text"`
	Meta string `json:"meta,omitempty"`
}

// ── Orchestration Session (P0) ──

// OrchestrationSession is the top-level container for a user's long-running work.
// It owns pipeline revisions, runs, agent bindings, and requirement conversations.
// OrchestrationSessionSummary is the lightweight history-list representation.
// Keep conversation and pipeline message arrays out of the list endpoint; they
// are loaded only when the user opens a specific session.
type OrchestrationSessionSummary struct {
	ID                string `json:"id"`
	Title             string `json:"title"`
	Workspace         string `json:"workspace"`
	Status            string `json:"status"`
	CreatedAt         string `json:"createdAt"`
	UpdatedAt         string `json:"updatedAt"`
	NativeSessionPath string `json:"nativeSessionPath,omitempty"`
	NativeSessionName string `json:"nativeSessionName,omitempty"`
}

type OrchestrationSession struct {
	ID                  string    `json:"id"`
	Title               string    `json:"title"`
	Workspace           string    `json:"workspace"`
	Status              string    `json:"status"` // active | paused | completed | archived | error
	CreatedAt           string    `json:"createdAt"`
	UpdatedAt           string    `json:"updatedAt"`
	NativeSessionPath   string    `json:"nativeSessionPath,omitempty"`
	NativeSessionName   string    `json:"nativeSessionName,omitempty"`
	RequirementMessages []ChatMsg `json:"requirementMessages"`
	ChatMessages        []ChatMsg `json:"chatMessages,omitempty"`
	PipelineMessages    []ChatMsg `json:"pipelineMessages,omitempty"`
	PipelineRevisionIDs []string  `json:"pipelineRevisionIDs"`
	CurrentPipelineID   string    `json:"currentPipelineID"`
	RunIDs              []string  `json:"runIDs"`
	CurrentRunID        string    `json:"currentRunID,omitempty"`
	AgentBindingIDs     []string  `json:"agentBindingIDs"`
	RuntimeIDs          []string  `json:"runtimeIDs,omitempty"`
	ActiveTask          string    `json:"activeTask,omitempty"`
	RewrittenTask       string    `json:"rewrittenTask,omitempty"`
}

// PipelineRevision is a versioned pipeline definition within a session.
type PipelineRevision struct {
	ID                string      `json:"id"`
	SessionID         string      `json:"sessionID"`
	Version           int         `json:"version"`
	Name              string      `json:"name"`
	Nodes             []AgentNode `json:"nodes"`
	Edges             []Edge      `json:"edges"`
	Source            string      `json:"source"` // flash_analysis | manual_edit | fork | migration
	BasedOnRevisionID string      `json:"basedOnRevisionID,omitempty"`
	LoopConfig        LoopConfig  `json:"loopConfig,omitempty"`
	CreatedAt         string      `json:"createdAt"`
	UpdatedAt         string      `json:"updatedAt"`
	Status            string      `json:"status"` // draft | active | superseded | archived
}

// NodeAttempt tracks one execution attempt of a node within a run.
type NodeAttempt struct {
	ID                string     `json:"id"`
	RunID             string     `json:"runID"`
	NodeID            string     `json:"nodeID"`
	IterationID       string     `json:"iterationID,omitempty"`
	AttemptNumber     int        `json:"attemptNumber"`
	Status            string     `json:"status"` // pending | running | complete | failed | blocked | canceled | interrupted
	Input             string     `json:"input,omitempty"`
	Output            string     `json:"output,omitempty"`
	Error             string     `json:"error,omitempty"`
	Stderr            string     `json:"stderr,omitempty"`
	Executor          string     `json:"executor"`
	Model             string     `json:"model"`
	Mode              string     `json:"mode"`
	Agent             string     `json:"agent,omitempty"`
	Skill             string     `json:"skill,omitempty"`
	AgentBindingID    string     `json:"agentBindingID"`
	ProviderSessionID string     `json:"providerSessionID,omitempty"`
	RuntimeID         string     `json:"runtimeID,omitempty"`
	StartedAt         string     `json:"startedAt,omitempty"`
	FinishedAt        string     `json:"finishedAt,omitempty"`
	TokenUsage        TokenUsage `json:"tokenUsage"`
}

// AgentBinding is the long-lived identity linking a pipeline node to its agent context.
type AgentBinding struct {
	ID                  string `json:"id"`
	SessionID           string `json:"sessionID"`
	NodeID              string `json:"nodeID"`
	Executor            string `json:"executor"`
	Model               string `json:"model"`
	Agent               string `json:"agent,omitempty"`
	Skill               string `json:"skill,omitempty"`
	Mode                string `json:"mode"`
	ContextPolicy       string `json:"contextPolicy"` // reuse | fresh_per_run | fresh_per_retry | fork
	ProviderSessionID   string `json:"providerSessionID,omitempty"`
	ProviderSessionPath string `json:"providerSessionPath,omitempty"`
	CurrentRuntimeID    string `json:"currentRuntimeID,omitempty"`
	LastRunID           string `json:"lastRunID,omitempty"`
	LastAttemptID       string `json:"lastAttemptID,omitempty"`
	CreatedAt           string `json:"createdAt"`
	UpdatedAt           string `json:"updatedAt"`
	Status              string `json:"status"` // active | detached | stopped | invalid | error
}

// ProviderSession records the native Reasonix/Mimo session information.
type ProviderSession struct {
	ID                 string `json:"id"`
	AgentBindingID     string `json:"agentBindingID"`
	Executor           string `json:"executor"`
	ExternalSessionID  string `json:"externalSessionID,omitempty"`
	SessionPath        string `json:"sessionPath,omitempty"`
	Workspace          string `json:"workspace,omitempty"`
	LastKnownRuntimeID string `json:"lastKnownRuntimeID,omitempty"`
	LastKnownEndpoint  string `json:"lastKnownEndpoint,omitempty"`
	CreatedAt          string `json:"createdAt"`
	UpdatedAt          string `json:"updatedAt"`
	Status             string `json:"status"` // active | lost | closed
}

// ExecutionOptions configures how a pipeline run is executed.
type ExecutionOptions struct {
	Trigger            string `json:"trigger"` // manual | retry | resume | loop | scheduled
	ParentRunID        string `json:"parentRunID,omitempty"`
	ReuseAgentSessions bool   `json:"reuseAgentSessions"`      // default true
	ContextPolicy      string `json:"contextPolicy,omitempty"` // reuse | fresh_per_run | fresh_per_retry
	ResumeRunID        string `json:"resumeRunID,omitempty"`
	// Workspace is snapshotted on PipelineRun creation. Execution must not
	// silently follow the process cwd after a session or browser changes it.
	Workspace string `json:"workspace,omitempty"`
}

// ── Loop Types (P7) ──

// LoopConfig configures loop behavior for a pipeline.
type LoopConfig struct {
	Enabled         bool   `json:"enabled"`
	Mode            string `json:"mode"`                      // none | review_decides | fixed
	MaxIterations   int    `json:"maxIterations"`             // review_decides: max loop count
	FixedIterations int    `json:"fixedIterations,omitempty"` // fixed: exact iteration count
	ReviewNodeID    string `json:"reviewNodeID"`              // node ID of the reviewer
	Protocol        string `json:"protocol"`                  // loop-review-v1
}

// Iteration status constants.
const (
	IterationRunning          = "running"
	IterationPassed           = "passed"
	IterationPassedByLimit    = "passed_by_limit"
	IterationCompletedByLimit = "completed_by_limit"
	IterationRevising         = "revising"
	IterationBlocked          = "blocked"
	IterationFailed           = "failed"
	IterationInterrupted      = "interrupted"
	IterationCanceled         = "canceled"
)

// LoopIteration tracks one iteration within a loop run.
type LoopIteration struct {
	ID              string `json:"id"`
	RunID           string `json:"runID"`
	Number          int    `json:"number"`
	Status          string `json:"status"` // see Iteration* constants
	InputTask       string `json:"inputTask,omitempty"`
	ReviewAttemptID string `json:"reviewAttemptID,omitempty"`
	Decision        string `json:"decision,omitempty"` // pass | revise | blocked
	NextTask        string `json:"nextTask,omitempty"`
	StartedAt       string `json:"startedAt"`
	FinishedAt      string `json:"finishedAt,omitempty"`
	Error           string `json:"error,omitempty"`
}

// ReviewDecision is the structured output from a reviewer in loop mode.
type ReviewDecision struct {
	SchemaVersion   string   `json:"schemaVersion"`
	Decision        string   `json:"decision"` // pass | revise | blocked
	Confidence      float64  `json:"confidence"`
	Summary         string   `json:"summary"`
	BlockingIssues  []string `json:"blockingIssues"`
	RequiredChanges []string `json:"requiredChanges"`
	NextTask        string   `json:"nextTask"`
	Evidence        []string `json:"evidence"`
}
