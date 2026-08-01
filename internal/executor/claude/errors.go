// Package claude implements a Claude Code executor for the orchestrator.
//
// Two execution modes are supported, mirroring the codex and mimo executors:
//
//   - run: one-shot `claude -p --output-format json` processes. Each node
//     attempt is one CLI invocation; the JSON result carries the assistant
//     text and the session id for cross-run resume via `--resume`.
//   - serve: a retained `claude -p --input-format stream-json
//     --output-format stream-json --verbose` process driven over its
//     stdin/stdout pipes using the Claude Code Agent SDK wire protocol
//     (JSON Lines). The retained session keeps conversation history and can
//     be interrupted (control_request interrupt) and resumed (`--resume`).
//
// The browser never dials the provider directly; the orchestrator proxies
// console state through its HTTP/SSE Runtime Console API.
package claude

import "errors"

var (
	// ErrExecutorStart is returned when the Claude CLI process fails to start.
	ErrExecutorStart = errors.New("claude: failed to start process")

	// ErrExecutorTimeout is returned when the execution exceeds the configured timeout.
	ErrExecutorTimeout = errors.New("claude: execution timed out")

	// ErrExecutorCanceled is returned when the context is canceled.
	ErrExecutorCanceled = errors.New("claude: execution canceled")

	// ErrExecutorExit is returned when the process exits with a non-zero exit code.
	ErrExecutorExit = errors.New("claude: non-zero exit code")

	// ErrExecutorEmptyOutput is returned when the process produces no output.
	ErrExecutorEmptyOutput = errors.New("claude: empty output")

	// ErrExecutorProtocol is returned when the output cannot be parsed.
	ErrExecutorProtocol = errors.New("claude: protocol error")

	// ErrTurnInterrupted is returned when a retained turn was explicitly
	// interrupted. The session and runtime remain reusable for a later turn.
	ErrTurnInterrupted = errors.New("claude: turn interrupted")
)
