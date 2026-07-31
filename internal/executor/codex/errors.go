package codex

import "errors"

var (
	// ErrExecutorStart is returned when the Codex CLI process fails to start.
	ErrExecutorStart = errors.New("codex: failed to start process")

	// ErrExecutorTimeout is returned when the execution exceeds the configured timeout.
	ErrExecutorTimeout = errors.New("codex: execution timed out")

	// ErrExecutorCanceled is returned when the context is canceled.
	ErrExecutorCanceled = errors.New("codex: execution canceled")

	// ErrExecutorExit is returned when the process exits with a non-zero exit code.
	ErrExecutorExit = errors.New("codex: non-zero exit code")

	// ErrExecutorEmptyOutput is returned when the process produces no output.
	ErrExecutorEmptyOutput = errors.New("codex: empty output")

	// ErrExecutorProtocol is returned when the output cannot be parsed.
	ErrExecutorProtocol = errors.New("codex: protocol error")

	// ErrAppServerTurnInterrupted is returned when a retained app-server Turn
	// was explicitly interrupted. The Thread and Runtime remain reusable.
	ErrAppServerTurnInterrupted = errors.New("codex: app-server turn interrupted")
)
