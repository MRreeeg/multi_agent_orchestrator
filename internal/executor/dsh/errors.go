// Package dsh implements a DeepSeek Harness (DSH) executor for the
// orchestrator.
//
// DSH is exposed to the orchestrator through its one-shot profile:
//
//	dsh --profile headless "<task>"
//
// which creates one fresh persisted session, drives it to quiescence, prints
// the final assistant message to stdout, and exits 0 on a completed turn or 1
// on an error. There is no retained serve protocol to speak (no app-server /
// stream-json equivalent), so this executor is run-only: every node attempt is
// one CLI invocation with no cross-attempt session resume.
//
// Model, workspace, and sandbox semantics differ from the codex/claude CLIs:
//
//   - The model is not a `--model` flag; DSH reads its default model from its
//     own harness home (`$DSH_HOME/settings.yaml` → `agent-default-model`).
//     This executor maps a node model onto a temporary `--patch` overlay for
//     the `agent-default-model` composition row (see ExecOptions.Model).
//   - The workspace is the process cwd (set via cmd.Dir), not a flag.
//   - Tool approval is non-interactive in headless mode, so the sandbox /
//     approval boundary is expressed through the DSH_PERMISSION_MODE env var.
//
// The browser never dials DSH directly; the orchestrator captures stdout and
// stderr from the one-shot process.
package dsh

import "errors"

var (
	// ErrExecutorStart is returned when the dsh process fails to start.
	ErrExecutorStart = errors.New("dsh: failed to start process")

	// ErrExecutorTimeout is returned when the execution exceeds the configured timeout.
	ErrExecutorTimeout = errors.New("dsh: execution timed out")

	// ErrExecutorCanceled is returned when the context is canceled.
	ErrExecutorCanceled = errors.New("dsh: execution canceled")

	// ErrExecutorExit is returned when the process exits with a non-zero exit code.
	ErrExecutorExit = errors.New("dsh: non-zero exit code")

	// ErrExecutorEmptyOutput is returned when the process produces no output.
	ErrExecutorEmptyOutput = errors.New("dsh: empty output")

	// ErrExecutorProtocol is returned when a nil result is produced without an error.
	ErrExecutorProtocol = errors.New("dsh: protocol error")
)
