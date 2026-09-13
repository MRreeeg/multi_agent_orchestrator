package orchestrator

import "errors"

// ErrRuntimeBusy marks a manual Runtime Console send that arrived while the
// retained runtime is executing a node turn. The serve layer maps it to HTTP
// 409 so the frontend can offer "interrupt and send / queue until idle"
// instead of showing a generic failure — previously the prompt was fired into
// a busy server and silently vanished, reading as "the send did nothing".
//
// It deliberately does not wrap context errors: loop.go treats those as
// scheduler-driven kills.
var ErrRuntimeBusy = errors.New("runtime busy: a node turn is active")

// ErrRuntimeInterrupted marks a provider turn that was deliberately stopped by
// the operator from the Runtime Console ("interrupt current turn"). It is
// distinct from a context error: the retained runtime and its session/thread
// stay alive and usable, and the run should be recorded as interrupted
// (resumable) rather than failed. Without this sentinel the interrupt is
// indistinguishable from a real failure — the runtime reports "completed
// without assistant output" and the user reads it as the feature being broken.
var ErrRuntimeInterrupted = errors.New("runtime turn interrupted by user in Runtime Console")
