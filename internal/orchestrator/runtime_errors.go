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
