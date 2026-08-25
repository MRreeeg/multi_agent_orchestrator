package orchestrator

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Activity-based turn governance replaces fixed wall-clock kill switches for
// retained provider runtimes (opencode serve / codex app-server / mimo acp /
// claude stream-json). A node that is legitimately working streams events the
// whole time (text deltas, tool calls, usage updates), while a genuinely hung
// one goes silent. So the watchdog renews on every liveness signal and only
// fires when the stream goes quiet or the turn grossly exceeds its ceiling.
//
// Two knobs, both env-overridable:
//
//	REASONIX_TURN_IDLE_TIMEOUT   silence window that marks a turn stalled
//	                             (default 15m; tolerates long silent tool
//	                             calls while still catching wedged turns)
//	                             (default 5m)
//	REASONIX_TURN_MAX_DURATION   absolute ceiling for one turn regardless of
//	                             activity (default 2h; spec.TurnTimeout may
//	                             override per node)
var (
	turnIdleTimeoutDefault = envDuration("REASONIX_TURN_IDLE_TIMEOUT", 15*time.Minute)
	turnMaxDurationDefault = envDuration("REASONIX_TURN_MAX_DURATION", 2*time.Hour)
)

// Sentinel stall errors. They deliberately do NOT wrap context.Canceled /
// context.DeadlineExceeded: loop.go treats those as "the scheduler killed this
// node" and force-stops the retained runtime in response. A watchdog stall is
// a property of the provider turn, not of the orchestrator's schedule — the
// runtime must stay alive (status=error) so the session can be resumed or
// manually inspected instead of being torn down.
var (
	ErrTurnIdleTimeout = errors.New("provider turn stalled: no activity within the idle window")
	ErrTurnMaxDuration = errors.New("provider turn exceeded its maximum allowed duration")
)

func envDuration(name string, def time.Duration) time.Duration {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	if d, err := time.ParseDuration(raw); err == nil && d > 0 {
		return d
	}
	return def
}

// activityClock records the unix-nano timestamp of the most recent liveness
// signal from a runtime's event stream. Safe for concurrent use.
type activityClock struct {
	ns atomic.Int64
}

func newActivityClock() *activityClock {
	c := &activityClock{}
	c.touch()
	return c
}

// touch is nil-receiver safe: runtimes constructed outside the manager
// (unit tests) may not carry a clock.
func (c *activityClock) touch() {
	if c == nil {
		return
	}
	c.ns.Store(time.Now().UnixNano())
}

func (c *activityClock) last() time.Time {
	if c == nil {
		return time.Time{}
	}
	return time.Unix(0, c.ns.Load())
}

// activityWatchdog reports whether the watchdog (not the parent context)
// cancelled the turn, plus which limit fired.
type activityWatchdog struct {
	fired atomic.Bool
	mu    sync.Mutex
	cause error
}

// Err returns the stall error when the watchdog fired, nil otherwise.
func (w *activityWatchdog) Err() error {
	if !w.fired.Load() {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cause == nil {
		return ErrTurnIdleTimeout
	}
	return w.cause
}

// watchTurnActivity derives a cancellable context from parent that is cancelled
// when either the activity clock stays silent longer than idle or the turn
// outlives total. A non-positive idle or total disables that respective limit.
// The returned CancelFunc must always be called to release the watcher.
func watchTurnActivity(parent context.Context, clock *activityClock, idle, total time.Duration) (context.Context, context.CancelFunc, *activityWatchdog) {
	ctx, cancel := context.WithCancel(parent)
	wd := &activityWatchdog{}
	if clock == nil || (idle <= 0 && total <= 0) {
		return ctx, cancel, wd
	}
	go func() {
		defer cancel()
		started := time.Now()
		tick := time.Duration(0)
		if idle > 0 {
			tick = idle / 4
		}
		if total > 0 {
			half := total / 4
			if tick == 0 || half < tick {
				tick = half
			}
		}
		if tick > 30*time.Second {
			tick = 30 * time.Second
		}
		if tick < 100*time.Millisecond {
			tick = 100 * time.Millisecond
		}
		ticker := time.NewTicker(tick)
		defer ticker.Stop()
		for {
			select {
			case <-parent.Done():
				// Parent cancellation propagates through ctx on its own; do
				// not claim it as a stall.
				return
			case <-ticker.C:
				if idle > 0 && time.Since(clock.last()) >= idle {
					wd.mu.Lock()
					wd.cause = ErrTurnIdleTimeout
					wd.mu.Unlock()
					wd.fired.Store(true)
					return
				}
				if total > 0 && time.Since(started) >= total {
					wd.mu.Lock()
					wd.cause = ErrTurnMaxDuration
					wd.mu.Unlock()
					wd.fired.Store(true)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()
	return ctx, cancel, wd
}
