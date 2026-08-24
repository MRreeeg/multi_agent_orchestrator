package orchestrator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestWatchTurnActivityFiresOnSilence(t *testing.T) {
	clock := newActivityClock()
	// Backdate the clock so the silence window has already elapsed.
	clock.ns.Store(time.Now().Add(-time.Hour).UnixNano())
	ctx, cancel, wd := watchTurnActivity(context.Background(), clock, 50*time.Millisecond, 0)
	defer cancel()
	deadline := time.After(2 * time.Second)
	for ctx.Err() == nil {
		select {
		case <-deadline:
			t.Fatal("watchdog did not fire on a silent clock")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !wd.fired.Load() {
		t.Fatal("watchdog cancelled the context without reporting a stall")
	}
	if !errors.Is(wd.Err(), ErrTurnIdleTimeout) {
		t.Fatalf("stall cause = %v, want ErrTurnIdleTimeout", wd.Err())
	}
}

func TestWatchTurnActivityRenewsOnTouch(t *testing.T) {
	clock := newActivityClock()
	ctx, cancel, wd := watchTurnActivity(context.Background(), clock, 60*time.Millisecond, 0)
	defer cancel()
	// Keep touching well past the idle window: the watchdog must never fire.
	stop := time.After(300 * time.Millisecond)
	for {
		select {
		case <-stop:
			if wd.fired.Load() {
				t.Fatal("watchdog fired despite continuous activity")
			}
			if ctx.Err() != nil {
				t.Fatalf("context cancelled despite activity: %v", ctx.Err())
			}
			return
		default:
		}
		clock.touch()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestWatchTurnActivityMaxDuration(t *testing.T) {
	clock := newActivityClock()
	ctx, cancel, wd := watchTurnActivity(context.Background(), clock, 0, 50*time.Millisecond)
	defer cancel()
	deadline := time.After(2 * time.Second)
	for ctx.Err() == nil {
		select {
		case <-deadline:
			t.Fatal("watchdog did not enforce the total-duration ceiling")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if !errors.Is(wd.Err(), ErrTurnMaxDuration) {
		t.Fatalf("stall cause = %v, want ErrTurnMaxDuration", wd.Err())
	}
}

func TestWatchTurnActivityParentCancelIsNotAStall(t *testing.T) {
	clock := newActivityClock()
	parent, parentCancel := context.WithCancel(context.Background())
	ctx, cancel, wd := watchTurnActivity(parent, clock, time.Hour, time.Hour)
	defer cancel()
	parentCancel()
	deadline := time.After(2 * time.Second)
	for ctx.Err() == nil {
		select {
		case <-deadline:
			t.Fatal("child context did not follow parent cancellation")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	if wd.fired.Load() || wd.Err() != nil {
		t.Fatal("parent cancellation must not be reported as a watchdog stall")
	}
}

func TestStallErrorsAreNotContextErrors(t *testing.T) {
	// loop.go force-stops retained runtimes when an execution error is
	// context.Canceled / context.DeadlineExceeded. Stall errors must stay
	// distinct from those, or every watchdog cut would tear down the runtime
	// the user is trying to resume.
	for _, err := range []error{ErrTurnIdleTimeout, ErrTurnMaxDuration} {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%v must not match a context error", err)
		}
	}
}
