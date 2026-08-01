// console_stream.go implements delta coalescing for the Runtime Console.
//
// Provider event streams (Codex app-server, Mimo ACP) emit one event per tiny
// token delta. Rendering every delta as its own console row floods the UI with
// fragments such as "App", "/", "Local". The coalescer merges deltas that
// belong to the same logical message into one consolidated event, flushed when
// a non-delta boundary event arrives, when the message identity changes, or
// after a short quiet period.
package orchestrator

import (
	"strings"
	"sync"
	"time"
)

// consoleStreamCoalescer buffers streaming text deltas into a single
// RuntimeConsoleEvent. The flush callback is invoked exactly once per
// consolidated block and must not call back into the coalescer.
type consoleStreamCoalescer struct {
	mu         sync.Mutex
	timer      *time.Timer
	method     string
	key        string
	category   string
	text       strings.Builder
	at         time.Time
	flushAfter time.Duration
	flush      func(RuntimeConsoleEvent)
	stopped    bool
}

func newConsoleStreamCoalescer(flushAfter time.Duration, flush func(RuntimeConsoleEvent)) *consoleStreamCoalescer {
	if flushAfter <= 0 {
		flushAfter = 400 * time.Millisecond
	}
	return &consoleStreamCoalescer{flushAfter: flushAfter, flush: flush}
}

// append buffers one streaming delta. method is the canonical console method,
// key groups deltas of the same message, category is "reasoning"/"assistant".
func (c *consoleStreamCoalescer) append(method, key, category, text string) {
	if strings.TrimSpace(text) == "" {
		return
	}
	c.mu.Lock()
	if c.stopped {
		c.mu.Unlock()
		return
	}
	if c.text.Len() > 0 && (c.method != method || c.key != key) {
		// A different message started: flush the previous block first.
		c.mu.Unlock()
		c.flushNow()
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			return
		}
		c.method, c.key, c.category, c.at = method, key, category, time.Now()
	} else if c.text.Len() == 0 {
		c.method, c.key, c.category, c.at = method, key, category, time.Now()
	}
	c.text.WriteString(text)
	if c.timer == nil {
		c.timer = time.AfterFunc(c.flushAfter, c.flushNow)
	} else {
		c.timer.Reset(c.flushAfter)
	}
	c.mu.Unlock()
}

// flushNow emits the buffered block immediately (timer goroutine or boundary
// event). The flush callback runs without holding the coalescer lock so the
// runtime event buffer can be updated without deadlock.
func (c *consoleStreamCoalescer) flushNow() {
	var pending *RuntimeConsoleEvent
	c.mu.Lock()
	if !c.stopped && c.text.Len() > 0 {
		evt := RuntimeConsoleEvent{At: c.at, Level: "info", Method: c.method, Text: c.text.String(), Category: c.category}
		c.text.Reset()
		if c.timer != nil {
			c.timer.Stop()
			c.timer = nil
		}
		pending = &evt
	}
	c.mu.Unlock()
	if pending != nil && c.flush != nil {
		c.flush(*pending)
	}
}

// stop cancels the pending timer and drops the buffer. Called when a runtime
// is stopped so a late timer cannot emit events for a dead runtime.
func (c *consoleStreamCoalescer) stop() {
	c.mu.Lock()
	c.stopped = true
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.text.Reset()
	c.mu.Unlock()
}
