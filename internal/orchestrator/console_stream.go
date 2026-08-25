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

// consoleReasoningMaxChars 是单个推理事件块的体积分段阈值。稳定 key 合并后，
// 持续流式会让"静默 400ms 才落盘"永不触发，整段思考会涨成一大坨；按体积
// 强制分段，让 Console 呈现为多个可折叠的中等段落而非一条巨型记录。
const consoleReasoningMaxChars = 3000

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
	// 推理块体积分段：攒满阈值立即落一条（解锁后 flush，避免自锁），
	// 后续推理内容开新块。
	overflow := category == "reasoning" && c.text.Len() >= consoleReasoningMaxChars
	if overflow {
		if c.timer != nil {
			c.timer.Stop()
			c.timer = nil
		}
		c.mu.Unlock()
		c.flushNow()
		return
	}
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
