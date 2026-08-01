package orchestrator

import (
	"strings"
	"testing"
	"time"

	codexclient "reasonix/internal/executor/codex"
	mimoclient "reasonix/internal/executor/mimo"
)

func collectFlushed(capacity int) (chan RuntimeConsoleEvent, func(RuntimeConsoleEvent)) {
	ch := make(chan RuntimeConsoleEvent, capacity)
	return ch, func(evt RuntimeConsoleEvent) { ch <- evt }
}

func TestConsoleStreamCoalescerMergesDeltasUntilBoundary(t *testing.T) {
	flushed, flush := collectFlushed(4)
	c := newConsoleStreamCoalescer(time.Hour, flush)
	defer c.stop()

	for _, frag := range []string{"App", "/", "Local", "/", "go-build"} {
		c.append("agent_message", "msg-1", "assistant", frag)
	}
	select {
	case evt := <-flushed:
		t.Fatalf("delta stream flushed before boundary: %#v", evt)
	default:
	}
	c.flushNow()
	select {
	case evt := <-flushed:
		if evt.Text != "App/Local/go-build" || evt.Category != "assistant" || evt.Method != "agent_message" {
			t.Fatalf("flushed event = %#v", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("boundary flush did not emit the consolidated event")
	}
}

func TestConsoleStreamCoalescerFlushesOnKeyChange(t *testing.T) {
	flushed, flush := collectFlushed(4)
	c := newConsoleStreamCoalescer(time.Hour, flush)
	defer c.stop()

	c.append("agent_message", "msg-1", "assistant", "first")
	c.append("agent_message", "msg-2", "assistant", "second")
	// msg-1 must be flushed as a separate block before msg-2 starts.
	select {
	case evt := <-flushed:
		if evt.Text != "first" {
			t.Fatalf("first block = %#v", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("key change did not flush the previous block")
	}
	c.flushNow()
	select {
	case evt := <-flushed:
		if evt.Text != "second" {
			t.Fatalf("second block = %#v", evt)
		}
	case <-time.After(time.Second):
		t.Fatal("second block not flushed")
	}
}

func TestConsoleStreamCoalescerQuietFlush(t *testing.T) {
	flushed, flush := collectFlushed(2)
	c := newConsoleStreamCoalescer(120*time.Millisecond, flush)
	defer c.stop()
	c.append("agent_thought", "msg-1", "reasoning", "thinking...")
	select {
	case evt := <-flushed:
		if evt.Category != "reasoning" || !strings.Contains(evt.Text, "thinking") {
			t.Fatalf("quiet flush = %#v", evt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("quiet timer did not flush")
	}
}

func TestConsoleStreamCoalescerStopDropsPending(t *testing.T) {
	flushed, flush := collectFlushed(2)
	c := newConsoleStreamCoalescer(100*time.Millisecond, flush)
	c.append("agent_message", "msg-1", "assistant", "dropped")
	c.stop()
	select {
	case evt := <-flushed:
		t.Fatalf("stopped coalescer flushed: %#v", evt)
	case <-time.After(300 * time.Millisecond):
	}
}

func TestCodexStreamPartClassifiesDeltas(t *testing.T) {
	cases := []struct {
		method string
		params string
		delta  bool
		cat    string
		key    string
	}{
		{"item/reasoning/textDelta", `{"turnId":"t1","delta":"x"}`, true, "reasoning", "t1"},
		{"item/agentMessage/delta", `{"turnId":"t1","delta":"x"}`, true, "assistant", "t1"},
		{"item/input/textDelta", `{"turnId":"t1","delta":"x"}`, true, "prompt", "t1"},
		{"turn/completed", `{"threadId":"t","turn":{"id":"t1"}}`, false, "", ""},
	}
	for _, tc := range cases {
		method, key, cat, ok := codexStreamPart(codexclient.AppServerEvent{Method: tc.method, Params: []byte(tc.params)})
		if ok != tc.delta || cat != tc.cat || key != tc.key {
			t.Fatalf("codexStreamPart(%s) = (%q,%q,%q,%v), want delta=%v cat=%q key=%q", tc.method, method, key, cat, ok, tc.delta, tc.cat, tc.key)
		}
	}
}

func TestMimoStreamPartClassifiesChunks(t *testing.T) {
	if method, key, cat, ok := mimoStreamPart(mimoclient.AcpEvent{Update: "agent_message_chunk", MessageID: "m1", Text: "ok"}); !ok || method != "agent_message" || key != "m1" || cat != "assistant" {
		t.Fatalf("agent_message_chunk = %q,%q,%q,%v", method, key, cat, ok)
	}
	if method, key, cat, ok := mimoStreamPart(mimoclient.AcpEvent{Update: "agent_thought_chunk", MessageID: "m1", Text: "t"}); !ok || method != "agent_thought" || key != "m1" || cat != "reasoning" {
		t.Fatalf("agent_thought_chunk = %q,%q,%q,%v", method, key, cat, ok)
	}
	if _, _, _, ok := mimoStreamPart(mimoclient.AcpEvent{Update: "usage_update"}); ok {
		t.Fatal("usage_update must not be treated as a delta")
	}
}
