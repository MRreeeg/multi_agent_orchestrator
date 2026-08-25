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

// 推理块体积分段：长思考不再涨成一大坨——攒满阈值立即落一条，后续开新块。
func TestConsoleStreamCoalescerReasoningSplitsBySize(t *testing.T) {
	flushed, flush := collectFlushed(8)
	c := newConsoleStreamCoalescer(time.Hour, flush) // 无静默、无边界的持续流
	defer c.stop()

	chunk := strings.Repeat("思", 1000)
	for i := 0; i < 7; i++ { // 累计 7000 字符 ≥ 阈值 3000 → 至少分段两次
		c.append("agent_thought", "reasoning", "reasoning", chunk)
	}
	got := 0
	var sizes []int
drain:
	for {
		select {
		case evt := <-flushed:
			got++
			sizes = append(sizes, len([]rune(evt.Text)))
		default:
			break drain
		}
	}
	if got < 2 {
		t.Fatalf("reasoning stream of 7000 chars should split into >=2 blocks, got %d (%v)", got, sizes)
	}
	for _, s := range sizes[:got-1] { // 除最后一块外都应接近阈值（非巨型块）
		if s > consoleReasoningMaxChars+1000 {
			t.Fatalf("block oversized: %d chars", s)
		}
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
