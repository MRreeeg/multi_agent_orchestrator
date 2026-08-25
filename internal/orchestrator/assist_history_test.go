package orchestrator

import (
	"os"
	"path/filepath"
	"testing"
)

// 辅助手历史持久化：写入→读取往返、倒序、损坏行容忍、空文件语义。
func TestAssistHistoryRoundTrip(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", dir)
	if DataRoot() != filepath.Clean(dir) {
		t.Fatalf("DataRoot not overridden: %s", DataRoot())
	}

	recs, err := (&Store{}).AssistHistory(0)
	if err != nil || len(recs) != 0 {
		t.Fatalf("empty store: got %d recs, err=%v", len(recs), err)
	}

	for i, task := range []string{"task-A", "task-B", "task-C"} {
		ok := appendAssistHistory(AssistHistoryRecord{
			Task:     task,
			ModelRef: "opencode/mimo-v2.5-free",
			Driver:   "opencode",
			OK:       true,
			Result:   "result-" + task,
			Error:    "",
		})
		if ok != nil {
			t.Fatalf("append %d: %v", i, ok)
		}
	}
	_ = appendAssistHistory(AssistHistoryRecord{Task: "task-fail", Error: "boom"})

	got, err := (&Store{}).AssistHistory(10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("want 4 records, got %d", len(got))
	}
	// 时间倒序：最新在前 → task-fail 第一条。
	if got[0].Task != "task-fail" || got[0].OK {
		t.Fatalf("newest-first violated: %+v", got[0])
	}
	if got[3].Task != "task-A" || got[3].Result != "result-task-A" {
		t.Fatalf("oldest record mismatch: %+v", got[3])
	}

	// limit 截断：只取最近 2 条（写入序 A,B,C,fail → 倒序 [fail, C]）。
	got2, _ := (&Store{}).AssistHistory(2)
	if len(got2) != 2 || got2[0].Task != "task-fail" || got2[1].Task != "task-C" {
		t.Fatalf("limit=2 mismatch: %+v", got2)
	}

	// 损坏行容忍：插入垃圾行后仍能读出合法记录。
	p := assistHistoryPath()
	content, _ := os.ReadFile(p)
	_ = os.WriteFile(p, append([]byte("not-json\n"), content...), 0o644)
	got3, err := (&Store{}).AssistHistory(0)
	if err != nil || len(got3) != 4 {
		t.Fatalf("corrupt-line tolerance: %d recs, err=%v", len(got3), err)
	}
}

func TestTruncateRunes(t *testing.T) {
	long := make([]rune, assistTaskMaxRune+100)
	for i := range long {
		long[i] = '中'
	}
	out := truncateRunes(string(long), assistTaskMaxRune)
	if rn := len([]rune(out)); rn <= assistTaskMaxRune {
		t.Fatalf("truncated output too short: %d", rn)
	}
	if truncateRunes("  hi  ", 10) != "hi" {
		t.Fatal("trim behavior changed")
	}
}
