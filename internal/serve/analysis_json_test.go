package serve

import "testing"

func TestExtractLastJSONObject(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantOK  bool
	}{
		{name: "bare single line", in: `{"summary":"ok"}`, want: `{"summary":"ok"}`, wantOK: true},
		{name: "leading and trailing chatter", in: "好的，以下是分析结果：\n{\"summary\":\"完成\"}\n希望有帮助", want: `{"summary":"完成"}`, wantOK: true},
		{name: "markdown fence", in: "```json\n{\"summary\":\"fenced\",\"blocking\":[]}\n```", want: `{"summary":"fenced","blocking":[]}`, wantOK: true},
		{name: "pretty printed multiline", in: "{\n  \"summary\": \"pretty\",\n  \"blocking\": []\n}", want: "{\n  \"summary\": \"pretty\",\n  \"blocking\": []\n}", wantOK: true},
		{name: "braces inside strings do not break balance", in: "说明 {不是JSON}\n{\"summary\":\"含 } 花括号 \\\"引用\\\"\",\"note\":\"{fake}\"}", want: `{"summary":"含 } 花括号 \"引用\"","note":"{fake}"}`, wantOK: true},
		{name: "last object wins", in: "{\"summary\":\"first\"}\n中间文字\n{\"summary\":\"second\"}", want: `{"summary":"second"}`, wantOK: true},
		{name: "no json at all", in: "我觉得这个任务还需要更多信息才能分析。", wantOK: false},
		{name: "empty input", in: "", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := extractLastJSONObject(tt.in)
			if ok != tt.wantOK {
				t.Fatalf("extractLastJSONObject() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && string(got) != tt.want {
				t.Fatalf("extractLastJSONObject() = %q, want %q", got, tt.want)
			}
		})
	}
}
