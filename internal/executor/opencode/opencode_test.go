package opencode

import "testing"

const sampleStream = `{"type":"step_start","timestamp":1785997454089,"sessionID":"ses_abc","part":{"type":"step-start"}}
{"type":"text","timestamp":1785997459262,"sessionID":"ses_abc","part":{"type":"text","text":"OK"}}
{"type":"step_finish","timestamp":1785997459473,"sessionID":"ses_abc","part":{"type":"step-finish","tokens":{"total":16012,"input":14217,"output":3,"cost":0}}}`

func TestParseRunOutput(t *testing.T) {
	res := ParseRunOutput([]byte(sampleStream))
	if res.Output != "OK" {
		t.Fatalf("output = %q, want %q", res.Output, "OK")
	}
	if res.SessionID != "ses_abc" {
		t.Fatalf("sessionID = %q, want ses_abc", res.SessionID)
	}
	if res.TotalTokens != 16012 {
		t.Fatalf("tokens = %d, want 16012", res.TotalTokens)
	}
	if res.Cost != 0 {
		t.Fatalf("cost = %v, want 0", res.Cost)
	}
}

func TestDiscoverBin(t *testing.T) {
	// Should not panic; on machines with opencode installed it returns a path.
	_ = DiscoverBin()
}
