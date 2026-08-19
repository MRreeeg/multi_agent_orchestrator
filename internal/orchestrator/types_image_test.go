package orchestrator

import (
	"encoding/json"
	"testing"
)

func TestChatMsgImagesRoundTrip(t *testing.T) {
	m := ChatMsg{
		Role:   "user",
		Text:   "看图",
		Meta:   "",
		Images: []ChatImage{{ID: "123_abcd", Name: "shot.png"}},
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var back ChatMsg
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Images) != 1 || back.Images[0].ID != "123_abcd" || back.Images[0].Name != "shot.png" {
		t.Fatalf("images = %+v", back.Images)
	}
}

func TestChatMsgImagesOmitEmpty(t *testing.T) {
	m := ChatMsg{Role: "ai", Text: "x"}
	b, _ := json.Marshal(m)
	if str := string(b); str != `{"role":"ai","text":"x"}` {
		t.Fatalf("marshal = %s", str)
	}
}
