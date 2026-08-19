package serve

import "testing"

func TestModelTypeDesc(t *testing.T) {
	cases := []struct{ name, want string }{
		{"xiaomi/mimo-v2.5", "视觉 ✓"},
		{"mimo-v2.5", "视觉 ✓"},
		{"mimo/mimo-auto", "自动"},
		{"xiaomi/mimo-v2.5-pro", "对话"},
		{"xiaomi/mimo-v2.5-pro-ultraspeed", "对话"},
		{"deepseek/deepseek-chat", "对话"},
		{"deepseek-v4-flash", "对话"},
		{"deepseek-reasoner", "推理"},
		{"opencode-go/glm-5.3", "视觉 ✓"},
		{"claude-opus-4-8[1M]", "对话"},
		{"o3", "推理"},
		{"mystery-model", ""},
		{"", ""},
	}
	for _, c := range cases {
		if got := modelTypeDesc(c.name); got != c.want {
			t.Errorf("modelTypeDesc(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}