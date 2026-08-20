package orchestrator

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestModelSupportsVision(t *testing.T) {
	cases := []struct {
		model string
		want  bool
	}{
		{"mimo-v2.5", true},
		{"mimo-v2.5-pro", false},
		{"glm-4v", true},
		{"claude-sonnet-4-6", true},
		{"Claude-Opus-4", true},
		{"deepseek-chat", false},
		{"deepseek-v4-flash", false},
		{"", false},
	}
	for _, c := range cases {
		if got := modelSupportsVision(c.model); got != c.want {
			t.Errorf("modelSupportsVision(%q) = %v, want %v", c.model, got, c.want)
		}
	}
}

func TestTaskReferencesImages(t *testing.T) {
	refs := []ImageRef{{ID: "1730000000000_abcdef01", Name: "效果图1.1.png"}}
	cases := []struct {
		task string
		want bool
	}{
		{"请读取 效果图1.1.png 并描述", true},
		{"比对 效果图2.1.png 与设计稿", true},
		{"截图见 screenshot.jpg", true},
		{"没有任何图片，只改代码", false},
		{"", false},
	}
	for _, c := range cases {
		if got := taskReferencesImages(c.task, refs); got != c.want {
			t.Errorf("taskReferencesImages(%q) = %v, want %v", c.task, got, c.want)
		}
	}
}

// writeTestAttachment writes a fake attachment file under an isolated data
// root and returns the resolved path.
func writeTestAttachment(t *testing.T, id string) string {
	t.Helper()
	root := t.TempDir()
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)
	dir := filepath.Join(root, "attachments")
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, id+".png")
	if err := os.WriteFile(p, []byte("fake"), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestImageAttachmentPaths(t *testing.T) {
	id := "1730000000000_abcdef01"
	p := writeTestAttachment(t, id)
	refs := []ImageRef{{ID: id, Name: "效果图1.1.png"}, {ID: "bad/../glob", Name: "x.png"}}
	got := imageAttachmentPaths(refs)
	if len(got) != 1 || got[0] != p {
		t.Fatalf("imageAttachmentPaths = %v, want [%s]", got, p)
	}
}

func TestAutoVisionInject(t *testing.T) {
	id := "1730000000000_abcdef01"
	imgPath := writeTestAttachment(t, id)

	store := NewStore()
	run := &PipelineRun{
		Images: []ImageRef{{ID: id, Name: "效果图1.1.png"}},
	}
	task := "架构师方案：请读取 效果图1.1.png 并按模板输出"

	// 1. Assist disabled → path list injected, no dispatch.
	noAssist := &AgentNode{Type: NodeExecutor, Label: "执行者", Model: "deepseek-chat"}
	out := store.autoVisionInject(context.Background(), run, noAssist, task)
	if !strings.Contains(out, imgPath) {
		t.Errorf("assist-off should inject attachment path; output:\n%s", out)
	}
	if strings.Contains(out, "辅助手自动识图结果") {
		t.Errorf("assist-off must NOT auto delegate; output:\n%s", out)
	}

	// 2. Assist on + non-vision model → automatic delegation with injected result.
	var gotDispatch AssistDispatchOptions
	dispatched := false
	autoVisionDispatch = func(_ context.Context, _ *Store, opts AssistDispatchOptions) (*AssistDispatchResult, error) {
		dispatched = true
		gotDispatch = opts
		return &AssistDispatchResult{
			RuntimeID: "rt_test",
			Port:      12345,
			ModelRef:  "opencode/mimo-v2.5",
			Result:    "[图片 效果图1.1.png]: 这是一张 UI 设计稿，主色调蓝色，包含登录表单。",
		}, nil
	}
	defer func() { autoVisionDispatch = func(ctx context.Context, s *Store, opts AssistDispatchOptions) (*AssistDispatchResult, error) {
		return s.AssistDispatch(ctx, opts)
	} }()
	assistOn := &AgentNode{Type: NodeExecutor, Label: "执行者", Model: "deepseek-chat", Assist: &AssistConfig{Enabled: true}}
	out = store.autoVisionInject(context.Background(), run, assistOn, task)
	if !dispatched {
		t.Fatal("assist-on non-vision executor should auto delegate")
	}
	if len(gotDispatch.Images) != 1 || gotDispatch.Images[0] != imgPath {
		t.Errorf("dispatch images = %v, want [%s]", gotDispatch.Images, imgPath)
	}
	if !strings.Contains(out, "辅助手自动识图结果") || !strings.Contains(out, "这是一张 UI 设计稿") {
		t.Errorf("auto result must be injected; output:\n%s", out)
	}

	// 3. Vision-capable model → path list only, no delegation.
	vision := &AgentNode{Type: NodeExecutor, Label: "执行者", Model: "mimo-v2.5", Assist: &AssistConfig{Enabled: true}}
	dispatched = false
	out = store.autoVisionInject(context.Background(), run, vision, task)
	if dispatched {
		t.Error("vision model must not auto delegate")
	}
	if !strings.Contains(out, imgPath) {
		t.Errorf("vision model should still get attachment path; output:\n%s", out)
	}

	// 4. Dispatch failure → honest degradation, task still executes.
	autoVisionDispatch = func(_ context.Context, _ *Store, _ AssistDispatchOptions) (*AssistDispatchResult, error) {
		dispatched = true
		return nil, context.DeadlineExceeded
	}
	out = store.autoVisionInject(context.Background(), run, assistOn, task)
	if !strings.Contains(out, "辅助手自动识图失败") || !strings.Contains(out, "禁止编造") {
		t.Errorf("failure must degrade honestly; output:\n%s", out)
	}

	// 5. No image reference in task → untouched.
	dispatched = false
	out = store.autoVisionInject(context.Background(), run, assistOn, "纯代码任务，无图片")
	if dispatched || strings.Contains(out, "辅助手自动识图") {
		t.Errorf("task without image reference must be untouched; output:\n%s", out)
	}
}

func TestAutoVisionInjectWorkspaceImages(t *testing.T) {
	// 图片直接放在 run 工作目录（未走上传附件通道）：Orchestrator 必须能
	// 在工作目录定位并委派，而不是跳过。
	root := t.TempDir()
	t.Setenv("REASONIX_ORCHESTRATOR_DATA_DIR", root)
	ws := filepath.Join(root, "project")
	if err := os.MkdirAll(filepath.Join(ws, "assets"), 0755); err != nil {
		t.Fatal(err)
	}
	img1 := filepath.Join(ws, "assets", "效果图1.1.png")
	img2 := filepath.Join(ws, "效果图2.1.png")
	for _, p := range []string{img1, img2} {
		if err := os.WriteFile(p, []byte("fake"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	store := NewStore()
	run := &PipelineRun{ExecOptions: ExecutionOptions{Workspace: ws}} // 无 Images
	task := "请读取 assets/效果图1.1.png 与 效果图2.1.png 并按模板输出"

	var gotImages []string
	autoVisionDispatch = func(_ context.Context, _ *Store, opts AssistDispatchOptions) (*AssistDispatchResult, error) {
		gotImages = append(gotImages, opts.Images...)
		return &AssistDispatchResult{RuntimeID: "rt_t", Port: 1, ModelRef: "opencode/mimo-v2.5",
			Result: "[图片 效果图1.1.png]: 蓝色主视觉登录页。"}, nil
	}
	defer func() { autoVisionDispatch = func(ctx context.Context, s *Store, opts AssistDispatchOptions) (*AssistDispatchResult, error) {
		return s.AssistDispatch(ctx, opts)
	} }()

	assistOn := &AgentNode{Type: NodeExecutor, Label: "执行者", Model: "deepseek-chat", Assist: &AssistConfig{Enabled: true}}
	out := store.autoVisionInject(context.Background(), run, assistOn, task)

	if len(gotImages) != 2 {
		t.Fatalf("expected dispatch with 2 workspace images, got %v", gotImages)
	}
	found := false
	for _, p := range gotImages {
		if p == img1 || p == img2 {
			found = true
		}
	}
	if !found {
		t.Errorf("workspace image paths not dispatched: %v", gotImages)
	}
	if !strings.Contains(out, "效果图1.1.png →") || !strings.Contains(out, "效果图2.1.png →") {
		t.Errorf("path list must be injected; output:\n%s", out)
	}
	if !strings.Contains(out, "蓝色主视觉登录页") {
		t.Errorf("auto vision result must be injected; output:\n%s", out)
	}

	// 找不到的引用 → 未定位声明，不委派。
	autoVisionDispatch = func(_ context.Context, _ *Store, _ AssistDispatchOptions) (*AssistDispatchResult, error) {
		t.Fatal("dispatch must not run when no image can be located")
		return nil, nil
	}
	out = store.autoVisionInject(context.Background(), run, assistOn, "请读取 不存在.png 并描述")
	if !strings.Contains(out, "附件库与工作目录") || !strings.Contains(out, "无法自动识图") {
		t.Errorf("missing image must degrade honestly; output:\n%s", out)
	}
}