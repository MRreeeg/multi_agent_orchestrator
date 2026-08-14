package orchestrator

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseModelLines(t *testing.T) {
	got := parseModelLines("mimo/mimo-auto\nxiaomi/mimo-v2.5\n\nActive code page: 65001\nxiaomi/mimo-v2.5\n")
	if len(got) != 2 {
		t.Fatalf("models = %v, want 2 unique", got)
	}
	if got[0] != "mimo/mimo-auto" || got[1] != "xiaomi/mimo-v2.5" {
		t.Errorf("models = %v, want sorted unique list", got)
	}
}

func TestMergeProbedModelsOverridesPerExecutor(t *testing.T) {
	catalog := NodeTypeCatalog()
	before := catalog[0].ModelsByExecutor[ExecutorMimo]
	report := ProbeReport{Executors: []ProbedExecutor{
		{Executor: "mimo", Available: true, Models: []string{"only/mimo-probe"}},
	}}
	merged := mergeProbedModels(catalog, report)
	if got := merged[0].ModelsByExecutor[ExecutorMimo]; len(got) != 1 || got[0] != "only/mimo-probe" {
		t.Fatalf("mimo models after merge = %v, want probe override", got)
	}
	if len(before) == 0 {
		t.Fatal("baseline mimo models empty, test meaningless")
	}
	// 其他执行器保留静态兜底
	if got := merged[0].ModelsByExecutor[ExecutorReasonix]; len(got) == 0 {
		t.Fatal("reasonix models lost after merge")
	}
	// 空报告不覆盖
	noProbe := mergeProbedModels(NodeTypeCatalog(), ProbeReport{})
	if got := noProbe[0].ModelsByExecutor[ExecutorMimo]; len(got) == 0 {
		t.Fatal("empty probe report must keep static models")
	}
}

func TestProbeCodexReadsConfigAndOverlays(t *testing.T) {
	codexHome := t.TempDir()
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(codexHome, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("config.toml", "model = \"deepseek-v4-flash\"\nmodel_provider = \"deepseek\"\n\n[model_providers.custom]\nmodels = [\"gpt-5.6-luna\", \"gpt-5\"]\n")
	write("deepseek.config.toml", "model_provider = \"deepseek\"\n[model_providers.deepseek]\nmodels = [\"deepseek-v4-flash\", \"deepseek-v4-pro\"]\n")

	t.Setenv("CODEX_HOME", codexHome)
	p := probeCodex(t.Context())
	if !p.Available {
		// 本机没装 codex 时跳过模型断言，但解析逻辑仍应无 panic
		return
	}
	joined := strings.Join(p.Models, ",")
	for _, want := range []string{"deepseek-v4-flash", "gpt-5.6-luna", "deepseek-v4-pro"} {
		if !strings.Contains(joined, want) {
			t.Errorf("codex models missing %q: %v", want, p.Models)
		}
	}
}
