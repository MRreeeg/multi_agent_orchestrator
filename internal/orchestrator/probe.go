package orchestrator

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"gopkg.in/yaml.v3"

	"reasonix/internal/config"
	claudeclient "reasonix/internal/executor/claude"
	dshclient "reasonix/internal/executor/dsh"
	opencodeclient "reasonix/internal/executor/opencode"
	"reasonix/internal/proc"
)

// ProbedExecutor 是一台机器上程序化探测到的执行器能力：二进制是否存在、
// 可用模型、可用 agent 人设。探测是纯程序化的（跑 CLI 的 models 命令 / 读
// 配置文件），绝不调用 AI——新电脑上没有任何 agent 时，用 AI 检查 AI 只会
// 越查越乱。
type ProbedExecutor struct {
	Executor  string   `json:"executor"`
	Available bool     `json:"available"`
	Models    []string `json:"models"`
	Agents    []string `json:"agents,omitempty"`
	Error     string   `json:"error,omitempty"`
}

// ProbeReport 是一次完整探测的结果快照。
type ProbeReport struct {
	Executors []ProbedExecutor `json:"executors"`
	ProbedAt  time.Time        `json:"probedAt"`
	FromCache bool             `json:"fromCache"`
}

// probeCache 缓存最近一次探测：/nodes/types、/analysis/options、/selfcheck
// 都会触发"首次初始化自检"（第一次调用即探测），之后 60s 内复用。
var probeCache struct {
	mu      sync.Mutex
	report  *ProbeReport
	fetched time.Time
}

const probeCacheTTL = 60 * time.Second

// ProbeModels 探测本机各执行器可用 agent 与模型。每次探测并发执行、单项
// 5s 超时；结果缓存 60s。失败项降级为空列表并附错误信息，绝不 500。
func ProbeModels(ctx context.Context) ProbeReport {
	probeCache.mu.Lock()
	defer probeCache.mu.Unlock()
	if probeCache.report != nil && time.Since(probeCache.fetched) < probeCacheTTL {
		return ProbeReport{Executors: probeCache.report.Executors, ProbedAt: probeCache.report.ProbedAt, FromCache: true}
	}
	report := ProbeReport{ProbedAt: time.Now()}
	results := make([]ProbedExecutor, len(executorProbers))
	var wg sync.WaitGroup
	for i, p := range executorProbers {
		wg.Add(1)
		go func(i int, p func(ctx context.Context) ProbedExecutor) {
			defer wg.Done()
			probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			results[i] = p(probeCtx)
		}(i, p)
	}
	wg.Wait()
	report.Executors = results
	probeCache.report = &report
	probeCache.fetched = time.Now()
	return report
}

// executorProbers 是各执行器的程序化探测器，与 NodeTypeCatalog 的执行器顺序一致。
var executorProbers = []func(ctx context.Context) ProbedExecutor{
	probeReasonix,
	probeMimo,
	probeCodex,
	probeClaude,
	probeOpencode,
	probeDsh,
}

func probeExecutable(name string, fallback func() string) string {
	if bin := fallback(); bin != "" {
		return bin
	}
	if path, err := exec.LookPath(name); err == nil {
		return path
	}
	return ""
}

// probeReasonix 从 reasonix 自身配置读取已配置 provider 的模型（不跑 CLI）。
func probeReasonix(ctx context.Context) ProbedExecutor {
	p := ProbedExecutor{Executor: "reasonix"}
	p.Available = probeExecutable("reasonix", findReasonixBin) != ""
	if !p.Available {
		return p
	}
	ws := detectWorkspace()
	cfg, err := config.LoadForRoot(ws)
	if err != nil {
		p.Error = err.Error()
		return p
	}
	seen := map[string]bool{}
	var models []string
	add := func(m string) {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			return
		}
		seen[m] = true
		models = append(models, m)
	}
	for i := range cfg.Providers {
		entry := &cfg.Providers[i]
		if !entry.Configured() {
			continue
		}
		add(entry.DefaultModel())
		for _, m := range entry.ChatModelList() {
			add(m)
		}
	}
	if len(models) == 0 {
		// 兜底：reasonix 内置别名。
		for _, m := range []string{"deepseek-pro", "deepseek-flash", "deepseek", "mimo-pro", "mimo-flash"} {
			add(m)
		}
	}
	sort.Strings(models)
	p.Models = models
	return p
}

// probeMimo 运行 `mimo models` 枚举可用模型。
func probeMimo(ctx context.Context) ProbedExecutor {
	p := ProbedExecutor{Executor: "mimo"}
	bin := probeExecutable("mimo", func() string { return "" })
	if bin == "" {
		return p
	}
	p.Available = true
	out, err := runModelCommand(ctx, bin, "models")
	if err != nil {
		p.Error = err.Error()
		return p
	}
	p.Models = parseModelLines(out)
	return p
}

// probeCodex 解析 ~/.codex/config.toml 及其覆盖层（deepseek/ccs），收集
// 顶层 model 与各 model_providers 的模型列表。
func probeCodex(ctx context.Context) ProbedExecutor {
	p := ProbedExecutor{Executor: "codex"}
	bin := probeExecutable("codex", func() string { return "" })
	if bin == "" {
		return p
	}
	p.Available = true
	home, _ := os.UserHomeDir()
	codexDir := filepath.Join(home, ".codex")
	if env := strings.TrimSpace(os.Getenv("CODEX_HOME")); env != "" {
		codexDir = env
	}
	seen := map[string]bool{}
	var models []string
	add := func(m string) {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			return
		}
		seen[m] = true
		models = append(models, m)
	}
	// 主配置 + 覆盖层：cc-switch 会重写主配置，覆盖层自包含。
	for _, name := range []string{"config.toml", "deepseek.config.toml", "ccs.config.toml"} {
		path := filepath.Join(codexDir, name)
		var c struct {
			Model          string `toml:"model"`
			ModelProvider  string `toml:"model_provider"`
			ModelProviders map[string]struct {
				Models []string `toml:"models"`
			} `toml:"model_providers"`
		}
		if _, err := toml.DecodeFile(path, &c); err != nil {
			continue
		}
		add(c.Model)
		for _, pv := range c.ModelProviders {
			for _, m := range pv.Models {
				add(m)
			}
		}
	}
	if len(models) == 0 {
		for _, m := range []string{"ccs", "o3", "codex-default", "deepseek-v4-flash", "gpt-5.6-luna"} {
			add(m)
		}
	}
	sort.Strings(models)
	p.Models = models
	return p
}

// probeClaude 解析 ~/.claude/settings.json 与 ~/.claude-deepseek/settings.json
// 的 env 模型映射（ANTHROPIC_MODEL / ANTHROPIC_DEFAULT_*）。
func probeClaude(ctx context.Context) ProbedExecutor {
	p := ProbedExecutor{Executor: "claude"}
	bin := probeExecutable("claude", claudeclient.DiscoverBin)
	if bin == "" {
		return p
	}
	p.Available = true
	home, _ := os.UserHomeDir()
	seen := map[string]bool{}
	var models []string
	add := func(m string) {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			return
		}
		seen[m] = true
		models = append(models, m)
	}
	for _, dir := range []string{filepath.Join(home, ".claude"), filepath.Join(home, ".claude-deepseek")} {
		path := filepath.Join(dir, "settings.json")
		var s struct {
			Env map[string]string `json:"env"`
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		// settings.json 是宽松 JSON；用标准库解析，失败跳过。
		if err := json.Unmarshal(raw, &s); err != nil {
			continue
		}
		for _, key := range []string{"ANTHROPIC_MODEL", "ANTHROPIC_DEFAULT_HAIKU_MODEL", "ANTHROPIC_DEFAULT_OPUS_MODEL", "ANTHROPIC_DEFAULT_SONNET_MODEL", "ANTHROPIC_SMALL_FAST_MODEL"} {
			add(s.Env[key])
		}
	}
	if len(models) == 0 {
		for _, m := range []string{"ccs", "opus", "sonnet", "haiku", "claude-fable-5", "deepseek-v4-flash"} {
			add(m)
		}
	}
	sort.Strings(models)
	p.Models = models
	return p
}

// probeOpencode 运行 `opencode models` 枚举可用模型。
func probeOpencode(ctx context.Context) ProbedExecutor {
	p := ProbedExecutor{Executor: "opencode"}
	bin := probeExecutable("opencode", opencodeclient.DiscoverBin)
	if bin == "" {
		return p
	}
	p.Available = true
	out, err := runModelCommand(ctx, bin, "models")
	if err != nil {
		p.Error = err.Error()
		return p
	}
	p.Models = parseModelLines(out)
	return p
}

// probeDsh 读取 $DSH_HOME/settings.yaml 的 agent-default-model，并固定官方
// 直连模型；客制化 agent 预设作为 dsh 的 agent 人设一并报告。
func probeDsh(ctx context.Context) ProbedExecutor {
	p := ProbedExecutor{Executor: "dsh"}
	p.Available = dshclient.DiscoverBin() != ""
	if !p.Available {
		return p
	}
	seen := map[string]bool{}
	var models []string
	add := func(m string) {
		m = strings.TrimSpace(m)
		if m == "" || seen[m] {
			return
		}
		seen[m] = true
		models = append(models, m)
	}
	add("deepseek-v4-flash")
	add("deepseek-v4-pro")
	// settings.yaml 保存的默认模型优先展示。
	if home := dshclient.DshHome(); home != "" {
		if s, err := os.ReadFile(filepath.Join(home, "settings.yaml")); err == nil {
			var doc struct {
				DefaultModel struct {
					Model string `yaml:"model"`
				} `yaml:"agent-default-model"`
			}
			if yaml.Unmarshal(s, &doc) == nil {
				add(doc.DefaultModel.Model)
			}
		}
	}
	sort.Strings(models)
	p.Models = models
	// dsh 的 agent 人设 = 本地客制化 agent 预设。
	for _, preset := range dshclient.ListAgentPresets() {
		p.Agents = append(p.Agents, preset.ID)
	}
	sort.Strings(p.Agents)
	return p
}

// NodeTypeCatalogWithProbes 返回静态节点目录，但 ModelsByExecutor 按本机
// 探测结果覆盖（探测失败/为空时保留静态兜底）。/nodes/types 与
// /analysis/options 用这个结果，让前端下拉反映每台电脑的真实能力。
func NodeTypeCatalogWithProbes(ctx context.Context) []NodeTypeInfo {
	return mergeProbedModels(NodeTypeCatalog(), ProbeModels(ctx))
}

// mergeProbedModels 把探测结果合并进静态目录：探测到模型时按执行器覆盖。
func mergeProbedModels(catalog []NodeTypeInfo, report ProbeReport) []NodeTypeInfo {
	if len(report.Executors) == 0 {
		return catalog
	}
	byExecutor := map[string]ProbedExecutor{}
	for _, pe := range report.Executors {
		byExecutor[pe.Executor] = pe
	}
	for i := range catalog {
		merged := map[ExecutorType][]string{}
		for k, v := range catalog[i].ModelsByExecutor {
			merged[k] = append([]string{}, v...)
		}
		for _, ex := range catalog[i].Executors {
			if pe, ok := byExecutor[string(ex)]; ok && len(pe.Models) > 0 {
				merged[ex] = append([]string{}, pe.Models...)
			}
		}
		catalog[i].ModelsByExecutor = merged
	}
	return catalog
}

// runModelCommand 运行一个只输出列表的命令并捕获 stdout。
func runModelCommand(ctx context.Context, bin, sub string) (string, error) {
	cmd := exec.CommandContext(ctx, bin, sub)
	proc.HideWindow(cmd)
	out, err := cmd.Output()
	if err != nil {
		return "", err
	}
	return string(out), nil
}

// parseModelLines 把 CLI 输出的模型行解析成列表（去空行、去装饰符）。
func parseModelLines(out string) []string {
	var models []string
	seen := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "Active code page:") {
			continue
		}
		if seen[line] {
			continue
		}
		seen[line] = true
		models = append(models, line)
	}
	sort.Strings(models)
	return models
}
