package orchestrator

import (
	"context"
	"os/exec"
	"strings"
	"sync"
	"time"

	claudeclient "reasonix/internal/executor/claude"
	dshclient "reasonix/internal/executor/dsh"
	opencodeclient "reasonix/internal/executor/opencode"
	"reasonix/internal/proc"
)

// ExecutorHealth describes the availability of one executor's CLI binary as
// discovered on this machine. Used by the orchestrator self-check panel.
type ExecutorHealth struct {
	Executor  string `json:"executor"`
	Available bool   `json:"available"`
	Bin       string `json:"bin,omitempty"`
	Version   string `json:"version,omitempty"`
	Error     string `json:"error,omitempty"`
}

// SelfCheck is the one-click self-check report: agent catalog, live runtime
// states, skill catalog with search roots, executor binary probes, the
// programmatically probed agents/models (no AI involved), and the locally
// authored DSH agent presets imported from $DSH_HOME/.agent-presets. It also
// carries whether the bundled dsh-agent-pack is present, so the frontend can
// offer an explicit "一键导入" action instead of installing on every check.
type SelfCheck struct {
	Agents      []NodeTypeInfo              `json:"agents"`
	Runtimes    []RuntimeState              `json:"runtimes"`
	Skills      []SkillInfo                 `json:"skills"`
	SkillRoots  []string                    `json:"skillRoots"`
	Health      []ExecutorHealth            `json:"health"`
	DshPresets  []dshclient.AgentPresetInfo `json:"dshPresets"`
	Probes      ProbeReport                 `json:"probes"`
	PackInstall dshclient.PackInstallReport `json:"packInstall"`
	CheckedAt   time.Time                   `json:"checkedAt"`
}

// CheckExecutors probes each supported executor binary in this build:
// reasonix, mimo, codex, claude, opencode, dsh. Probing is read-only;
// binaries are only queried for a --version line, they are never started as
// services. Probes run in parallel so the report returns in the time of the
// slowest single binary (bounded by the 3s version timeout), not the sum of
// all.
func CheckExecutors(ctx context.Context) []ExecutorHealth {
	probes := []struct {
		name   string
		locate func() (string, []string)
	}{
		{"reasonix", func() (string, []string) { return findReasonixBin(), nil }},
		{"mimo", func() (string, []string) { return lookupBin("mimo"), nil }},
		{"codex", func() (string, []string) { return lookupBin("codex"), nil }},
		{"claude", func() (string, []string) {
			if bin := claudeclient.DiscoverBin(); bin != "" {
				return bin, nil
			}
			return lookupBin("claude"), nil
		}},
		{"opencode", func() (string, []string) { return opencodeclient.DiscoverBin(), nil }},
		{"dsh", func() (string, []string) { bin, prefix, _ := dshclient.Command(); return bin, prefix }},
	}
	out := make([]ExecutorHealth, len(probes))
	var wg sync.WaitGroup
	for i, p := range probes {
		wg.Add(1)
		go func(i int, name string, locate func() (string, []string)) {
			defer wg.Done()
			out[i] = probeOne(ctx, name, locate)
		}(i, p.name, p.locate)
	}
	wg.Wait()
	return out
}

func probeOne(ctx context.Context, name string, locate func() (string, []string)) ExecutorHealth {
	bin, prefix := locate()
	bin = strings.TrimSpace(bin)
	if bin == "" {
		return ExecutorHealth{Executor: name, Available: false, Error: "binary not found on PATH"}
	}
	version, verr := probeVersion(ctx, bin, prefix)
	// A binary that fails to answer --version is not usable: report it as
	// unavailable so the self-check panel does not claim a broken binary is
	// ready to run.
	if verr != nil {
		return ExecutorHealth{Executor: name, Available: false, Bin: displayCommand(bin, prefix), Error: verr.Error()}
	}
	return ExecutorHealth{Executor: name, Available: true, Bin: displayCommand(bin, prefix), Version: version}
}

func displayCommand(bin string, prefix []string) string {
	if len(prefix) == 0 {
		return bin
	}
	return strings.TrimSpace(bin + " " + strings.Join(prefix, " "))
}

func lookupBin(name string) string {
	if path, err := exec.LookPath(name); err == nil {
		return path
	}
	return ""
}

// probeVersion asks one binary for its version line with a short timeout.
// 10s (not tighter) because freshly rebuilt Windows binaries can be held up
// by antivirus first-scan or cold start while five probes run in parallel;
// a 3s cap caused transient "unavailable" false positives in self-check.
func probeVersion(ctx context.Context, bin string, prefix []string) (string, error) {
	type result struct {
		line string
		err  error
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	args := append(append([]string{}, prefix...), "--version")
	cmd := exec.CommandContext(probeCtx, bin, args...)
	proc.HideWindow(cmd)
	ch := make(chan result, 1)
	go func() {
		out, err := cmd.Output()
		// 按行取第一条有意义的输出版本：跳过空行与 .cmd shim 的
		// "Active code page: NNNN" 横幅（npm shim 会先打印它）。
		line := ""
		for _, l := range strings.Split(string(out), "\n") {
			l = strings.TrimSpace(l)
			if l == "" || strings.HasPrefix(l, "Active code page:") {
				continue
			}
			line = l
			break
		}
		if len(line) > 80 {
			line = line[:80]
		}
		ch <- result{line: line, err: err}
	}()
	select {
	case r := <-ch:
		return r.line, r.err
	case <-probeCtx.Done():
		return "", probeCtx.Err()
	}
}

// SelfCheckSnapshot assembles a one-click self-check report from already
// computed state (node types, live runtimes, skill catalog with roots) plus a
// fresh executor binary probe, the programmatically probed agents/models, and
// the locally authored DSH agent presets. It only probes whether the bundled
// dsh-agent-pack exists on this machine; installation is a separate explicit
// action (the "一键导入" button → /dsh-presets/install), so a self-check never
// mutates $DSH_HOME behind the user's back.
func SelfCheckSnapshot(ctx context.Context) SelfCheck {
	return SelfCheck{
		Agents:      NodeTypeCatalog(),
		Runtimes:    AllRuntimes(),
		Skills:      ListAvailableSkills(),
		SkillRoots:  skillSearchRoots(),
		Health:      CheckExecutors(ctx),
		DshPresets:  dshclient.ListAgentPresets(),
		Probes:      ProbeModels(ctx),
		PackInstall: dshclient.ProbeAgentPack(),
		CheckedAt:   time.Now(),
	}
}

// InstallAgentPack runs the bundled dsh-agent-pack installer explicitly
// (button-triggered). It is idempotent and safe to call repeatedly.
func InstallAgentPack() dshclient.PackInstallReport {
	return dshclient.EnsureAgentPackInstalled()
}

// AllRuntimes merges the live runtime state of every executor manager.
func AllRuntimes() []RuntimeState {
	merged := make([]RuntimeState, 0, 16)
	for _, list := range []func() []*RuntimeState{
		reasonixRuntimeMgr.List,
		mimoRuntimeMgr.List,
		codexRuntimeMgr.List,
		claudeRuntimeMgr.List,
		opencodeRuntimeMgr.List,
	} {
		for _, rt := range list() {
			if rt != nil {
				merged = append(merged, *rt)
			}
		}
	}
	return merged
}

// NodeTypeCatalog returns the static agent/model/skill catalog shown in the
// config panel. It is the source of truth the frontend also uses when it
// builds pipeline nodes.
func NodeTypeCatalog() []NodeTypeInfo {
	allExecutors := []ExecutorType{ExecutorReasonix, ExecutorMimo, ExecutorCodex, ExecutorClaude, ExecutorOpencode, ExecutorDsh}
	return []NodeTypeInfo{
		{
			Type:   NodeArchitect,
			Label:  "架构师",
			Models: []string{"deepseek-pro", "deepseek-flash", "deepseek", "deepseek-v4-flash", "mimo-v2.5-pro", "mimo-v2.5", "xiaomi/mimo-v2.5", "ccs", "o3", "codex-default"},
			ModelsByExecutor: map[ExecutorType][]string{
				ExecutorReasonix: {"deepseek-pro", "deepseek-flash", "deepseek"},
				ExecutorMimo:     {"mimo-v2.5-pro", "mimo-v2.5", "xiaomi/mimo-v2.5"},
				ExecutorCodex:    {"ccs", "o3", "codex-default", "deepseek-v4-flash"},
				ExecutorClaude:   {"ccs", "opus", "sonnet", "haiku", "claude-fable-5", "deepseek-v4-flash"},
				ExecutorOpencode: {"opencode/deepseek-v4-flash-free", "deepseek/deepseek-v4-flash", "deepseek/deepseek-v4-pro"},
				ExecutorDsh:      {"deepseek-v4-flash", "deepseek-v4-pro"},
			},
			Skills:    skillCatalogNames(),
			Executors: allExecutors,
		},
		{
			Type:   NodeReviewer,
			Label:  "审查者",
			Models: []string{"deepseek-flash", "deepseek", "xiaomi/mimo-v2.5"},
			ModelsByExecutor: map[ExecutorType][]string{
				ExecutorReasonix: {"deepseek-flash", "deepseek"},
				ExecutorMimo:     {"xiaomi/mimo-v2.5"},
				ExecutorCodex:    {"ccs", "o3", "codex-default", "deepseek-v4-flash"},
				ExecutorClaude:   {"ccs", "opus", "sonnet", "haiku", "claude-fable-5", "deepseek-v4-flash"},
				ExecutorOpencode: {"opencode/deepseek-v4-flash-free", "deepseek/deepseek-v4-flash", "deepseek/deepseek-v4-pro"},
				ExecutorDsh:      {"deepseek-v4-flash", "deepseek-v4-pro"},
			},
			Skills:    skillCatalogNames(),
			Executors: allExecutors,
		},
		{
			Type:   NodeExecutor,
			Label:  "执行者",
			Models: []string{"deepseek-flash", "deepseek-pro", "deepseek-v4-flash", "xiaomi/mimo-v2.5", "xiaomi/mimo-v2.5-pro", "o3", "codex-default"},
			ModelsByExecutor: map[ExecutorType][]string{
				ExecutorReasonix: {"deepseek-flash", "deepseek-pro"},
				ExecutorMimo:     {"xiaomi/mimo-v2.5", "xiaomi/mimo-v2.5-pro"},
				ExecutorCodex:    {"ccs", "o3", "codex-default", "deepseek-v4-flash"},
				ExecutorClaude:   {"ccs", "opus", "sonnet", "haiku", "claude-fable-5", "deepseek-v4-flash"},
				ExecutorOpencode: {"opencode/deepseek-v4-flash-free", "deepseek/deepseek-v4-flash", "deepseek/deepseek-v4-pro"},
				ExecutorDsh:      {"deepseek-v4-flash", "deepseek-v4-pro"},
			},
			Skills:    skillCatalogNames(),
			Executors: allExecutors,
		},
	}
}

func skillCatalogNames() []string {
	infos := ListAvailableSkills()
	out := make([]string, 0, len(infos))
	for _, info := range infos {
		out = append(out, info.Name)
	}
	if len(out) == 0 {
		out = []string{"brainstorming", "executing-plans", "review-agent", "harness-eval", "loop"}
	}
	return out
}
