# Orchestrator 控制台修复设计：子进程黑框与 Runtime Console 刷新

> 日期：2026-08-05
> 范围：`multi_agent_orchestrator`（Reasonix / Multi-Agent Orchestrator）
> 状态：已批准（用户确认方案后进入实现）

## 背景与问题

桌面版（orchestrator-app.exe）与浏览器版在运行编排流水线时暴露两个体验问题：

1. **黑框**：每个节点执行时，编排器启动的子进程（codex / mimo / claude / reasonix 自调用）
   会弹出黑色控制台窗口，一个 pipeline 出现 N 个黑框，且窗口内无有用信息。
   原因是子进程创建时没有设置 Windows `CREATE_NO_WINDOW` 标志。
2. **Runtime Console 刷新打断阅读**：控制台每 1.2 秒轮询一次，且每条事件到达都会
   整块重建事件列表（innerHTML），导致：
   - 用户展开的 `<details>` 日志条目被折叠回关闭状态；
   - 用户向上翻阅旧日志时，滚动条被重置（未在底部时没有恢复滚动位置）。

## 目标与非目标

目标：

- 流水线运行全程不弹任何子进程控制台窗口；
- Runtime Console 事件列表增量更新：展开状态保留、滚动位置只在“位于底部”时才自动跟随。

非目标：

- 不处理“循环卡死”问题（用户明确暂缓）；
- 不改变 API、数据格式、执行器行为或运行日志内容；
- 不做无关重构。

## 方案一：隐藏子进程控制台窗口

机制：复用现有 `internal/proc.HideWindow(cmd)`（Windows 下设置
`SysProcAttr.CreationFlags |= CREATE_NO_WINDOW`），在所有编排子进程启动点调用。

覆盖点（文件与位置）：

| 位置 | 子进程 | 说明 |
|---|---|---|
| `internal/orchestrator/pipeline.go` `newRetainedRuntimeCommand` | codex app-server / mimo acp / claude serve / reasonix serve | 一处覆盖全部保留运行时（codex_runtime / mimo_runtime / claude_runtime / pipeline.go:575 均经此创建） |
| `internal/orchestrator/pipeline.go` `executeReasonixRun` | reasonix run | 节点一次性执行 |
| `internal/orchestrator/pipeline.go` `runMimoCommand` | mimo run | 一次性执行 |
| `internal/orchestrator/pipeline.go` `understandTask` | reasonix run（flash） | 任务理解辅助调用 |
| `internal/orchestrator/pipeline.go` 内 `taskkill` 调用 | taskkill | 进程树清理，顺带隐藏 |
| `internal/executor/codex/codex.go` `executeWithInput` | codex exec | 一次性执行 |
| `internal/executor/claude/claude.go` | claude -p | 一次性执行 |

说明：

- `proc` 包在 Windows 与非 Windows 均有 `HideWindow` 桩实现（`hide_other.go` 为空操作），
  因此改动对跨平台构建安全。
- 隐藏窗口不影响子进程输出捕获：stdout/stderr 仍走管道进入 Runtime Console 与运行记录。

## 方案二：Runtime Console 增量渲染与状态保留

位置：`internal/serve/orchestrator_frontend/index.html`
（`renderRuntimeConsole` / `refreshRuntimeConsole`，桌面版经 go:embed 内嵌）。

行为变更：

1. **增量追加**：每个 runtime 缓存“已渲染事件列表”。刷新时若新事件列表是旧列表的
   超集（前缀相同），仅把新增事件追加为 DOM 节点；已有节点不重建，
   `<details open>` 展开状态自然保留。
2. **滚动保留**：追加前记录 `scrollTop` 与“是否接近底部”（`scrollHeight - scrollTop -
   clientHeight < 阈值`）。接近底部时追加后滚到底；否则完全不动滚动位置。
3. **退化重建**：当事件列表发生非追加变化（如数量减少、后端合并回填导致前缀不一致）时，
   整块重建，但重建前保存每个 `<details>` 的展开状态（按索引）与 `scrollTop`，重建后恢复。
4. **元信息与按钮**：`.rc-meta`、发送/中断按钮状态仍每次更新（与日志区分离，不受影响）。

## 验证

- `go build ./cmd/reasonix ./cmd/orchestrator-app`、`go vet` 相关包；
- 现有 `proc.HideWindow` 相关单测通过；
- 前端 JS 语法校验（node --check）；
- 实机验证：桌面版跑一个 3 节点流水线，确认：
  - 全程无黑框；
  - 打开 Runtime Console，展开若干条目、滚动到中部，等新事件到达后展开状态与滚动位置不变。

## 风险与回滚

- 风险低：均为局部行为修改，不涉及数据与 API。
- 回滚：恢复 `pipeline.go` / `codex.go` / `claude.go` / `index.html` 的改动即可；
  桌面版需重新构建（前端为内嵌资源）。
