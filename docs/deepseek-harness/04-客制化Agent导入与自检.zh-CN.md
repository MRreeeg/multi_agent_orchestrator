# 客制化 Agent 导入与自检（Reasonix × DSH 预设集成）

> 版本：2026-08-14 ｜ 适用：想让 Reasonix 编排控制台的 dsh 节点直接用上本地客制化的 DSH agent，并在自检时自动导入的人
> 关联：[[00-创造模式配置交接文档.zh-CN.md]]、[[03-自定义Agent打包与跨电脑复用.zh-CN.md]]、[[02-DSH执行器接入与配置.zh-CN.md]]
> 代码：`internal/executor/dsh/presets.go`、`internal/orchestrator/health.go`、`internal/serve/orchestrator.go`、`internal/serve/orchestrator_frontend/index.html`

---

## 1. 一句话方案

**本地客制化 agent = `$DSH_HOME/.agent-presets/<id>/` 目录**（`agent.cordis.yml` + `preset.yml`，可选 `headless.patch.yml`）。Reasonix 在**自检时自动扫描并导入**这些目录：自检面板列出它们，dsh 节点配置面板出现「客制化 Agent」下拉；节点选中后，执行器把该预设的 `headless.patch.yml` 作为 `--patch` 覆盖层传给 `dsh --profile headless`。

## 2. 两个机制，各司其职

| 场景 | 机制 | 文件 |
|---|---|---|
| DSH Web 交互会话 | DSH 定义式 preset（persona 行 + 工具行裁剪，可含 subagent/workflow 等能力） | `agent.cordis.yml` |
| Reasonix dsh 节点（headless 一次性） | 同一职责的扁平化补丁：persona 覆盖 `system-prompt` 行 + 禁用预设未提供的工具行 | `headless.patch.yml` |

为什么节点不用 preset 本体：`dsh --profile headless` 的 one-shot runner 不挂载 preset 花名册（无 Web 的 agent-presets 行），实测预设组合无法生效；而 `--patch` 的 persona 覆盖是交接文档 §4 已验证的机制。`headless.patch.yml` 把同一个职责"翻译"成 headless 能吃的形式，两份文件一起分发、一起维护。

## 3. 自检自动导入（/selfcheck）

`GET /orchestrator/api/selfcheck` 新增 `dshPresets` 字段：`DiscoverDshPresets()` 扫描 `$DSH_HOME/.agent-presets/*/`，读取 `preset.yml` 的 `name`/`description`，并检查 `headless.patch.yml` 是否存在（`hasPatch`）。前端自检面板新增「客制化 DSH Agent（本地自动导入）」区，逐条显示名称、id、描述、目录与"可用于节点 / 无 headless 补丁"徽章。

节点配置面板的专用端点 `GET /orchestrator/api/dsh-presets` 返回同一份数据（初始化时加载，不必等自检）。

## 4. 节点配置与执行链路

```text
前端节点配置面板（executor=dsh 时显示「客制化 Agent」下拉）
  → node.dshPreset = <id>（随 Pipeline 持久化）
  → executeNode → ExecSpec.DshPreset
  → DshPipelineExecutor → ExecOptions.AgentPreset
  → ResolvePresetPatch(id, DshHome)  ← 在 $DSH_HOME/.agent-presets/<id>/headless.patch.yml 查找
  → args += --patch <patch>（与模型覆盖补丁并列）
  → dsh --profile headless --patch … "<task>"
```

失败即大声：预设不存在或无 `headless.patch.yml` 时节点直接失败（错误信息给出目录与修复提示），绝不静默退回内置 persona——否则用户以为客制化生效了、实际没有。

## 5. 前端「Prompt 还是客制化 Agent」标注

- **节点卡片**（Canvas）新增「Agent 来源」行：
  - dsh 节点选中预设 → `客制化 Agent：前端分析师 · 管家（frontend-analyst）`（强调色加粗）
  - dsh 节点未选 → `Prompt 式：内置 persona`
  - 其他执行器 → `Prompt 式：Skill 注入 <skill>` 或 `Prompt 式：角色描述`
- **节点详情弹窗**同样显示「Agent 来源」。
- **配置面板**：客制化 Agent 下拉第一项是「默认（内置 persona，Prompt 式）」，无 headless 补丁的预设置灰。

## 6. 换电脑复用

`dsh-agent-pack/install.ps1 -Mode user` 会把 `presets/*` 装进目标电脑的 `$DSH_HOME/.agent-presets/`（`-SkipPresets` 可跳过）；`-Mode temp -Preset <id>` 用某个预设的 headless 补丁跑一次做验证。预设目录与 skills 一样零依赖，复制即用。

## 7. 维护约定

- 改 persona/职责时，`agent.cordis.yml` 与 `headless.patch.yml` **两份文件同步改**（后者是前者的 headless 翻译）。
- `headless.patch.yml` 里 `disabled: true` 的每一行都对应"预设组合里没有该工具行"：加工具行到组合时，记得从补丁里移除对应禁用。
- 新增角色预设时：`presets/<id>/` 三件套 + 自检自动出现，无需改后端代码。

## 8. 验收清单

- [ ] `$DSH_HOME/.agent-presets/` 下存在 4 个预设目录，每个都有 `agent.cordis.yml` + `preset.yml` + `headless.patch.yml`
- [ ] 控制台 `/selfcheck` 的「客制化 DSH Agent」区列出 4 个预设，均可用于节点
- [ ] dsh 节点配置面板「客制化 Agent」下拉可选 4 个预设，卡片显示「客制化 Agent：xxx」
- [ ] 未选预设的 dsh 节点卡片显示「Prompt 式：内置 persona」；reasonix 节点显示「Prompt 式：Skill 注入 …」
- [ ] 建一个 dsh 节点选中 `reviewer` 并执行：输出符合审查者 persona，且节点无 shell 工具（stderr/输出中无命令执行）
- [ ] 把节点 `dshPreset` 改成不存在的 id 执行：节点失败且错误信息指向预设目录
- [ ] `go test ./internal/executor/dsh ./internal/orchestrator ./internal/serve`、`go vet`、`go build ./...` 全绿
