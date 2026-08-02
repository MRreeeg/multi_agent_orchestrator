# 编排控制台前端 UX 提升：管家首屏 + 对话式编排主链路

> date: 2026-08-02
> status: 已实现（v1，方向 A）
> 关联：[[MULTI_AGENT_README]]、`internal/serve/orchestrator_frontend/index.html`

## 问题

- 左上角为裸 "Reasonix"，工程感强，不像面向用户的"管家"产品；
- 欢迎区为静态文案，输入框 placeholder 为"输入需求... (Enter 记录)"，用户不知道"说一句话就能自动生成编排"；
- 底层已有"需求 → 自动分析 → 生成架构师/执行者/审查者 DAG → 自动切画布"能力（sendChat），但首屏完全没有呈现。

## 目标（用户确认的关键）

把「**说出任务 → 管家自动编排 → 一键执行**」变成打开就能用的主入口；视觉在现有深色工程主题（oklch 中性 + 单一 accent 橙 + DM Sans/JetBrains Mono）上做细节打磨，不做全量重构。

## 方案（方向 A：管家首屏）

| 模块 | 改动 |
|---|---|
| 品牌 | 左上角 → 「多智能体管家 · Reasonix Orchestrator」；副标题"多Agent 协作编排 · 架构 → 实现 → 审查" |
| 首屏 hero | 大标题"把任务交给我，剩下的我来编排" + 大输入框（textarea）+ 「✨ 生成编排」按钮 + 3 个示例 chips（端口范围解析 / 重构并补测试 / 代码审查），点击填入输入框 |
| 交互 | hero 回车 / 按钮发送；发送后自动分析 → 生成 DAG → 自动切画布 → toast + 「执行」按钮高亮脉冲引导 |
| 状态反馈 | 分析中按钮变"分析中…"禁用；按钮按压反馈（scale .97）；chip hover/active |
| taste 修正 | hero 按钮去掉 accent→violet 渐变（单一强调色，符合 design-taste 禁 AI 紫/多强调色），hover 用 tinted shadow |

## 已实现（提交列表）

- `index.html`：topbar 品牌、hero 欢迎区、hero/chips CSS、sendChat 读取 hero 输入 + 加载态 + 恢复、生成后「执行」高亮脉冲。
- 验证：`node --check` 通过、`go build ./internal/serve` 通过、`go test ./internal/serve` 通过。

## 追加：运行看板 + 审查决策 + 迭代进度（2026-08-02 晚）

- `PipelineIteration` SSE 事件（runID/iteration/maxIterations），Loop 每轮开始时发出；运行看板显示「第 x 轮 / 共 y 轮」。
- 审查者节点完成时解析 `loop-review-v1` JSON，运行看板下方显示**决策卡片**（✓ 通过 / 🔄 需修改 / ⛔ 阻塞 + 置信 + summary + requiredChanges + nextTask）。
- 运行看板（chat 顶部）：每节点状态点（等待/运行中脉冲/完成/失败）+ 单行输出摘要 + 停止/收起按钮，启动自动出现。
- 修复：run-dash HTML 容器中途未落盘导致 CSS/JS 引用空元素的破损中间态。
- 验证：event/orchestrator/serve 测试全绿；index 渲染断言含 run-dash/rd-nodes/rd-stop/rd-review。

## 追加：交互式执行分析 + 原生桌面应用（2026-08-02）

- **AI 执行进展分析**：运行看板「分析进展」按钮 → `POST /runs/{id}/analysis`（后端汇总 run 任务/迭代/各节点输出/审查决策 → reasonix deepseek-flash 生成 `{summary,progress,blocking,suggestions}`）→ 前端卡片展示卡点与建议。
- **原生桌面应用**：`cmd/reasonix-desktop`（WebView2，webview_go）——进程内嵌 HTTP 服务（随机回环端口）+ 原生窗口「多智能体管家 · Reasonix Orchestrator」（1440×900）。`go build ./cmd/reasonix-desktop` → 运行 exe 即应用窗口，关闭即退出。已验证窗口存活。

## 后续可选（方向 B：全量视觉重构，待确认）

- 字体升级（Geist/Satoshi）、emoji → SVG 图标系统、卡片 → 边界分组、stagger/tactile 动效、执行过程实时可视化面板。
- 若用户确认需要，单独立项执行。

## 验收

1. `go build ./cmd/reasonix` 启动后首屏显示"多智能体管家" + hero 输入框；
2. hero 输入任务 → 自动生成编排 → 自动切画布 → 「执行」高亮；
3. hero 示例 chips 可填入；回车/按钮均可发送；
4. 分析中按钮禁用 + "分析中…"，失败恢复。
