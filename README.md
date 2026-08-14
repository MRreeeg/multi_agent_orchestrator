# 🧭 多智能体管家 · Multi-Agent Orchestrator

> 把任务交给我，剩下的我来编排。
> 一个把「**说一句话 → 自动编排 → 多 Agent 协作执行 → 审查决策 → 迭代收敛**」串成一条完整流水线的编排控制台。

---

## ✨ 它是什么

这是一个 **多 Agent 协作编排系统**：你只需要描述任务，它会自动拆解、自动组建一支"虚拟团队"（架构师 → 执行者 → 审查者），让它们像真人团队一样分工协作、互相审查、迭代到通过为止。

不再是"你手动拖节点、逐个配置、盯着一堆日志"的工程工具，而是一个 **管家式的智能工作台**：

- 🗣️ **一句话编排**：输入任务 → 自动分析 → 自动生成「架构师 → 执行者 → 审查者」流程 → 一键执行
- 👀 **运行看板**：执行时实时看到每个 Agent 在做什么、输出什么、进行到第几轮，随时可停止/干预
- ✅ **审查决策卡**：审查者的 pass / revise / blocked 结论 + 置信度 + 修改清单 + 下一步，一眼看清
- 🧠 **AI 进展分析**：随时点「分析进展」，管家告诉你"现在到哪了、卡在哪、下一步该干嘛"
- 💬 **人工介入**：随时向保留的 Agent 会话发消息、打断、追问，不影响 Loop 历史
- 📦 **原生桌面应用**：WebView2 独立窗口（不再是浏览器标签页），带应用图标、记住窗口位置
- 🔌 **多执行器 + 多模型路由**：codex / mimo / claude / opencode / dsh（DeepSeek Harness）混用；deepseek 官方直连 / ccs（中转站）按节点自由路由

---

## 🚀 快速开始

### 方式一：桌面应用（推荐）
```powershell
scripts\start-desktop.bat     # 双击：自动构建并打开「多智能体管家」窗口
```
打开后，在首页输入框描述你的任务，点「✨ 生成编排」，检查自动生成的三个节点，点「执行」。

### 方式二：浏览器控制台
```powershell
.\start.bat                   # 或 .\scripts\start-orchestrator.ps1
# 自动打开 http://127.0.0.1:8788/orchestrator
```

### 依赖
| 项 | 说明 |
|---|---|
| Go 工具链 | 构建用 |
| gcc（CGO）+ WebView2 运行时 | 桌面应用需要（Win11 / Edge 自带 WebView2） |
| codex / mimo / claude / opencode / dsh CLI | 执行器；按节点配置 provider/模型 |
| DeepSeek / 中转站 API key | 模型路由（见下方模型配置） |

---

## 🧩 一个典型 Loop 长什么样

```
你："为项目 X 实现端口解析功能，含 14 场景测试，仅标准库"

   ┌─────────┐   方案     ┌─────────┐   代码     ┌─────────┐
   │  架构师  │ ───────▶ │  执行者  │ ───────▶ │  审查者  │
   │ 设计&规划│          │ 实现&测试│          │ 核对计划 │
   └─────────┘           └─────────┘           └─────────┘
        ▲                                        │
        └──────────── revise（返回修改）──────────┘
        （pass → 结束；fixed → 跑满 N 轮）
```

- **架构师**：只读分析，产出设计文档（自动落盘到 `工作区/.reasonix/plans/`），列出每轮计划；
- **执行者**：按方案写代码、跑测试，只做当前一轮；
- **审查者**：**对照架构师设计文档 + 原始任务**判断覆盖度（不是只看当轮语法），输出 `pass / revise / blocked` + 修改清单；
- 循环由 Orchestrator 控制，审查者决定要不要进入下一轮（`review_decides`）或固定轮数（`fixed`）。

---

## 🎛️ 执行器与模型路由

| 执行器 | run（一次性） | serve（保留 Runtime + Console） |
|---|---|---|
| **codex** | `codex exec` | `codex app-server`（WebSocket）+ Thread 复用 + 中断/恢复 |
| **mimo** | `mimo run` | `mimo acp`（ACP JSON-RPC over stdio）+ 会话复用 |
| **claude** | `claude -p --output-format json` | stream-json stdio 保留进程 + Console + 人工消息 |

**模型路由（每节点独立）：**
- `providerRoute=ccswitch` / `model=ccs` → 省略 `--model`，走 cc-switch / 中转站（如 rightapi 的 gpt）
- `providerRoute=` 空 + `model=deepseek-v4-flash` 等 → 原样透传，DeepSeek 官方直连（codex/claude 均支持）
- 执行者 codex + deepseek 官方直连、审查者 codex + ccs 中转，**同一个 Loop 里可共存互不干扰**（cc-switch 配置器机制已适配，见手册）

---

## 🖥️ 前端体验（管家感）

- 浅色主题：暖白 + 青绿强调色，现代字体栈（Segoe UI Variable / Space Grotesk / Cascadia Code）
- 首屏 hero：「把任务交给我，剩下的我来编排」+ 大输入框 + 示例 chips
- 运行看板：节点状态点 + 实时输出摘要 + 迭代进度 + 停止/收起
- Runtime Console：流式事件（问答展开、系统日志折叠）、两步确认发送、Enter 发送
- 节点卡片直接显示输出摘要；启动失败保留错误供诊断

---

## 📚 文档（随仓库分发，其他电脑拿到即可照做）

| 文档 | 内容 |
|---|---|
| [`MULTI_AGENT_README.md`](MULTI_AGENT_README.md) | 功能总览、运行方式、Codex/Mimo/Claude 与 CCSwitch 说明 |
| [`NEW_AGENT_QUICKSTART.zh-CN.md`](NEW_AGENT_QUICKSTART.zh-CN.md) | **新 Agent / 模型快速接入执行手册**（10 个接入点 + 8 步实操 + 验收 + 排查） |
| [`docs/deepseek-harness/`](docs/deepseek-harness) | **DSH（DeepSeek Harness）执行器**：定义式 Agent vs Prompt 式对比分析、接入配置、自定义 Agent 跨电脑打包复用、样例 agent pack |
| [`docs/superpowers/specs/`](docs/superpowers/specs) | 设计文档（Mimo ACP、控制台 UX、执行器接入等） |
| [`REASONIX.md`](REASONIX.md) | 底层引擎说明 |

> 💡 想接入新的 Agent / 模型（如本机装个 Claude Code、或换个模型）？直接看 `NEW_AGENT_QUICKSTART.zh-CN.md`，按步骤改 10 个接入点即可，无需改框架核心。

---

## 🏗️ 技术架构（简）

```
┌─────────────────────────────────────────────────────────┐
│  桌面应用 cmd/orchestrator-app（WebView2，无控制台窗口）  │
│    或 浏览器控制台（start.bat → 127.0.0.1:8788）          │
├─────────────────────────────────────────────────────────┤
│  前端（单页 HTML）：hero / 画布 / 运行看板 / Console / 历史 │
├─────────────────────────────────────────────────────────┤
│  internal/serve：Orchestrator HTTP/SSE API + 前端         │
├─────────────────────────────────────────────────────────┤
│  internal/orchestrator：                                │
│    Loop 状态机（迭代/审查/终止） · Pipeline DAG          │
│    RuntimeManager（codex/mimo/claude 保留进程）          │
│    审查者对照设计文档 · AI 进展分析接口                    │
├─────────────────────────────────────────────────────────┤
│  internal/executor/{codex,mimo,claude}：CLI/协议适配      │
│  （codex app-server WS · mimo ACP stdio · claude stream-json）│
└─────────────────────────────────────────────────────────┘
浏览器 / 应用窗口 永远只连 Orchestrator 的 HTTP/SSE，不直连 Provider。
```

---

## 📦 目录结构

```
├─ cmd/orchestrator-app      # 原生桌面应用（WebView2）
├─ cmd/reasonix              # CLI / serve 入口
├─ internal/executor/        # codex / mimo / claude / opencode / dsh 执行器适配
├─ internal/orchestrator/    # Loop、Pipeline、RuntimeManager、审查协议
├─ internal/serve/           # HTTP/SSE 服务 + 前端（orchestrator_frontend）
├─ scripts/                  # start-desktop / start-orchestrator 等
├─ docs/                     # 设计文档、使用说明
└─ NEW_AGENT_QUICKSTART.zh-CN.md  # 新 Agent 快速接入手册
```

---

## ⚠️ 安全

- **API Key 绝不进入仓库**：key 只存在于本机（`~/.codex`、`~/.claude-deepseek` 等），代码/文档用占位符；
- 桌面应用内嵌的模型子进程（reasonix）从 `bin/` 或 PATH 解析，需与 `orchestrator-app.exe` 同目录。

## 📄 License

MIT —— 见 [LICENSE](LICENSE)。
