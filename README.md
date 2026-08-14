<div align="center">

# 🧭 Multi-Agent Orchestrator · 多智能体管家

**说一句话，剩下的交给管家。** 跨 Agent 协调编排控制台：架构师想、执行者做、审查者把关，随时人工介入。

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/dl/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux-555?style=for-the-badge)](#)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=for-the-badge)](#contributing)

![演示：一句话生成编排并执行](docs/screenshots/demo.gif "需要补充：3 秒演示 GIF——输入需求→自动编排→执行→审查通过")

</div>

---

## 📦 特性

| | |
|---|---|
| 🗣 **一句话编排** | 输入需求 → 自动拆解成「架构师 → 执行者 → 审查者」DAG，支持串行/并行/汇聚 |
| 🔄 **Loop 状态机** | `fixed` 精确 N 轮 / `review_decides` 审查决定；每轮独立 Iteration 记录，可审计可恢复 |
| 🔌 **6 种执行器** | reasonix · mimo · codex · claude · opencode · dsh，每节点独立模型路由，混用互不干扰 |
| 💰 **成本优先** | 规划用强模型、执行用便宜/免费模型（`deepseek-v4-flash-free` 等）、审查用轻量模型 |
| 💬 **人工介入** | Runtime Console 发消息/打断、工具权限批准卡片、停止/恢复，人工对话不污染 Loop 历史 |
| 🤖 **客制化 Agent** | DSH 预设（管家 等）一键选用；卡片明确标注「客制化 Agent」vs「Prompt 式」 |
| 🔍 **程序化自检** | 纯程序化探测本机可用 Agent/模型（不烧 AI），换电脑即插即用 |
| 📦 **桌面应用** | WebView2 原生窗口，也可浏览器使用 |
| 🧩 **共用 Skill** | DSH 直接复用 codex/mimo 已装 skill，不重复下载 |

## 🚀 快速开始

> 只需要 **Go 1.25+** 和 **DeepSeek API Key**；其他执行器都是可选的（装哪个用哪个，一个都不装也能跑）。

```powershell
# 1. 克隆
git clone https://github.com/MRreeeg/multi_agent_orchestrator.git
cd multi_agent_orchestrator

# 2. 配置凭据（永不进仓库）
$env:DEEPSEEK_API_KEY = "sk-..."

# 3. 启动（自动编译并打开控制台）
.\scripts\start-orchestrator.ps1
```

在首页输入框说一句话，点「✨ 生成编排」，检查自动生成的三个节点，点「▶ 执行」。

![控制台首页](docs/screenshots/home.png "需要补充：首页 hero 输入框 + 工作目录卡片截图")

> 想要原生桌面应用？双击 `scripts\start-desktop.bat`。

## 🏗 架构

```text
┌─────────────────────────────────────────────────────────────────────┐
│  你的一句话                                                          │
│     │                                                               │
│     ▼                                                               │
│  ┌──────────┐   rewrite + DAG   ┌────────────────────────────────┐  │
│  │ 需求分析   │ ───────────────▶ │ 流水线（架构师→执行者→审查者）      │  │
│  └──────────┘                   └────────────────────────────────┘  │
│                                        │                            │
│                                        ▼  executePipelineV2          │
│  ┌──────────────┐   ┌────────────────┐   ┌──────────────────────┐   │
│  │ 🏗 架构师     │──▶│ ⚒ 执行者       │──▶│ 🔍 审查者             │   │
│  │ 只读·方案+验收 │   │ 实现+测试验证    │   │ pass/revise/blocked   │   │
│  └──────────────┘   └────────────────┘   └──────────┬───────────┘   │
│        ▲              ▲               revise        │ pass           │
│        └──────────────┴─────────────────────────────┘               │
│                          Loop 状态机（Orchestrator 控制）             │
│  👤 你：发消息 / 打断 / 批准工具 / 停止 / 恢复（Runtime Console）       │
└─────────────────────────────────────────────────────────────────────┘
```

| 执行器 | 说明 | 典型模型 |
|---|---|---|
| `reasonix` | 内置多模型 Agent | deepseek-pro / deepseek-flash |
| `mimo` | 小米 MiMo | mimo-v2.5 / mimo-v2.5-pro |
| `codex` | OpenAI Codex CLI | ccs（中转） / deepseek-v4-flash |
| `claude` | Claude Code | sonnet / deepseek 官方直连 |
| `opencode` | 开源编码 Agent | **免费模型**（*-free） |
| `dsh` | DeepSeek Harness | deepseek-v4-flash / pro + 客制化 Agent |

## 🧪 测试

```powershell
go build ./...
go test ./internal/executor/dsh ./internal/orchestrator ./internal/serve -count=1
```

## 📚 文档

- [`MULTI_AGENT_README.md`](MULTI_AGENT_README.md) — 多 Agent 编排完整说明
- [`NEW_AGENT_QUICKSTART.zh-CN.md`](NEW_AGENT_QUICKSTART.zh-CN.md) — 新执行器接入（10 接入点）
- [`docs/deepseek-harness/`](docs/deepseek-harness/) — DSH 执行器 / 客制化 Agent / opencode 模型接入
- [`docs/调试记录.md`](docs/调试记录.md) — 调试与踩坑记录

## ❓ FAQ

**会不会很烧钱？** 默认成本优先：执行段可切免费模型（`opencode/*-free`），每个节点独立指定模型，Token 统计实时可见。

**中途能插手吗？** 能。给 Agent 发消息、打断当前轮、批准/拒绝工具请求、停止/恢复 Run，人工对话不会污染 Loop 历史。

**必须装 6 个执行器吗？** 不用。只有 DeepSeek 凭据即可跑通全流程；自检会程序化探测并自动适配你机器上的执行器与模型。

**API Key 安全吗？** 只走环境变量或本机凭据文件；仓库内无真实密钥（有扫描保障），`settings.yaml` 永不落盘 key。

## 🤝 Contributing

欢迎 PR。请先阅读 [`CONTRIBUTING.md`](CONTRIBUTING.md)；报告问题时附上「自检面板截图」最快定位。

## 📄 License

[MIT](LICENSE)

---

<div align="center"><sub>⭐ 觉得有用请 Star；有问题开 Issue。</sub></div>
