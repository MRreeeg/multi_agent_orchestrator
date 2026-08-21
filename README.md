<div align="center">

# 🧭 Multi-Agent Orchestrator

[English](README.md) · [简体中文](README.zh-CN.md)

**Say one sentence — let the steward handle the rest.** A cross-agent orchestration console: the architect thinks, executors build, a reviewer gates every round, and you can step in anytime.

[![Go](https://img.shields.io/badge/Go-1.25+-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/dl/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)
[![Platform](https://img.shields.io/badge/Platform-Windows%20%7C%20macOS%20%7C%20Linux-555?style=for-the-badge)](#)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen?style=for-the-badge)](#contributing)

![Demo: one sentence to orchestration and execution](docs/screenshots/demo.gif "TODO: 3-second demo GIF — type a requirement → auto orchestration → execute → review passed")

</div>

---

## 📦 Features

| | |
|---|---|
| 🗣 **One-sentence orchestration** | Type a requirement → auto-decomposed into an "Architect → Executor → Reviewer" DAG, with serial / parallel / fan-in support |
| 🔄 **Loop state machine** | `fixed` exact N rounds or `review_decides` reviewer-gated; every round is an auditable, resumable Iteration record |
| 🎯 **Feedback targets** | Drag a dashed line from the reviewer onto any node to choose who receives review feedback — multiple parallel targets, even back to the architect for a re-think |
| 🔌 **6 executors** | reasonix · mimo · codex · claude · opencode · dsh — independent model routing per node, mix freely |
| 💰 **Cost-first** | Strong model plans, cheap/free models execute (`deepseek-v4-flash-free` etc.), lightweight model reviews |
| 👀 **Auto vision fallback** | Node model can't see images? The orchestrator proactively delegates to a vision-capable helper runtime and injects the result — no prompt-hoping |
| 🩹 **Stall auto-repair** | Watchdog detects stuck nodes → interrupt & guide (serve mode) or kill & rerun |
| 💬 **Human in the loop** | Message/interrupt agents from the Runtime Console, approve tool calls, stop/resume runs — human chat never pollutes Loop history |
| 🤖 **Custom agents** | DSH presets selectable per node; cards clearly mark「custom Agent」vs「prompt-based」 |
| 🔍 **Programmatic self-check** | Probes locally available agents/models without burning AI tokens — plug-and-play on a new machine |
| 📦 **Desktop app** | Native WebView2 window; browser works too |
| 🧩 **Shared skills** | DSH reuses skills already installed for codex/mimo — no duplicate downloads |

## 🚀 Quick Start

> All you need is **Go 1.25+** and a **DeepSeek API key**; every other executor is optional (use whichever you have installed — it runs even with none of them).

```bash
# 1. Clone
git clone https://github.com/MRreeeg/multi_agent_orchestrator.git
cd multi_agent_orchestrator

# 2. Credentials (never committed)
export DEEPSEEK_API_KEY="sk-..."        # macOS / Linux
# $env:DEEPSEEK_API_KEY = "sk-..."      # Windows PowerShell

# 3. Launch (auto-builds and opens the console)
./scripts/start-orchestrator.sh         # macOS / Linux
.\scripts\start-orchestrator.ps1        # Windows
```

Type one sentence into the home input, click 「✨ Generate Orchestration」, review the three generated nodes, then click 「▶ Run」.

![Console home](docs/screenshots/home.png "TODO: hero input + workspace card screenshot")

> Prefer a native desktop app? Double-click `scripts\start-desktop.bat` (Windows).

## 🏗 Architecture

```text
┌─────────────────────────────────────────────────────────────────────┐
│  Your one sentence                                                  │
│     │                                                               │
│     ▼                                                               │
│  ┌──────────┐   rewrite + DAG   ┌────────────────────────────────┐  │
│  │ Requirement│ ───────────────▶ │ Pipeline (architect→executor    │  │
│  │ analysis  │                   │ →reviewer)                      │  │
│  └──────────┘                   └────────────────────────────────┘  │
│                                        │                            │
│                                        ▼  executePipelineV2          │
│  ┌──────────────┐   ┌────────────────┐   ┌──────────────────────┐   │
│  │ 🏗 Architect  │──▶│ ⚒ Executor     │──▶│ 🔍 Reviewer           │   │
│  │ read-only plan│   │ implement+test │   │ pass/revise/blocked   │   │
│  └──────────────┘   └────────────────┘   └──────────┬───────────┘   │
│        ▲              ▲               revise        │ pass           │
│        └──────────────┴─────────────────────────────┘               │
│                          Loop state machine (orchestrator-driven)    │
│  👤 You: message / interrupt / approve tools / stop / resume (Console)│
└─────────────────────────────────────────────────────────────────────┘
```

| Executor | Notes | Typical models |
|---|---|---|
| `reasonix` | Built-in multi-model agent | deepseek-pro / deepseek-flash |
| `mimo` | Xiaomi MiMo | mimo-v2.5 / mimo-v2.5-pro |
| `codex` | OpenAI Codex CLI | ccs (relay) / deepseek-v4-flash |
| `claude` | Claude Code | sonnet / deepseek direct |
| `opencode` | Open-source coding agent | **free models** (`*-free`) |
| `dsh` | DeepSeek Harness | deepseek-v4-flash / pro + custom agents |

## 🧪 Tests

```bash
go build ./...
go test ./internal/executor/dsh ./internal/orchestrator ./internal/serve -count=1
```

## 📚 Docs for Humans & AI Agents

These manuals are written as **structured handover documents**: read them yourself, or hand one wholesale to your AI agent and let it perform the integration/config on its own.

- [`MULTI_AGENT_README.md`](MULTI_AGENT_README.md) — full multi-agent orchestration manual
- [`NEW_AGENT_QUICKSTART.zh-CN.md`](NEW_AGENT_QUICKSTART.zh-CN.md) — wiring up a new executor/model (10 integration points)
- [`docs/deepseek-harness/`](docs/deepseek-harness/) — DSH executor / custom agents / opencode model access; includes ready-to-consume handover docs designed to be executed by an AI agent without further human confirmation
- [`docs/调试记录.md`](docs/调试记录.md) — debugging & pitfall notes

## ❓ FAQ

**Will this burn money?** Cost-first by default: the execution stage can run free models (`opencode/*-free`), each node pins its own model, and token usage is visible in real time.

**Can I intervene mid-run?** Yes — message agents, interrupt the current round, approve/reject tool requests, stop/resume runs. Human chat never pollutes Loop history.

**Do I need all 6 executors?** No. A DeepSeek credential alone runs the whole flow; self-check programmatically probes your machine and adapts to whatever executors/models exist.

**Is my API key safe?** Keys live only in environment variables or local credential files; the repo contains no real secrets (enforced by scanning), and `settings.yaml` never stores keys.

## 🤝 Contributing

PRs welcome — please read [`CONTRIBUTING.md`](CONTRIBUTING.md) first. Attaching a self-check panel screenshot makes issue triage much faster.

## 📄 License

[MIT](LICENSE)

---

<div align="center"><sub>⭐ Star if useful; open an Issue for problems.</sub></div>
