# DeepSeek Harness（DSH）接入文档

把 DeepSeek Harness 作为 Reasonix 多Agent编排控制台的新执行器（`executor=dsh`），并给出"定义式 Agent vs Prompt 式 Harness"的决策分析与自定义 Agent 跨电脑复用方案。

| 文档 | 内容 |
|---|---|
| [`00-创造模式配置交接文档.zh-CN.md`](00-创造模式配置交接文档.zh-CN.md) | **（交接单）** 交给「创造模式」执行的完整配置清单：DSH 设置、agent pack 安装、模型路由、双通道复用、验收 |
| [`01-对比分析-定义式Agent-vs-Prompt式Harness.zh-CN.md`](01-对比分析-定义式Agent-vs-Prompt式Harness.zh-CN.md) | **（重点）** 三种 agent 定义方式对比、是否值得加入、如何加入 |
| [`02-DSH执行器接入与配置.zh-CN.md`](02-DSH执行器接入与配置.zh-CN.md) | `executor=dsh` 使用手册：前置条件、模型/权限语义、已知限制、验收 |
| [`03-自定义Agent打包与跨电脑复用.zh-CN.md`](03-自定义Agent打包与跨电脑复用.zh-CN.md) | 自定义 agent pack 打包/安装/跨电脑复用 |
| [`dsh-agent-pack/`](dsh-agent-pack/) | 可直接分发的样例 pack（三角色 skill + persona 覆盖层 + 安装器） |

## 本次接入内容（代码）

- `internal/executor/dsh/`（新）：`dsh --profile headless` 一次性执行器
- `internal/orchestrator/dsh_pipeline.go`（新）：`DshPipelineExecutor`
- `internal/orchestrator/types.go` / `pipeline.go` / `health.go`：注册、校验、目录
- `internal/serve/orchestrator_frontend/index.html`：执行器/模型下拉与自检面板

## 快速验收

```powershell
go test ./internal/executor/dsh ./internal/orchestrator -count=1   # 单元测试
$env:RUN_INTEGRATION='1'; go test ./internal/executor/dsh -run TestDshHeadless -count=1 -v  # 真实模型
```
