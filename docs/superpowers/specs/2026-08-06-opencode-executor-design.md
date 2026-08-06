# 接入 opencode 执行器与 DeepSeek 官方 API 设计

> 日期：2026-08-06
> 范围：`multi_agent_orchestrator`（Reasonix / Multi-Agent Orchestrator）
> 状态：已批准（用户确认方案后进入实施计划）

## 背景与目标

用户本机已安装 opencode CLI（1.18.14，原生 exe 可直接被 Go 调用），其模型列表包含
免费的 `opencode/deepseek-v4-flash-free`。目标：

1. 在编排器中新增 `opencode` 执行器，支持 run（一次性）与 serve（保留运行时）两种模式，
   节点可选择免费模型 `opencode/deepseek-v4-flash-free` 或官方模型
   `deepseek/deepseek-v4-flash`。
2. 接入 DeepSeek 官方 API（`deepseek-v4-flash`），API key 取自
   `D:\code\appii\deepseek.txt`，同时配置到 opencode 与 reasonix 两侧。
3. 每步修改同步 git 提交并推送 GitHub，维护 `docs/调试记录.md`。

非目标：

- 不改变现有执行器（reasonix / mimo / codex / claude）行为；
- 不处理"循环卡死"遗留问题（另行跟进）；
- 不提交任何 API key 到 git（`.env` 已在 .gitignore）。

## 一、opencode 执行器

### run 模式（一次性）

- 命令：`opencode run -m <provider/model> --format json [--session <id>] <prompt>`；
- 解析 JSON 事件流：累加 `{"type":"text",...,"text":...}` 为最终文本，
  取 `step_finish` 的 tokens/cost，`sessionID` 供续跑；
- 新包 `internal/executor/opencode/opencode.go`：
  - 二进制发现：优先 `node_modules/opencode-ai/bin/opencode.exe`（npm 全局布局），
    回退 PATH（跳过 .ps1 shim）；
  - `proc.HideWindow(cmd)` 防黑框；
  - 长 Prompt 走 stdin。

### serve 模式（保留运行时）

- 命令：`opencode serve --port <随机端口> --hostname 127.0.0.1`，`cmd.Dir = workspace`；
- 新包 `internal/executor/opencode/client.go`（HTTP 客户端，封装 opencode server API）：
  - `POST /session` `{title}` → sessionID；
  - `POST /session/{id}/message` `{parts:[{type:"text",text}], model:"provider/model"}`
    → 最终文本；
  - `POST /session/{id}/abort` → 中断（保留会话）；
  - `GET /session/{id}/message` → 历史（Runtime Console）；
  - `POST /session/{id}/permissions/{pid}` `{response:"deny"}` → 自动拒绝权限请求；
  - 实时事件：订阅 `GET /event`（SSE），解析 `message.part.delta` /
    `message.part.updated` 喂入 Runtime Console（推理/回答分块）；
    控制台轮询由前端负责（复用 codex/mimo Runtime Console 模式）。
- 新 `internal/orchestrator/opencode_runtime.go`：
  - OpenCodeRuntimeManager：spawn、状态机（starting/idle/busy/error/stopped）、
    Runtime Console 快照、interrupt/stop；
  - runtimeKey = `nodeID|model|workspace|providerRoute`（与其余执行器一致）。

### 注册点（镜像快速接入手册 10 点）

| # | 位置 | 改动 |
|---|---|---|
| 1 | `internal/orchestrator/types.go` | `ExecutorOpencode ExecutorType = "opencode"` |
| 2 | `internal/executor/opencode/`（新建） | run 一次性 + serve HTTP 客户端 |
| 3 | `internal/orchestrator/opencode_runtime.go`（新建） | 保留运行时管理 |
| 4 | `internal/orchestrator/pipeline.go` | `executors` map 注册 `OpenCodePipelineExecutor` |
| 5 | `internal/orchestrator/pipeline.go` | 节点校验：run/serve、模型格式 |
| 6 | `internal/orchestrator/pipeline.go` | `resolveExecutorModelRef`：opencode 原样透传 |
| 7 | `internal/orchestrator/pipeline.go` | serve → `runtime_console` 访问模式 |
| 8 | `internal/serve/orchestrator.go` | `nodeTypes` 加 opencode + 模型列表 |
| 9 | `internal/serve/orchestrator.go` | runtime 路由分派到 OpenCodeRuntimeManager |
| 10 | 前端 | 零改动（nodeTypes 驱动） |

模型列表（opencode 执行器）：`opencode/deepseek-v4-flash-free`（免费）、
`deepseek/deepseek-v4-flash`、`deepseek/deepseek-v4-pro`。

## 二、DeepSeek 官方 API

- **opencode 侧**：将 appii key 写入 opencode DeepSeek 凭据
  （`~\.local\share\opencode\auth.json` 或 `DEEPSEEK_API_KEY` 环境变量），
  使 `deepseek/deepseek-v4-flash` 可用；
- **reasonix 侧**：`reasonix.toml` 增加官方 provider
  （kind=openai，base_url=`https://api.deepseek.com`，模型 `deepseek-v4-flash`，
  `api_key_env=DEEPSEEK_API_KEY`），`.env` 写入 appii key（gitignored）。

## 三、调试记录与 git 同步

- 新建 `docs/调试记录.md`：记录每次改动的日期、内容、验证结果；
- 分步 commit + push 到 `origin`（github.com/MRreeeg/multi_agent_orchestrator）：
  ① run 执行器 → ② serve 运行时 → ③ 注册/前端 → ④ DeepSeek 配置 → ⑤ 文档；
- 每步先构建 + 验证，再提交；key 只进 `.env`，不进 git。

## 四、验证

- run：opencode 免费模型端到端跑流水线节点；
- serve：单节点 serve 跑通 + Runtime Console 可见流式事件；
- DeepSeek 官方：opencode 与 reasonix 各一次真实调用（用 appii key）；
- 黑框：opencode 子进程加 HideWindow，跑节点无黑框；
- `go build` / `go vet` 相关包通过，前端 JS 语法校验通过。

## 风险与回滚

- 风险：opencode server API 以实测为准（POST /session 请求体、message parts 格式），
  实现中以真实调用校准；均为新增代码，不影响现有执行器；
- 回滚：撤销新增文件与注册点即可；`.env` 中的 key 可随时移除。
