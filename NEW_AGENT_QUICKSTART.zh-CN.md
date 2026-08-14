# 新 Agent / 模型快速接入执行手册（Reasonix 多Agent编排控制台）

> 版本：2026-08-01
> 适用：`<仓库根目录>`（本文件夹）随电脑分发时，其他电脑按本手册即可把新 Agent/模型接入编排控制台。
> 已实现范例：**Claude Code（`executor=claude`）**——本手册以它为例逐步演示，其他 Agent 照此模式扩展。
> 关联：[[MULTI_AGENT_README]]、[[Reasonix Orchestrator Loop 与多执行器功能说明]]（笔记库）、`internal/executor/claude/`（范例代码）。

---

## 0. 手册用途

本文件夹（`multi_agent_pack`）可整体拷贝到任意电脑。拿到后：

1. 编译运行：`go build ./cmd/reasonix`（见 `MULTI_AGENT_README.md`）；
2. 已有执行器：`reasonix` / `mimo` / `codex` / `claude` / `opencode` / `dsh`（DeepSeek Harness，见 `docs/deepseek-harness/`）；
3. 想新增一个 Agent（如 `copilot`、`cursor`、`gemini-cli`）：按本手册第 3~8 节执行，约 8 步完成。

> [!tip] 最快路径
> 新 Agent 的代码结构完全镜像 `internal/executor/claude/` + `internal/orchestrator/claude_runtime.go`，把其中 "claude" 替换成你的执行器名即可；本手册每步都标注了"范例文件"，可直接对照 diff。

---

## 1. 结论速览（前置问题）

### 1.1 同一类型 Agent 能否在一个 Loop 里多个互不影响？

**可以。** 框架按节点隔离：

- 保留 Runtime 的复用键 = `nodeID + model + workspace + providerRoute`（见各 `runtimeKey()`）；
- 不同节点 → 不同 Runtime / 不同会话 / 不同进程（run 模式天然一次性隔离）；
- 同一节点跨 Loop 轮次 → 复用同一 Runtime / 同一会话（上下文延续）。

**唯一共享点**：`ccswitch` 路由是用户自己启动的全局单点。两个节点都写 `model=ccs/providerRoute=ccswitch` 时会共享同一上游模型；要每节点独立模型，请用"自配模型"（见 1.2）。

### 1.2 开启 ccswitch 路由后，还能用节点自己配置的模型吗？

**可以，按节点选择**（`providerRoute` 是每节点字段，不是全局开关）：

| 方式 | 节点配置 | Orchestrator 行为 | 适用 |
|---|---|---|---|
| ccswitch 路由 | `model=ccs / providerRoute=ccswitch` | 省略 `--model`，模型由已启动的 ccswitch 决定 | 统一走代理 |
| 自配模型 | `model=<你的模型名> / providerRoute=`（空） | 透传 `--model <模型名>`，由 Agent CLI 自己的 provider 配置解析 | 每节点独立模型（deepseek、opus、claude-xxx…） |

> 例：执行者 `executor=codex, model=deepseek-xxx, providerRoute=`（自配）；审查者 `executor=codex, model=ccs, providerRoute=ccswitch`（走路由）。两个节点各自独立 Runtime。
> 注：`codex` 的 deepseek 硬拒绝已放开为运行时透传（见第 5 节）。

---

## 2. 架构速览与传输决策

```text
要接入的 Agent CLI 支持什么？
├─ 一次性非交互执行（codex exec / claude -p / mimo run）
│    → run 模式：一次节点 Attempt = 一次 CLI 调用，解析 stdout 文本
├─ 长期保留进程/会话（可 resume、可流式、可中断）
│    ├─ 输出 WebSocket（codex app-server）→ serve：后端连 WS JSON-RPC
│    ├─ 输出 stdio JSON-RPC（mimo acp = ACP；claude stream-json = Agent SDK）
│    │    → serve：spawn 子进程 + stdin/stdout 管道 + 行协议
│    └─ 输出 HTTP/SSE（reasonix serve 旧式）→ serve：HTTP 轮询/SSE
```

要点：

- **浏览器永远不直连 Provider**：Runtime Console 一律走 Orchestrator HTTP/SSE（1.2s 轮询 + SSE 唤醒）；
- 后端 ↔ Provider：WebSocket / stdio / HTTP 皆可，封装在 `internal/executor/<agent>/` 包内；
- 中断语义：能 cancel 当前 turn 且保留会话 → 实现 Interrupt；不能 → 只支持 Stop。

---

## 3. 接入点总表（改哪些文件）

| # | 位置 | 作用 | 必须改 |
|---|---|---|---|
| 1 | `internal/orchestrator/types.go` | `ExecutorXxx ExecutorType` 常量 | ✅ |
| 2 | `internal/executor/<agent>/`（新建） | 执行器包：run 一次性执行 + serve 保留 Runtime 客户端 | ✅ |
| 3 | `internal/orchestrator/<agent>_runtime.go`（新建） | 保留 Runtime 管理：spawn、状态机、console、Interrupt/Stop、包级函数 | ✅（serve 需要） |
| 4 | `internal/orchestrator/pipeline.go` | `executors` map 注册 `ExecutorXxx: &XxxPipelineExecutor{}` | ✅ |
| 5 | `internal/orchestrator/pipeline.go` | `validateNodeExecutionConfigAtWorkspaceWithRoute` 校验 run/serve/providerRoute/模型 | ✅ |
| 6 | `internal/orchestrator/pipeline.go` | `resolveExecutorModelRef` 模型规范化/透传 | ✅ |
| 7 | `internal/orchestrator/pipeline.go` | `runtimeAccessMode`：serve → `runtime_console` | ✅ |
| 8 | `internal/serve/orchestrator.go` | `nodeTypes`：三种职责的 `executors` + `modelsByExecutor` | ✅ |
| 9 | `internal/serve/orchestrator.go` | runtime 路由：list/get/stop/console/message/interrupt 分派 | ✅ |
| 10 | `internal/serve/orchestrator_frontend/index.html` | 一般**零改动**（nodeTypes 驱动）；可选：模型推断/自定义模型入口 | ⚪ |

---

## 4. 分步执行（以 Claude Code 为例）

### 4.1 前置检查（目标电脑）

```powershell
# 1) Go 工具链
go version

# 2) Agent CLI 存在且可用（claude 范例）
claude --version

# 3) 找到原生二进制（Windows：跳过 npm shim，用 node_modules 下的原生 exe）
#    C:\Users\<你>\AppData\Roaming\npm\node_modules\@anthropic-ai\claude-code\bin\claude.exe
#    （代码里 discoverClaudeBin() 会自动找；shim .ps1/.cmd 不能直接 subprocess）

# 4) 鉴权/代理（claude 范例在 ~\.claude\settings.json）
#    env.ANTHROPIC_AUTH_TOKEN + env.ANTHROPIC_BASE_URL=<代理>
```

> 其他 Agent 同样要先确认：CLI 可执行、鉴权/代理配置、`--help` 里与模型/权限/输出格式相关的 flag。

### 4.2 Step 1 — 常量与注册（types.go / pipeline.go）

```go
// types.go
const (
    ExecutorReasonix ExecutorType = "reasonix"
    ExecutorMimo     ExecutorType = "mimo"
    ExecutorCodex    ExecutorType = "codex"
    ExecutorClaude   ExecutorType = "claude" // ← 新增
)

// pipeline.go executors map
executors = map[ExecutorType]PipelineExecutor{
    ExecutorReasonix: &ReasonixExecutor{},
    ExecutorMimo:     &MimoExecutor{},
    ExecutorCodex:    &CodexPipelineExecutor{},
    ExecutorClaude:   &ClaudePipelineExecutor{}, // ← 新增
}
```

范例：`internal/executor/claude/claude_pipeline.go`（`ClaudePipelineExecutor`：run 走 `client.Exec`，serve 走 `claudeRuntimeMgr.Execute`）。

### 4.3 Step 2 — executor 包：run 模式

新建 `internal/executor/<agent>/<agent>.go`（范例：`internal/executor/claude/claude.go`）：

- 命令构造：`<cli> -p --output-format json [--resume <sid>] [--model <m>] [--permission-mode <mode>]`；
- **长 Prompt 走 stdin**（Windows 命令行长度限制）：短 Prompt 作为位置参数，长 Prompt 用 `cmd.Stdin = strings.NewReader(prompt)`；
- 结果解析：`result`（最终文本）+ `session_id`（供 `--resume` 跨轮复用）+ `errors`/`usage`；
- 权限映射：`approvalMode=auto → bypassPermissions`、`ask → dontAsk`（与 codex/mimo 语义一致）；
- 二进制发现：跳过 npm shim，找原生 exe（`discoverClaudeBin()` 范例）。

### 4.4 Step 3 — executor 包：serve 客户端（保留 Runtime）

新建 `internal/executor/<agent>/sdk_client.go`（范例：`internal/executor/claude/sdk_client.go`）：

- 传输：spawn 子进程 + stdin/stdout 管道 + **JSON Lines 行协议**（bufio.Scanner，16MB buffer）；
- 握手：等 `system/init`（拿 `session_id`）→ 之后每次 Turn 写一条 `user` 消息；
- 流式累积：`assistant` 消息 / `stream_event` delta（`text_delta`→文本、`thinking_delta`→推理），按 Turn 累积最终文本；
- 结束：等 `result` 行（含 `session_id`/`total_cost_usd`/`errors`）；
- **竞态保护**：waiter 提前退出（ctx 取消）时，结果存 `pending`/`settle` 屏障，下一个 Prompt 先排空，杜绝旧 Turn 结果错配给新 Turn；
- 权限控制协议：init 后发送 `control_request {subtype:"initialize", protocolVersion:"1.0"}` 开启工具审批，`permission/can_use_tool` 请求由策略应答（auto→allow / ask→deny）；
- `Interrupt()`：发 `control_request {subtype:"interrupt"}`，保留会话；
- `Close()`：关 stdin（优雅退出）+ fail 所有 pending；进程树由 Runtime 层 kill。

### 4.5 Step 4 — 保留 Runtime 管理（镜像 mimo_runtime.go）

新建 `internal/orchestrator/<agent>_runtime.go`（范例：`internal/orchestrator/claude_runtime.go`，直接对照 `mimo_runtime.go`）：

- `XxxRuntimeManager`：`runtimes map[key]*runtime` + `SetUpdateSink`（SSE 桥）+ `notify`；
- 复用键 `nodeID|model|workspace|providerRoute`；spawn 保留进程（`newRetainedRuntimeCommand`），`cmd.Dir = workspace`；
- 就绪：轮询等握手完成（init/初始化 RPC），超时给 stderr 诊断；
- 状态机：`starting → idle ↔ busy → error / stopped`；`reserveTurn`（busy 互斥）/ `finishTurn`（interrupt → idle 且保留会话；其他错误 → error）；
- Console：`consoleStreamCoalescer` 合并流式增量（`text_delta`→assistant、`thinking_delta`→reasoning，400ms/边界落盘）；
- `Snapshot / Send（manual_turn）/ Interrupt / Stop` + 包级函数 `List/Get/Stop/GetConsole/SendMessage/Interrupt` + 全局 `xxxRuntimeMgr`；
- 权限策略：`auto → allow`、`ask → deny`。

### 4.6 Step 5 — pipeline / loop / persistence 接线

- `pipeline.go` `runtimeAccessMode`：`executor==Xxx && mode==serve → "runtime_console"`；
- `pipeline.go` `validateNodeExecutionConfigAtWorkspaceWithRoute`：`case ExecutorXxx:` 校验 run/serve、`providerRoute`（空或 ccswitch）、模型非空（ccswitch 除外）；
- `pipeline.go` `resolveExecutorModelRef`：自配模型**原样透传**（范例：`case ExecutorClaude: return model`）；
- `managedRuntimeState`：加 `xxxRuntimeMgr.Get`；
- `loop.go`：release 分支、retained reviewer（serve 且 reviewer 时保留会话跨轮）、`stopManagedRuntime` 分支；
- `persistence.go`：重启把 retained（codex/mimo/claude）运行时标 `stopped`、保留 Thread/Session 供 resume。

### 4.7 Step 6 — serve API 与 nodeTypes

`internal/serve/orchestrator.go`：

- `nodeTypes`：三种职责的 `Executors` 数组加 `ExecutorXxx`；`ModelsByExecutor[Xxx]` 填预设模型；
- runtime 路由：`listRuntimes` / `getRuntime` / `stopRuntime` / `getRuntimeConsole` / `sendRuntimeMessage` / `interruptRuntime` 各加一个分派分支（范例见 claude 分支）。

### 4.8 Step 7 — 前端（一般零改动）

- 执行器下拉 / 模型下拉由 `/nodes/types` 返回的 `executors` + `modelsByExecutor` 驱动 → 新增执行器自动出现；
- 可选增强（范例已做）：`inferExecutorFromModel` / `modelSupportsExecutor` / `enforceNodeConstraints` 增加新执行器分支；模型下拉加"✏️ 自定义模型"入口支持自配模型。

### 4.9 Step 8 — 测试与验收

单测（协议级，用伪进程/net.Pipe 覆盖，范例见 `internal/executor/claude/*_test.go`）：

- run：JSON 结果解析、权限映射、二进制发现；
- serve 客户端：握手、流式累积、竞态（pending/settle）、中断、权限 allow/deny、错误结果、断连 fail-pending；
- runtime：状态机、复用键、Interrupt 保留会话、Stop 清理、console coalescer、重启恢复。

E2E（真实模型，env 门控）：

```go
// internal/orchestrator/<agent>_e2e_test.go
if os.Getenv("RUN_INTEGRATION") != "1" { t.Skip("set RUN_INTEGRATION=1 ...") }
```

```powershell
$env:RUN_INTEGRATION='1'
go test ./internal/orchestrator -run TestClaudeServeReturnsVisibleTextEndToEnd -count=1 -v
```

---

## 5. 模型配置：ccs 路由 vs 自配模型（含放开 codex deepseek）

| 场景 | 节点配置 | 结果 |
|---|---|---|
| 走 ccswitch | `executor=codex|claude, model=ccs, providerRoute=ccswitch` | Orchestrator 省略 `--model`，模型由 ccswitch 决定 |
| 自配模型 | `executor=codex|claude, model=deepseek-xxx, providerRoute=` | 透传 `--model deepseek-xxx`，由 CLI 自身 provider 配置解析 |

- `ccs` 是路由别名：代码里在 `executeNodeWithLoopProtocolAtWorkspace` 中识别 `ccs/ccswitch` → `providerRoute=ccswitch, modelRef=""`；
- **codex 的 deepseek 硬拒绝已放开**（`validateNodeExecutionConfigAtWorkspaceWithRoute` 中删除了 `isDeepseekModel` 报错），模型可用性交给 CLI 运行时判断；自配模型不可用时错误会出现在节点 stderr，而非框架静默失败。

---

### 5.1 实例：codex 直连 DeepSeek 官方（不走中转/ccs）

需求：执行者用 `executor=codex, model=deepseek-v4-flash`（官方直连），审查者继续走 `providerRoute=ccswitch`（cc-switch 本地代理）。

> [!important] cc-switch 是什么（2026-08-02 核实）
> **cc-switch（CCSwitch）不是模型，是"模型配置器"**：它在 SQLite（`~/.cc-switch/cc-switch.db`）里管理多个 Codex provider 的完整 TOML 配置 + API key（如 DeepSeek 官方、Right Code 中转站、Xiaomi MiMo 等），切换时把选中 provider 的配置**整体写入 `~/.codex/config.toml` + `auth.json`**。因此：
> - 它同一时刻只有一个"当前激活"配置（全局）；
> - 它会重写/删掉你手写进 config.toml 的自定义段（`[model_providers.deepseek]` 可能被清掉）；
> - 多个节点想同时用不同 provider，**不能依赖 cc-switch 的全局切换**。

> [!important] 解决方案：profile 覆盖层（与 cc-switch 解耦）
> codex 的 `--profile <name>` 加载 `$CODEX_HOME/<name>.config.toml` 覆盖层，叠加在基础 config 之上。把 cc-switch 里的每个 provider 导出成独立覆盖层（自包含：显式 bearer token、`requires_openai_auth=false`，不依赖会被切换的全局 config.toml / auth.json），框架按节点加载对应 profile：
> - **run 模式**：`codex --profile <name> exec ...`
> - **serve 模式**：`codex app-server` 不接受 `--profile`，但接受嵌套 `-c` 覆盖 → 框架把 profile 文件解析成 `-c key=value`（如 `model_providers.custom.base_url=...`）传给 app-server

```powershell
# 一键从 cc-switch 导出 profile（cc-switch 里改配置后重跑一次）
python scripts/sync_codex_profiles.py
#   生成 deepseek.config.toml（DeepSeek 官方直连，deepseek-v4-flash）
#   生成 ccs.config.toml（Right Code 中转站，gpt-5.6-luna）
```

```toml
# ~/.codex/deepseek.config.toml（自包含）
model_provider = "custom"
model = "deepseek-v4-flash"
[model_providers.custom]
name = "deepseek"
base_url = "https://api.deepseek.com"
wire_api = "responses"
requires_openai_auth = false
experimental_bearer_token = "<你的 DeepSeek key>"

# ~/.codex/ccs.config.toml（自包含）
model_provider = "custom"
model = "gpt-5.6-luna"
model_reasoning_effort = "high"
[model_providers.custom]
name = "Right Code"
base_url = "https://www.rightapi.ai/codex/v1"
wire_api = "responses"
requires_openai_auth = false
experimental_bearer_token = "<你的中转站 key>"
```

框架按节点自动选择（`codexProfile`，已实现）：

| 节点配置 | run 模式 | serve 模式 |
|---|---|---|
| `model=deepseek-v4-flash, providerRoute=`（空） | `codex --profile deepseek exec -m deepseek-v4-flash` | app-server + 从 `deepseek.config.toml` 解析的 `-c` 覆盖（官方直连） |
| `model=ccs, providerRoute=ccswitch` | `codex --profile ccs exec`（gpt-5.6-luna） | app-server + 从 `ccs.config.toml` 解析的 `-c` 覆盖（中转站） |
| `model=o3 / codex-default` 等 | 不传 profile（用基础配置） | 不传 `-c`（用基础配置） |

实测结果（2026-08-02）：
- 执行者链路：`codex -p deepseek exec -m deepseek-v4-flash` 与 serve E2E → 官方直连返回文本；
- 审查者链路：`codex -p ccs exec -m gpt-5.6-luna` 与 serve E2E → rightapi 中转返回文本；
- **两路 serve 同时共存**（`TestCodexServeDeepseekOfficialEndToEnd` + `TestCodexServeCCSRightcodeEndToEnd`，各 ~3s）——互不干扰，且**不依赖 cc-switch 是否切换**；
- runtime 启动失败时会**保留 error 状态**供 Console 显示诊断（不再 404）。

> 另一台电脑接入步骤：① 在 cc-switch 里配好 DeepSeek / Right Code 两个 Codex provider；② 跑 `python scripts/sync_codex_profiles.py` 生成覆盖层；③ 控制台节点 `executor=codex, model=deepseek-v4-flash`（自配）或 `model=ccs, providerRoute=ccswitch`（路由）。

### 5.2 实例：Claude Code 直连 DeepSeek 官方（CLAUDE_CONFIG_DIR 独立配置）

需求：claude 节点用 `deepseek-v4-flash` 官方直连，同时保留默认 `~/.claude`（right.codes/ccs 代理）配置。

官方接入方式（DeepSeek 文档）：`ANTHROPIC_BASE_URL=https://api.deepseek.com/anthropic` + `ANTHROPIC_AUTH_TOKEN=<key>` + 模型映射（opus→deepseek-v4-pro、sonnet/haiku→deepseek-v4-flash）。

> [!important] 不覆盖现有配置
> 用 `CLAUDE_CONFIG_DIR` 指向独立配置目录，完全不动默认 `~/.claude`。目录（含 API key）**只存在于本机**，不进仓库。

本机已配置 `~/.claude-deepseek/settings.json`：

```json
{
  "env": {
    "ANTHROPIC_BASE_URL": "https://api.deepseek.com/anthropic",
    "ANTHROPIC_AUTH_TOKEN": "<你的 DeepSeek API Key>",
    "ANTHROPIC_MODEL": "deepseek-v4-pro[1m]",
    "ANTHROPIC_DEFAULT_OPUS_MODEL": "deepseek-v4-pro[1m]",
    "ANTHROPIC_DEFAULT_SONNET_MODEL": "deepseek-v4-pro[1m]",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL": "deepseek-v4-flash",
    "CLAUDE_CODE_SUBAGENT_MODEL": "deepseek-v4-flash",
    "CLAUDE_CODE_EFFORT_LEVEL": "max"
  }
}
```

框架按节点自动设置 `CLAUDE_CONFIG_DIR`（`claudeConfigDir`，已实现）：

| 节点配置 | 行为 |
|---|---|
| `executor=claude, model=deepseek-v4-flash`（或任意 deepseek-*） | 设 `CLAUDE_CONFIG_DIR=~/.claude-deepseek`（可用 `CLAUDE_DEEPSEEK_CONFIG_DIR` 覆盖）→ DeepSeek 官方直连 |
| `executor=claude, model=ccs, providerRoute=ccswitch` / 其他模型 | 不设 → 用默认 `~/.claude`（right.codes / cc-switch 代理） |

实测（2026-08-02 修复后全部可用）：
- run：`CLAUDE_CONFIG_DIR=~/.claude-deepseek claude -p --output-format json --model deepseek-v4-flash "只回复OK"` → 官方返回 OK（~3s，`modelUsage.deepseek-v4-pro[1m]`）；
- serve（stream-json）：**已修复 init 死锁并实测跑通**——claude 2.1.x 只在收到第一条 stdin `user` 消息后才发 init，框架原先"spawn 后只等 init"会死锁。修复：serve 不再单独等 init，第一次真实 turn 即触发 init，`Execute/Send` 在 turn 后从客户端捕获 session id。`TestClaudeServeDeepseekOfficialEndToEnd`（`RUN_INTEGRATION=1`）两轮真实跑通（首轮 ~9s 返回 OK，第二轮复用同一 session）。

> 换电脑：把 `~/.claude-deepseek/settings.json` 里的 key 换成自己的，框架代码无需改动。

## 6. 验收命令清单

```powershell
cd <本文件夹根目录>   # 例如 <仓库根目录>

# 单测（覆盖 executor / orchestrator / serve）
go test ./internal/executor/claude ./internal/orchestrator ./internal/serve -count=1

# 静态检查 + 编译
go vet ./internal/executor/claude ./internal/orchestrator ./internal/serve
go build ./cmd/reasonix
go build ./...

# 前端脚本语法（把 index.html 的 <script> 内容提取到临时文件后检查）
# 或直接跑 serve 测试（TestOrchestratorNodeTypesExposeExecutorSpecificModels 等）

# API 冒烟：/nodes/types 返回新执行器
# （serve 测试 TestOrchestratorNodeTypesExposeExecutorSpecificModels 已覆盖）

# 真实模型 E2E（需要 Provider 路由可用）
$env:RUN_INTEGRATION='1'
go test ./internal/orchestrator -run TestClaudeServeReturnsVisibleTextEndToEnd -count=1 -v
```

---

## 7. 常见问题排查

| 现象 | 排查 |
|---|---|
| 找不到 CLI / 启动失败 | `discoverClaudeBin()` 路径；Windows 用 node_modules 原生 exe，不要用 `.ps1/.cmd` shim |
| init 一直不出 / 无输出 | 确认 `--verbose`（stream-json 必需）；看 `~/.claude/debug` 日志；Provider 代理是否可用 |
| 模型请求 api_retry / Connection error | 外部 Provider 问题，非框架问题；等恢复后跑 E2E |
| 工具调用被拒（serve） | 确认 init 后发送了 `EnablePermissionProtocol()`；`approvalMode=auto` 才 allow |
| Interrupt 后无法再发消息 | 正常流程：interrupt → result 到达 → finishTurn 回 idle；若等不到 result 再 Stop 重启 Runtime |
| 重启后旧 Runtime 显示在线 | 已修：`persistence.go` 把 retained 运行时标 stopped 并保留 Thread/Session |
| 前端看不到新执行器 | 检查 `/nodes/types` 返回（`executors` 数组）；前端是 nodeTypes 驱动，不是写死的 |
| 想用自配模型但被前端拦截 | 用"✏️ 自定义模型"入口；或确认 `modelSupportsExecutor` 对自配模型放行 |
| codex 报 "you passed deepseek-flash/deepseek-v4-flash" | 模型名被 workspace 的 `reasonix.toml` 配置解析成了 `provider/model` 格式。已修复：codex/claude 模型 ref **原样透传**（`resolveExecutorModelRef`），不经 reasonix 配置解析；升级到含该修复的版本后重启 Orchestrator |

---

## 8. 已实现执行器对照表

| 能力 | codex | mimo | claude | opencode | dsh |
|---|---|---|---|---|---|
| run 一次性执行 | ✅ `codex exec` | ✅ `mimo run` | ✅ `claude -p --output-format json` | ✅ `opencode run` | ✅ `dsh --profile headless` |
| serve 保留 Runtime | ✅ app-server WS | ✅ mimo acp（ACP stdio） | ✅ stream-json stdio | ✅ opencode serve | ❌（headless 无保留协议，仅 run） |
| 跨轮会话复用 | ✅ thread/resume | ✅ session/load | ✅ --resume / init session | ✅ | ❌（每次全新会话） |
| 流式事件合并进 Console | ✅ | ✅ | ✅ | ✅ | ❌（一次性 stdout） |
| Interrupt 保留会话 | ✅ | ✅ | ✅ control_request interrupt | ✅ | ❌（一次性进程） |
| Stop 杀进程 | ✅ | ✅ | ✅ 关 stdin + kill | ✅ | ✅ ctx 取消 |
| 人工 Turn（Console） | ✅ | ✅ | ✅ | ✅ | ❌ |
| 权限映射 | ask→never / auto→yolo | ask→reject / auto→allow | ask→deny / auto→allow（控制协议） | ✅ | Trust/auto→`DSH_PERMISSION_MODE=danger-full-access`、只读→`read-only` |
| 节点类型下拉 | ✅ | ✅ | ✅ | ✅ | ✅ nodeTypes |
| 模型独立（多节点） | ✅ 自配模型 | ✅ | ✅ 自配模型 | ✅ provider/model | ⚠️ 受 `$DSH_HOME/settings.yaml` 优先约束（详见 docs/deepseek-harness/02） |

---

## 9. 参考资料

- 方案文档（笔记库）：`G:\工作\学习笔记\多agent项目\方案\Reasonix 多Agent编排控制台 — 新Agent与模型快速接入方案（Claude Code实例）.md`
- 调试记录：`G:\工作\学习笔记\多agent项目\Reasonix Orchestrator 调试记录.md` L-23 / L-24
- 范例代码：`internal/executor/claude/`（claude.go / sdk_client.go / *_test.go）、`internal/orchestrator/claude_runtime.go`
- **DSH（DeepSeek Harness）执行器范例**：`internal/executor/dsh/`（dsh.go / *_test.go）、`internal/orchestrator/dsh_pipeline.go`，文档见 `docs/deepseek-harness/`（对比分析 / 接入配置 / 自定义 Agent 打包复用）
- Claude 协议：`github.com/Roasbeef/claude-agent-sdk-go/blob/main/docs/cli-protocol.md`、docs.runloop.ai Claude SDK Protocol
- Mimo ACP 设计：`docs/superpowers/specs/2026-08-01-mimo-acp-websocket-design.md`
