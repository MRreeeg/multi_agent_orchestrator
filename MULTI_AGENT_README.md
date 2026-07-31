# Multi-Agent Orchestrator Pack

这是从 `DeepSeek-Reasonix-dev` 当前源码导出的独立 Multi-Agent / Orchestrator 控制台包，包含：

- Architect / Executor / Reviewer 节点编排；
- `review_decides` 与 `fixed` Loop；
- LoopConfig 保存、PipelineRevision、Run 复制；
- Resume、Iteration 查询、运行记录和历史会话；
- Reviewer JSON 协议 `loop-review-v1`；
- Skill Catalog、项目级技能和前端自动分配；
- Workspace 绑定与 Run Workspace 快照。

## 最简单的启动方式（Windows）

在资源管理器中双击：

```text
start.bat
```

或者在 PowerShell 中执行：

```powershell
.\start.ps1
```

启动脚本会自动：

1. 使用当前目录作为默认工作区；
2. 使用 `<仓库根目录>\.data\orchestrator` 保存控制台历史；
3. 编译 `./cmd/reasonix`；
4. 启动 `127.0.0.1:8788`；
5. 打开 `http://127.0.0.1:8788/orchestrator`；
6. 尝试开启工具自动审批。

## 常用参数

```powershell
# 换端口
.\start.ps1 -Port 8789

# 不自动打开浏览器
.\start.ps1 -NoBrowser

# 指定独立数据目录
.\start.ps1 -DataDir D:\reasonix-data

# 指定 Agent 实际工作的目录
.\start.ps1 -WorkspaceDir G:\work\my-project
```

也可以直接调用：

```powershell
.\scripts\start-orchestrator.ps1 -NoBrowser
```

## 运行前置条件

- Windows PowerShell；
- Go 1.25 或更高版本；
- 可用的模型/provider 配置；
- 如果模型配置使用远程 provider，需要对应的 API Key、网络和账户余额。

Reasonix 的全局配置默认不放在本目录中，而是由 `REASONIX_HOME` 控制。为了在另一台电脑上迁移，可以把配置放在本目录，例如：

```powershell
$env:REASONIX_HOME = "$PWD\.reasonix-home"
.\start.ps1
```

然后在 `.reasonix-home` 中准备对应的 `config.toml` 和凭据。不要把真实 API Key 提交到 GitHub。

## 目录说明

```text
cmd/                         Reasonix CLI 入口
internal/orchestrator/       Loop 和编排核心
internal/serve/              HTTP API 与内嵌前端
internal/serve/orchestrator_frontend/
                             Multi-Agent 控制台页面
.reasonix/commands/          项目命令
.reasonix/skills/            项目级技能
scripts/start-orchestrator.* 启动脚本
.data/                       本地历史数据（运行后生成，不提交）
```

## 验证源码

```powershell
go test ./internal/orchestrator ./internal/serve
go vet ./internal/orchestrator ./internal/serve
go build ./cmd/reasonix
```

完整程序启动后，控制台地址为：

```text
http://127.0.0.1:8788/orchestrator
```

## 与原项目的关系

这个目录是源码副本，不会读取原项目的 `.reasonix-orchestrator` 历史，也不会复用原项目的默认工作目录。两边可以同时存在；如果两边都使用 8788 端口，需要给其中一边指定不同端口。

本包没有复制旧的运行记录、编译产物、`.git` 历史和个人调研报告，因此首次启动会是干净的控制台。模型/provider 仍然属于外部运行环境，不会因为复制 Go 源码自动迁移。


## Loop 的真实语义

Canvas 只保留一组节点：

```text
architect → executor → reviewer
```

不要为了三轮创建九个节点，也不需要手动画 `reviewer → executor` 回边。Loop 由 Orchestrator 在 `PipelineRun` 中创建 `Iteration 1/2/3`，并复用同一组节点/AgentBinding。

- `review_decides`：`pass` 提前结束，`revise` 进入下一轮，`blocked` 终止；
- `fixed`：Reviewer 每轮都执行，即使 `pass` 也继续到 `fixedIterations`，最后以 `fixed_limit` 结束；
- Reviewer 只返回 `loop-review-v1`，不负责启动下一轮。

## Codex、Mimo 与 CCSwitch

### Codex：`run` 与 retained `serve` 的明确分工

| 节点模式 | Orchestrator 命令/协议 | 上下文与生命周期 |
|---|---|---|
| `run` | `codex exec ... -` | 一次性执行；完整 Prompt 经 stdin 传入，避免 Windows `The command line is too long`。 |
| `serve` | `codex app-server --listen ws://127.0.0.1:<port>` | 后端建立 loopback JSON-RPC WebSocket，并在同一 Runtime 中复用 Codex Thread。 |

`serve` 的每一次 Orchestrator 节点执行只创建一个 Codex Turn，随后等待 `turn/completed`。`PipelineRun` / `Iteration` 的推进仍只由 Orchestrator 的 Reviewer JSON 决策控制；Runtime 本身不会自行开始下一轮。

Runtime 的复用键为 `nodeID + model + workspace + providerRoute`。同一 Loop 的后续节点尝试会复用同一 Runtime 与 Thread；`ProviderSession.ExternalSessionID` 持久化 Thread ID。服务重启后，旧进程和 WebSocket **不会被假装恢复为在线**：历史 Runtime 会标为 stopped 并清理失效端点/PID，但保留 Thread ID，下一次 `serve` 会新启 App Server 并执行 `thread/resume`。

Runtime 状态统一为：执行时 `busy`、完成后 `idle`、连接/非预期 Turn 错误为 `error`、显式停止为 `stopped`。**Interrupt** 仅中断当前 Turn，保留 Runtime 与 Thread，完成后重新可发送新 Turn；只有 **Stop** 会关闭 WebSocket 并停止 `codex app-server` 子进程。

### Runtime Console（仅 Codex 第一版）

Canvas 节点的 retained Codex Runtime 可打开 **Runtime Console**：查看状态、端点、Thread/Turn、最终输出和原始事件；空闲时可发送人工新 Turn，运行时可 Interrupt 当前 Turn。

> [!important] 边界
> 浏览器只访问 Orchestrator 的 HTTP API / SSE，**不会直接连接 Codex Provider WebSocket**。人工 Turn 仅用于调试或补充上下文：不会创建、恢复或推进 `PipelineRun` / `Iteration`，也不会污染 Loop 历史。

App Server 事件通过 Orchestrator SSE 立即唤醒 Console 刷新，并保留 1.2 秒轮询作为断线恢复兜底。第一版协议只适用于 Codex；Mimo 等执行器未来需要各自适配，不复用 Codex JSON-RPC。

页面刷新和节点刚启动时，前端会将 `pipeline_node_runtime` SSE 临时事件与持久化 `RuntimeState` 合并，而不是覆盖。这样 `accessMode=runtime_console`、Runtime ID、Thread ID 不会丢失：Canvas 始终打开 Orchestrator Runtime Console，绝不会尝试由浏览器直接访问 `ws://`。

### Mimo

Mimo 的典型方式是：

```text
mimo serve
mimo run --attach <runtime-url>
```

### CCSwitch

如果使用 CCSwitch，请先由用户启动 CCSwitch 并开启路由，然后把节点配置为：

```text
executor: codex
model: ccs
providerRoute: ccswitch
```

`ccs` 是路由别名，不是要传给 Codex 的真实模型 ID。Orchestrator 会省略 `--model` / app-server `model` 字段，实际模型由用户已开启的 CCSwitch 路由决定。

### 明确不支持：`codex exec-server`

本版本未将 `codex exec-server` 放入执行路径。它与 `codex app-server` 的 Thread/Turn 生命周期及恢复语义尚未在本项目中独立验证；不能因为二者都使用 WebSocket 就互换使用。

## 三轮验收怎么看

不要只看 Canvas 的“节点 3/3”。应检查 Run 详情：

```text
status=complete
terminationReason=fixed_limit
fixedIterations=3
```

并看到三个不同的 `iterationID`，每轮都有一个 Reviewer Attempt。已核验的成功样例：

```text
run_1785480397543_1
```

它的真实结果是：

| 轮次 | Architect | Executor | Reviewer |
|---:|---|---|---|
| 1 | 执行 | 执行 | Codex/CCS 执行并返回 `pass` |
| 2 | 未重跑 | 执行 | Codex/CCS 执行并返回 `pass` |
| 3 | 未重跑 | 执行 | Codex/CCS 执行并返回 `pass` |

最终执行者报告：`go test 24/24 PASS`、`go build OK`、`go vet OK`、`gofmt -l` 无输出。

## Skill 与 Canvas 显示

当前运行记录已证实：

- Architect 使用 `brainstorming`；
- Executor 使用 `executing-plans`；
- Reviewer 使用 `review-agent`。

如果 Canvas 节点属性中的 Skill 仍为空，不等于运行时没有 Skill；当前版本仍存在“运行时绑定存在、节点 `skillIDs` 没有完全回写”的显示差异。该差异列入后续改进，不要用 Canvas 空字段否定 Attempt 里的实际记录。

## 迁移到其他电脑

需要迁移源码之外的运行前置条件：

1. Go 1.25+；
2. Reasonix 全局配置/凭据，或设置本地 `REASONIX_HOME`；
3. Codex、Mimo、CCSwitch 等 CLI；
4. API Key、网络和 CCSwitch 路由；
5. 目标工作区的读写权限。

本包默认：

```text
代码：当前目录
历史：.data\orchestrator
工作区：当前目录，可用 -WorkspaceDir 改变
```

不要把 `.data`、provider session、runtime 或 API Key 上传到 GitHub。源码包不会自动携带个人模型账户。

## 常用验证

```powershell
go test ./internal/executor/codex -count=1
go test ./internal/orchestrator -count=1
go test ./internal/serve -count=1
go vet ./internal/executor/codex ./internal/orchestrator ./internal/serve
go build ./cmd/reasonix
```

完整的 Loop、Resume、历史 Pipeline、Skill 和多执行器说明见：

- `G:\工作\学习笔记\多agent项目\Reasonix Orchestrator Loop 与多执行器功能说明.md`
- `G:\工作\学习笔记\多agent项目\Reasonix Orchestrator 调试记录.md`
