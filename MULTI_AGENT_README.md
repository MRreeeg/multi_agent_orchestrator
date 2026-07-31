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

Codex 节点当前使用一次性非交互命令：

```text
codex exec ... -
```

完整 Prompt 从 stdin 传入，避免 Windows `The command line is too long`。`codex exec resume` 可以延续 Codex thread；`codex exec-server` 是协议/WebSocket 服务入口，不能直接当成 Mimo/Reasonix 的 retained `serve` runtime。

Mimo 的典型方式是：

```text
mimo serve
mimo run --attach <runtime-url>
```

如果使用 CCSwitch，请先由用户启动 CCSwitch 并开启路由，然后把 Reviewer 配置为：

```text
executor: codex
model: ccs
providerRoute: ccswitch
```

`ccs` 是路由别名，不是要传给 Codex 的真实模型 ID；不要手工配置一个不存在的 `--model ccs`。

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
