# DSH 执行器接入与配置（executor=dsh）

> 版本：2026-08-14 ｜ 适用：`<仓库根目录>`
> 关联：[[01-对比分析-定义式Agent-vs-Prompt式Harness.zh-CN.md]]、[[03-自定义Agent打包与跨电脑复用.zh-CN.md]]
> 代码：`internal/executor/dsh/`、`internal/orchestrator/dsh_pipeline.go`

---

## 1. 这是什么

把 **DeepSeek Harness（DSH）** 作为 Reasonix 编排控制台的第 6 个执行器：

```text
executor=dsh  →  dsh --profile headless "<任务>"   （一次性，每次全新会话）
```

DSH 官方一键模式会创建一个全新的持久化会话、驱动 agent 完成工具调用直至静默、把**最后一条助手文本写到 stdout**、以 0（completed）/ 1（error）退出。本执行器把它封装成 `PipelineExecutor`，因此架构师/执行者/审查者三种职责都可以选 `dsh`，与 reasonix / mimo / codex / claude / opencode 混排在同一个 Loop 里。

**与 codex/claude 的关键差异（务必先读）：**

| 维度 | codex / claude | dsh |
|---|---|---|
| 会话 | run 可 resume / serve 保留会话 | **每次全新会话**，无 resume |
| 模式 | run + serve | **仅 run**（headless 无保留协议） |
| 模型 | `--model` 直传 | **无 `--model` 参数**，模型来自 `$DSH_HOME/settings.yaml` |
| 任务输入 | 长 prompt 走 stdin | **命令行位置参数**（Windows ~32K 上限） |
| 审批 | `--permission-mode` | `DSH_PERMISSION_MODE` 环境变量 |

---

## 2. 前置条件（目标电脑）

```powershell
# 1) 安装 DSH CLI（任选其一）
npm install -g @deepseek-ai/dsh          # 推荐：全局安装，路径稳定
# 或 npx @deepseek-ai/dsh ...            # 临时用，每次解析

# 2) 确认可用
dsh --version

# 3) 配置模型与凭据（DSH 自己的设置，不进仓库）
#    $DSH_HOME 默认 ~/.dsh
#    $DSH_HOME/settings.yaml:
#      agent-default-model:
#        provider: deepseek-official
#        model: deepseek-v4-pro
#        reasoningEffort: high
#    API Key：环境变量 DEEPSEEK_API_KEY，或 $DSH_HOME/.credentials.yaml

# 4) 预热 headless profile（首次运行会自动初始化，可先跑一次避免首个节点慢）
dsh --profile headless "reply with: ok"
```

二进制发现顺序（`internal/executor/dsh/dsh.go`）：`DSH_BIN` 环境变量 → npm 全局 / npx 缓存里的 `lib/bin.js`（用 `node <bin.js>` 直启，跳过 `.cmd/.ps1` shim）→ PATH 上的 `dsh`。找不到时节点会报 "binary not found" 类错误（自检面板 `/selfcheck` 也会显示 dsh 不可用）。

---

## 3. 在控制台使用

1. 打开控制台（`start.bat` → `http://127.0.0.1:8788/orchestrator`）。
2. 点击任意节点 → 配置面板 → **执行器选 `DSH`**（三种职责都开放）。
3. 模型下拉选 `deepseek-v4-flash` / `deepseek-v4-pro`，或用"✏️ 自定义模型"输入其他模型名。
4. 模式自动锁定为 `run`（DSH 不支持 serve，下拉被禁用）。
5. 保存 → 执行。

也可以直接调用 API 创建节点（`POST /pipelines`），例如：

```json
{
  "name": "dsh-demo",
  "nodes": [
    {"id": "n1", "type": "architect", "name": "架构师", "model": "deepseek-v4-flash", "executor": "dsh", "mode": "run", "role": "设计模块边界与实施清单"},
    {"id": "n2", "type": "executor", "name": "执行者", "model": "deepseek-v4-flash", "executor": "dsh", "mode": "run", "role": "按架构实现"},
    {"id": "n3", "type": "reviewer", "name": "审查者", "model": "deepseek-v4-flash", "executor": "dsh", "mode": "run", "role": "对照设计文档审查"}
  ],
  "edges": [{"id": "e1", "from": "n1", "to": "n2"}, {"id": "e2", "from": "n2", "to": "n3"}]
}
```

---

## 4. 模型与权限语义

### 4.1 模型

DSH 没有 `--model` 参数。本执行器把节点 `model` 字段翻译成一个**临时 `--patch` 覆盖层**，改写 `agent-default-model` 组合行（provider=`deepseek-official`）：

```yaml
# 临时生成，运行后删除（只含模型名，不含凭据）
- id: agent-default-model
  config:
    provider: deepseek-official
    model: deepseek-v4-flash
```

> ⚠️ **优先级注意**：`$DSH_HOME/settings.yaml` 里保存的 `agent-default-model`（Web 模型页写的就是它）**优先于**任何组合层配置。也就是说：如果用户在 DSH 设置里保存过默认模型，节点的 `--patch` 会被忽略。
>
> - 想要"节点模型说了算"：给节点配一个**专用 `DSH_HOME`**（每模型一个目录，里面只放 `settings.yaml`，key 走环境变量 `DEEPSEEK_API_KEY`）。执行器已支持 `ExecOptions.DshHome`，控制台暂未暴露该字段——需要时在 pipeline 层加一个节点字段即可。
> - 想要"跟随 DSH 默认"：节点 `model` 留空（后端校验允许空模型）。

### 4.2 权限 / 沙箱（重要）

headless 没有交互审批界面。本执行器把 Reasonix 的策略映射成 `DSH_PERMISSION_MODE`：

| Reasonix 节点策略 | DSH 环境 | 效果 |
|---|---|---|
| `ToolsReadOnly`（架构师/审查者只读） | `read-only` | 沙箱只读，不能写文件/执行变更命令 |
| `Trust=true` / `ApprovalMode=auto|yolo`（默认） | `danger-full-access` | 沙箱全开 + 审批 `never`，工具自动执行 |
| 其他 | `workspace-write` | 工作区沙箱 + 审批 `ask`（⚠️ headless 无人应答，可能卡住，不推荐） |

Reasonix 的 Loop 节点默认 `Trust=true`，所以 dsh 节点默认落在 `danger-full-access`。架构师/审查者如果希望强制只读，可在编排层设置只读策略（opencode 节点的 `ToolsReadOnly` 机制同理）。

### 4.3 Skill

- Reasonix 的节点 `Skill` 字段照常**注入 prompt**（与其他执行器一致的 `# SYSTEM-LEVEL SKILL INSTRUCTIONS` 包装）。
- 同时，DSH 自己会从 workspace / `$DSH_HOME` 发现 SKILL.md（见 03 文档）——两条通道互不干扰，可以都生效。

---

## 5. 已知限制与规避

| 限制 | 说明 | 规避 |
|---|---|---|
| 任务长度 | 任务以命令行位置参数传入，Windows 上限约 32K 字符 | 控制节点 prompt 大小；Loop 大上下文时优先 codex/claude；根治需 DSH 支持 stdin |
| 无保留会话 | 每次全新会话，`ContextPolicy` 不生效 | 需要跨轮上下文时用 codex/claude serve 节点 |
| 模型路由 | settings 优先于 `--patch` | 专用 `DSH_HOME` 或清空设置默认 |
| 无 usage 回传 | 控制台 token 统计缺失 | 从 `$DSH_HOME/sessions` 会话日志手工核对 |
| 首次较慢 | headless profile 自动初始化 | 预先 `dsh --profile headless "ok"` 预热 |
| serve 不可用 | 前端锁定 run | 设计上如此（headless 无保留协议） |

---

## 6. 验收命令

```powershell
cd <仓库根目录>

# 单元测试（不联网）
go test ./internal/executor/dsh ./internal/orchestrator -count=1

# 真实模型 E2E（需要 DSH 已安装 + 凭据可用）
$env:RUN_INTEGRATION='1'
go test ./internal/executor/dsh -run TestDshHeadless -count=1 -v

# 全量构建 + 静态检查
go build ./...
go vet ./internal/executor/dsh ./internal/orchestrator ./internal/serve
```

控制台验收：打开 `/selfcheck`，应看到 `DSH` 一行"可用"；节点配置面板执行器下拉里应出现 `DSH`。
