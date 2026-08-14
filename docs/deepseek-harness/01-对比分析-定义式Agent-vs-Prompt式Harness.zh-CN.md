# 对比分析：定义式 Agent（DSH）vs Prompt 职责描述式 Harness（Reasonix）vs 客制化 Agent 功能

> 版本：2026-08-14 ｜ 适用：`<仓库根目录>`（Reasonix 多Agent编排控制台）＋ DeepSeek Harness（DSH）
> 关联：[[02-DSH执行器接入与配置.zh-CN.md]]、[[03-自定义Agent打包与跨电脑复用.zh-CN.md]]、本文件夹内的 `dsh-agent-pack/`
> 目标读者：需要在"换个执行器"和"重新定义 agent 体系"之间做决策的人。

---

## 0. TL;DR（结论先行）

**这不是"谁更好"的二选一，而是"谁来控制循环"的两种哲学。**

| 方式 | 控制平面 | 一句话本质 |
|---|---|---|
| Prompt 职责描述式（Reasonix 现状） | **Orchestrator（外部）** | agent = 一段文本；流程由 Go 编排器决定 |
| 定义式 Agent（DSH） | **agent 自身（内部）** | agent = persona + 工具目录 + skills + 自治原语的结构化组合 |
| 客制化 Agent 功能（创作模式） | 定义式之上的一层 | 把"agent 行为"从文本升级为可编程、可打包、可版本化的制品 |

**三个直接回答：**

1. **谁更好？** 看任务形状。确定性多角色流水线（架构师→执行者→审查者、固定轮数、要审计、要人干预）→ **Reasonix 外部编排更好**；单 agent 深度自治（长任务自主拆解、目标驱动、多子代理并行）→ **DSH 定义式更好**。二者互补，不是替代。
2. **是否值得加入？** **值得，而且已经在本次接入完成**——但不是"换个引擎跑 prompt"，而是把 DSH 作为一个**具备定义式能力的新执行器**加入 Reasonix，让同一个 Loop 里可以混排 reasonix / mimo / codex / claude / opencode / **dsh**。真正的大头收益在第二步：把 Reasonix 的角色职责迁移成 DSH 的 **skills + persona 定义层**（本文件夹 `dsh-agent-pack/` 已给出可运行样例）。
3. **如何加入？** 分三个层次：①执行器层（本次已完成）；②定义层（角色 → SKILL.md/persona）；③打包层（自定义 agent pack 跨电脑一键复用）。详见第 5 节。

---

## 1. 三种方式的本质区别

### 1.1 Prompt 职责描述式 —— Reasonix 的现状

```text
节点 = rolePrompt(architect.md) + RoleDesc(用户填的职责) + SkillContent(注入) + 当前轮 input
       └── 全部拼成一段文本，每次调用全量塞进 prompt ──┘
```

- **agent 的定义是文本**：`internal/orchestrator/prompts/*.md` + 每个节点的 `RoleDesc`。
- **控制平面在编排器**：谁执行、几轮、何时停（`pass/revise/blocked`）、上下文要不要延续，都由 Go 的 Loop 状态机决定，agent 无权自己开 Goal、自己循环。
- **优点**：直观、透明、可审计；每个节点独立配置；不依赖任何外部框架。
- **代价**：
  - 职责边界靠"提示词约束"（"禁止写文件"只是建议，不是强制——如果执行器不听话或模型忽略，没有硬边界）；
  - 复用靠复制文本（同一套职责在另一个项目/电脑要重新粘贴）；
  - 上下文成本：角色描述 + 职责 + skill 全文每次全量注入（token 是实打实的钱）；
  - 表达能力上限 = 自然语言。

### 1.2 定义式 Agent —— DeepSeek Harness（DSH）

```text
agent = persona(身份)                       ← cordis.patch.yml / --patch / agent preset
      + 工具目录(能力清单，模型可见)          ← tools registry：fs/pwsh/skill/goal/subagent/workflow/web…
      + skills(SKILL.md，whenToUse 按需加载) ← 项目 .agents/skills、用户 $DSH_HOME/skills
      + preset(standard / code / minimal)   ← 一套完整的 agent 平面组合
      + 自治原语(goal / subagent / workflow / ralph loop)
      + 模型路由(agent-default-model)        ← $DSH_HOME/settings.yaml
      + 权限沙箱(permission presets)         ← read-only / workspace-write / danger-full-access（强制）
```

- **agent 的定义是结构化组合**（cordis 配置树：bundle patch 层叠 → profile 层 → home 层 → `--patch` 覆盖层）。
- **控制平面在 agent 内部**：goal 工具让 agent 自己维持长期目标、subagent 让它可以并行委托、workflow 让它编排多阶段、ralph loop 让它用全新上下文迭代。**这是 Reasonix 明确禁止节点做的事**（`不得自行设定 Goal、不得自行循环`）。
- **优点**：
  - 能力是"声明 + 强制执行"而非"描述"：`read-only` 沙箱真的只读；`danger-full-access` 审批策略真的 `never`；
  - skill **按需加载**：目录里挂 20 个 skill，只有匹配 `whenToUse` 的才把正文加载进上下文，省 token；
  - 一次定义、处处复用：preset / skill / persona 是文件，可 diff、可版本化、可分发；
  - 有自治深度：goal / subagent / workflow 是**一等公民**，不是 prompt 里模仿出来的。
- **代价**：
  - 复杂：cordis 插件层 + 配置树 + preset 隔离 realm，学习曲线陡；
  - 自治 agent 需要信任边界（它真的会自己开 Goal、自己跑子代理）；
  - 对编排器而言是"黑盒"：外部很难在它内部循环的中途插一脚（Reasonix 的 Stop/Interrupt 语义对 headless 不适用）。

### 1.3 客制化 Agent 功能 —— 创作模式 / 自定义工具

- 在定义式之上的一层**可编程扩展**：自定义 persona、自定义 preset（复制 `standard` 改工具目录）、自定义 skill、Code Mode（PTC：模型写一个 TypeScript 程序组合多步操作，一次往返完成五个工具调用）、cordis 插件（自定义工具/服务）。
- **本质**：agent 行为从"文本"提升为"代码/配置制品"——可测试、可版本化、可分发。
- 这正是"如何把本地客制化的 agent 放到别的电脑直接用"的答案载体（见第 5 节第 3 步 + `03-自定义Agent打包与跨电脑复用.zh-CN.md`）。

---

## 2. 逐维度对比

| 维度 | Prompt 式（Reasonix 节点） | 定义式（DSH agent） | 客制化（创作模式） |
|---|---|---|---|
| 表达载体 | 文本（md + RoleDesc） | 结构化配置（persona/工具/skills/preset） | 代码/配置制品（skill、preset、插件、PTC） |
| 职责边界 | 提示词约束（软） | 沙箱 + 审批策略 + 工具目录（硬） | 由你写的插件/工具决定 |
| 上下文成本 | 角色+职责+skill 全文每次全量注入 | persona 固定 + skill 按需加载 | 自定义工具描述计入目录（稳定前缀，可缓存） |
| 复用/分发 | 复制文本 | 文件即制品（可 diff/可打包） | 打包成 agent pack / npm 插件 |
| 跨电脑 | 每次重粘 | `install.ps1` 一键装到 `$DSH_HOME` / workspace | 同左（含插件依赖） |
| 可控性 | 高（编排器全权） | 中（agent 自治，外部只能约束边界） | 取决于你封装的粒度 |
| 可审计性 | 高（每节点一次调用） | 中（agent 内部多步，需要 console/会话日志） | 中 |
| 多执行器混排 | ✅ 原生 | ✅（作为执行器混入 Loop） | ✅ |
| 单 agent 长任务自治 | ❌（被编排器限制） | ✅ goal/subagent/workflow | ✅ 可深度定制 |
| 上手成本 | 低 | 中高 | 高 |
| 外部依赖 | 无 | Node.js + `@deepseek-ai/dsh` | 同左 + 你的代码 |

---

## 3. 关键洞察：控制平面不同 → 适用场景不同

**Reasonix 的 loop 是"编排器控制"**：审查者只输出 JSON 决策，下一轮由 Orchestrator 决定。这是**确定性、可审计、可干预**的设计——它害怕 agent 自作主张。

**DSH 的 loop 是"agent 自控制"**：goal 工具让 agent 自己维持目标直到完成，subagent/workflow 让它自己拆分并行。这是**深度自治**的设计——它信任 agent 的能力。

所以：

| 场景 | 谁更合适 | 说明 |
|---|---|---|
| 确定性多角色流水线、固定轮数、要人工看板 | **Reasonix 编排** | Loop 状态机 + Runtime Console + 每节点独立模型路由 |
| 同一个 Loop 里混排多执行器（deepseek 官方/ccs/claude/…） | **Reasonix 编排** | 本次 `executor=dsh` 就是干这个的 |
| 单 agent 长任务自主迭代（"把这个项目研究透并产出报告"） | **DSH 定义式** | goal + subagent + workflow + ralph loop |
| 同一批自定义 agent 能力跨电脑复用 | **定义式 + 打包** | skills + patch overlay + install 脚本 |
| 需要确定性 + 需要深度 | **两者嵌套** | Reasonix 编排器把 DSH 当作一个"自治子任务执行器"（dsh 节点内部可用 DSH 的 goal/subagent 完成一个大块，再把结果交回编排器审查） |

> ⚠️ 嵌套有一个边界：Reasonix 的 Loop 语义要求节点"一次调用只产出一轮结果"。DSH 节点内部可以自治（它的 goal/subagent 在 headless 进程里跑），但**必须收敛成一个最终答复**交回给编排器，不能把整个 Loop 搬进节点。

---

## 4. 是否值得加入？—— 值得，但要选对位置

### 4.1 加入的三个层次

| 层次 | 内容 | 状态 |
|---|---|---|
| ① 执行器层 | `executor=dsh`，run 模式（`dsh --profile headless`） | ✅ 本次已实现并通过真实模型 E2E |
| ② 定义层 | 把 Reasonix 角色（architect/executor/reviewer）迁移成 DSH skills + persona | 📦 已提供样例 `dsh-agent-pack/`，接入即用 |
| ③ 打包层 | 自定义 agent pack 跨电脑一键安装复用 | 📦 已提供 `install.ps1` + 安装说明 |

### 4.2 值得加入的理由（为什么不是"又换了一个模型"）

1. **补上 Reasonix 没有的"硬边界"**：DSH 的 read-only 沙箱对架构师/审查者节点是强制的，而 prompt 式只能"恳求"模型别写文件。
2. **skill 按需加载省 token**：同一份职责，DSH 只在匹配时加载正文。
3. **自治能力成为编排资产**：dsh 节点可以在一次调用内部完成"读代码 → 拆任务 → 并行子代理 → 汇总"，Reasonix 只需要管好节点边界。
4. **自定义 agent 变成可分发制品**：这是你"别的电脑直接复用"需求的正解（见 03 文档）。

### 4.3 成本与风险（诚实清单，v1 现状）

| # | 风险/限制 | 现状 | 缓解 |
|---|---|---|---|
| 1 | 运行时依赖：Node.js + `@deepseek-ai/dsh` | 需要目标电脑安装 | 二进制发现已支持 `node <bin.js>` / PATH / `DSH_BIN`；文档给出 `npm i -g` 命令 |
| 2 | headless 无保留会话 | 每次调用全新会话，`ContextPolicy` 无效，跨轮上下文不延续 | 文档明确；需要跨轮上下文时用 codex/claude serve |
| 3 | 任务只能走命令行位置参数 | Windows 命令行 ~32K 限制，超长 prompt 会失败 | 文档给出阈值；根治需 DSH 上游支持 stdin（已列为建议） |
| 4 | 模型路由受限 | `$DSH_HOME/settings.yaml` 的 `agent-default-model` 优先于 `--patch` 覆盖 | 严格按节点路由需专用 `DSH_HOME`（executor 已支持 `DshHome`） |
| 5 | 无 token/usage 回传 | DSH headless 不在 stdout 输出 usage，控制台统计缺失 | 文档注明；后续可解析会话日志 |
| 6 | 首次运行要初始化 headless profile | 第一次调用较慢（自动初始化） | 可预先跑一次 `dsh --profile headless "ping"` 预热 |
| 7 | headless 无交互审批 | `ask` 策略会卡住工具调用 | 实现已映射：`Trust/auto → danger-full-access`、`ToolsReadOnly → read-only` |

---

## 5. 如何加入（三步路线图）

### 第 1 步：执行器接入（✅ 本次完成）

代码改动一览（都合入 `<仓库根目录>`）：

| 文件 | 改动 |
|---|---|
| `internal/executor/dsh/`（新） | 执行器包：`dsh --profile headless` 一次性执行、二进制发现（node bin.js/PATH/DSH_BIN）、`--patch` 模型覆盖、`DSH_PERMISSION_MODE` 映射、`Active code page` 横幅剥离 |
| `internal/orchestrator/dsh_pipeline.go`（新） | `DshPipelineExecutor`：run-only、Skill 注入、权限映射 |
| `internal/orchestrator/types.go` | `ExecutorDsh = "dsh"` |
| `internal/orchestrator/pipeline.go` | executors 注册、`resolveExecutorModelRef` 透传、`validateNodeExecutionConfigAtWorkspaceWithRoute` 校验（run-only、允许空模型、拒绝 providerRoute）、空 mode 默认 run |
| `internal/orchestrator/health.go` | `CheckExecutors` 探测 + `NodeTypeCatalog` 暴露 dsh 及其模型 |
| `internal/serve/orchestrator_frontend/index.html` | 执行器下拉/模型下拉/自检面板显示 dsh；dsh 节点 mode 锁定 run |

验收：`go test ./internal/executor/dsh ./internal/orchestrator -count=1`；真实模型 E2E：`$env:RUN_INTEGRATION='1'; go test ./internal/executor/dsh -run TestDshHeadless -count=1 -v`。

### 第 2 步：定义层迁移（📦 本包已给出）

把三个角色 prompt 迁移成 DSH skills（`dsh-agent-pack/skills/`）：

| Reasonix 概念 | DSH 概念 | 迁移产物 |
|---|---|---|
| `prompts/architect.md` | skill `reasonix-architect`（read-only 职责） | `skills/reasonix-architect/SKILL.md` |
| `prompts/executor.md` | skill `reasonix-executor`（实现职责） | `skills/reasonix-executor/SKILL.md` |
| `prompts/reviewer.md` | skill `reasonix-reviewer`（审查职责） | `skills/reasonix-reviewer/SKILL.md` |
| 节点 `RoleDesc` | persona / prompt 前导 | `cordis.patch.yml` 的 `system-prompt.persona` |
| 节点 `Skill` 注入 | DSH 自带 skill 目录发现 | 安装到 `<workspace>/.agents/skills` 或 `$DSH_HOME/skills` |

一个 SKILL.md 同时被两套系统消费：Reasonix 读 frontmatter `description:` 进下拉菜单（按目录名注入 prompt）；DSH 按 `name/whenToUse` 做按需加载。**这是"加入"的最优形态：不复制体系，只映射语义。**

### 第 3 步：打包层（📦 已提供，见 03 文档）

`dsh-agent-pack/` 是一个可直接分发的目录：skills + `cordis.patch.yml` + `install.ps1` + `settings.example.yaml`。目标电脑执行 `install.ps1` 即可把整套自定义 agent 能力装进 `$DSH_HOME`（用户级）或 workspace（项目级）或临时 `--patch`（单次）。

进阶（需要时再做）：
- 每节点独立模型：给每个模型建一个专用 `DSH_HOME`（executor 的 `DshHome` 已支持），或编辑 `$DSH_HOME/settings.yaml`；
- 更重的自治：把 DSH 的 **workflow** 封装成一个 skill，dsh 节点一次调用跑完整的多阶段工作流；
- 自定义工具/插件：cordis 插件打包进 profile 的 `node_modules`（`dsh plugin --profile <name> add <pkg>`）。

---

## 6. 建议与下一步

- **如果目标是"多执行器混排、可审计流水线、控制台看板"**：保持 Reasonix 编排，dsh 作为执行器之一（现状即可用）。
- **如果目标是"把我客制化的 agent 复制到所有电脑直接用"**：用 `dsh-agent-pack` + `install.ps1`，并在每台电脑 `npm install -g @deepseek-ai/dsh` + 配置各自的 `$DSH_HOME/settings.yaml`（模型）与凭据。
- **如果目标是"单 agent 深度自治研究/编码任务"**：直接用 DSH 的 goal / workflow，把 Reasonix 当作批量调度入口。
- **长期升级项（建议反馈给 DSH 上游）**：headless 支持从 stdin 读任务（解决长 prompt）；headless 输出 usage JSON（解决统计）；可选 `--model` 直传（解决模型路由）。

---

## 附：参考

- Reasonix 接入手册：`NEW_AGENT_QUICKSTART.zh-CN.md`（10 个接入点清单，dsh 是第 6 个执行器范例）
- DSH 行为：`dsh --help`、`dsh --profile headless --help`、`$DSH_HOME/settings.yaml`、`~/.dsh/profiles/web/cordis.patch.yml`
- 本文件夹：`02-DSH执行器接入与配置.zh-CN.md`、`03-自定义Agent打包与跨电脑复用.zh-CN.md`、`dsh-agent-pack/`
