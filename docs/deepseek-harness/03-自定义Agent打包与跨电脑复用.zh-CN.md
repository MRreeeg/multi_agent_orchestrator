# 自定义 Agent 打包与跨电脑复用（DSH Agent Pack）

> 版本：2026-08-14 ｜ 适用：想把本地客制化的 DSH agent（persona / skill / 模型预设）复制到任意电脑直接复用的人
> 关联：[[02-DSH执行器接入与配置.zh-CN.md]]、[[01-对比分析-定义式Agent-vs-Prompt式Harness.zh-CN.md]]
> 样例：本文件夹 `dsh-agent-pack/`（可直接分发）

---

## 1. 一句话方案

**DSH 的自定义 agent = 一堆文件**（skills 的 `SKILL.md` + persona 的 `cordis.patch.yml` + 模型的 `settings.yaml`）。把它们放进一个目录（agent pack），用一个 `install.ps1` 装到目标电脑的 `$DSH_HOME`（用户级）或 workspace（项目级），就完成了"别的电脑直接复用"。

```text
my-agent-pack/
├── README.md               # 这个 pack 是什么、怎么用
├── install.ps1             # 一键安装器（三种安装模式）
├── cordis.patch.yml        # persona / agent 行为覆盖层（可选）
├── settings.example.yaml   # agent-default-model 示例（不含 key）
└── skills/
    ├── <skill-name>/SKILL.md   # 一个 skill 一个目录
    └── ...
```

---

## 2. DSH 从哪些地方发现自定义 agent 能力

### 2.1 Skills（技能）

| 来源 | 路径 | 生效范围 |
|---|---|---|
| 项目级 | `<workspace>/.dsh/skills/` 或 `<workspace>/.agents/skills/` | 该工作区内的 DSH 会话 |
| 用户级 | `$DSH_HOME/skills/`（默认 `~/.dsh/skills`）或 `~/.agents/skills/` | 该电脑上所有 DSH 会话（**推荐的共用落点**） |
| 自定义 | `customSkillDirs` 配置（可加任意目录） | 仅当该 provider **未被禁用** 时（见下方警告） |

> [!WARNING] `customSkillDirs` 在 web profile 下静默失效
> `@deepseek-ai/dsh-web-app` 会在补丁层把 **host 平面的 `skill-filesystem` 行显式禁用**
> （其原文：*the base host `skill-filesystem` row is disabled here (presets own local discovery)*）。
> 因此写进 `$DSH_HOME/cordis.patch.yml` 的 `customSkillDirs` 命中一个 disabled 行——
> **不报错，skill 也不会出现**。验证：`dsh --profile web --dump-config | findstr /C:"skill-filesystem" -A 6`，
> 会看到 `disabled: true`。
>
> 真正生效的是各 preset 自己挂载的 provider，而**它们都会扫描默认用户根 `$DSH_HOME/skills`（rank 400）**。
> 所以共用本机已有 skill 的正确做法是：把外部 skill 目录**链接**进 `$DSH_HOME/skills`（见 5.2 节）。

格式（两种都认）：

```text
<root>/<name>/SKILL.md      # 目录 bundle（推荐）
<root>/<name>.md            # 扁平文件
```

`SKILL.md` 头部 frontmatter 必须有 `name`（kebab-case）和 `description`，可选 `whenToUse`：

```markdown
---
name: reasonix-architect
description: 架构师节点：只读分析与方案设计，产出实施清单与验收标准。
whenToUse: 当节点承担架构师职责，需要设计并交给下游执行时。
---

（正文：完整指令）
```

> 模型只在你明确说"加载 X skill"或任务命中 `whenToUse` 时，才把正文加载进上下文——**目录里挂多少个 skill 都不烧 token**。

### 2.2 Persona（身份/系统提示词）

- 用户级：`$DSH_HOME/cordis.patch.yml`（home 层，所有 profile 生效）
- 单次：`dsh --profile headless --patch ./cordis.patch.yml "<task>"`（临时覆盖层，优先级最高）
- 内容示例：

```yaml
# cordis.patch.yml —— 把系统提示词换成你的身份
- id: system-prompt
  config:
    persona: >-
      你是一名资深系统架构师，工作目录是 {{cwd}}，模型是 {{model}}。
      只输出方案，不写代码。
```

### 2.3 模型预设

`$DSH_HOME/settings.yaml`：

```yaml
agent-default-model:
  provider: deepseek-official   # DSH 内置官方直连路由
  model: deepseek-v4-pro
  reasoningEffort: high
```

> API Key 永不进仓库：走环境变量 `DEEPSEEK_API_KEY` 或本机 `$DSH_HOME/.credentials.yaml`。

---

## 3. 三种安装模式（install.ps1 已实现）

| 模式 | 命令 | 装到哪 | 适合 |
|---|---|---|---|
| 用户级 | `.\install.ps1 -Mode user` | `$DSH_HOME/skills/` + `$DSH_HOME/cordis.patch.yml` | 这台电脑所有 DSH 会话都用 |
| 项目级 | `.\install.ps1 -Mode project -Workspace G:\work\my-project` | `<workspace>/.agents/skills/` | 只在某个项目/工作区生效（不污染全局） |
| 临时 | `.\install.ps1 -Mode temp -Task "..."` | 不落盘，直接拼 `--patch` 跑一次 | 验证 persona/skill 效果 |

安装后验证：

```powershell
dsh --profile headless "列出你已加载的 skills"        # 看 DSH 侧
# 或 Reasonix 控制台 /selfcheck → Skill 库           # 看 Reasonix 侧（若装到 Reasonix 技能根）
```

---

## 4. 与 Reasonix 的双通道复用（本包的核心巧思）

一个 `SKILL.md` **同时被两套系统消费**：

1. **Reasonix 通道**：Reasonix 从 `~/.config/reasonix/skills/`（或 `REASONIX_SKILL_DIR`）发现 `<name>/SKILL.md`，读 `description:` 进节点 Skill 下拉；选中后把正文**注入节点 prompt**（现有机制，所有执行器一致，包括 dsh）。
2. **DSH 通道**：DSH 从 `$DSH_HOME/skills/` / workspace `.agents/skills/` 发现同一个文件，按 `whenToUse` **按需加载**。

所以：**把 `dsh-agent-pack` 装到 Reasonix 技能根（用户级）→ 控制台 Skill 下拉出现三个角色 skill；同时装到 `$DSH_HOME/skills` → dsh 节点内部自动获得同样的能力。** 一份文件，两套语义，不复制体系。

---

## 5. 跨电脑操作清单（别人拿到 pack 后）

```powershell
# 1) 装 DSH
npm install -g @deepseek-ai/dsh
dsh --version

# 2) 配凭据（环境变量即可，不进仓库）
$env:DEEPSEEK_API_KEY = "sk-..."

# 3) 装 agent pack（用户级，全电脑生效：skills + persona + 4 个客制化 agent 预设）
cd my-agent-pack
.\install.ps1 -Mode user

# 3b)（强烈推荐）让 DSH 共用本机其他 agent 已装的 skill——只建链接，不复制、不重装
.\install.ps1 -Mode user -ShareSkills
#   或单独运行（可先 -DryRun 预览）：
pwsh -File .\share-skills.ps1 -DryRun
pwsh -File .\share-skills.ps1

# 4) （可选）按需改模型
notepad $env:DSH_HOME\settings.yaml    # agent-default-model

# 5) 验证
dsh --profile headless "你是什么角色？有哪些 skill？"
```

> 第 3b 步用 **NTFS 目录联接（junction）** 把外部 skill 链进 `$DSH_HOME/skills`：文件仍只有一份
> （在原目录里），DSH 任何预设都能发现；skill 监视器生效，**链接后无需重启**即出现在 agent 的 skill
> 目录中。默认来源：`~/.codex/skills`（含 `.system`）、mimocode 内置 skill、`G:\codex\skillpack\codex_skills`；
> 用 `-Source 'D:\my-skills'` 指定其他目录。
>
> 跨电脑时目标机器上没有的目录会自动跳过，脚本会给出"skip (missing root)"提示——不会因为某台机器
> 没装 Codex 就报错。

### 5.2 共用本机已有 skill 的正确做法（链接而非复制）

```powershell
# 预览：列出会链接哪些 skill、来自哪个目录
pwsh -File .\share-skills.ps1 -DryRun

# 执行：为每个外部 skill 建 junction -> $DSH_HOME/skills/<name>
pwsh -File .\share-skills.ps1

# 之后在 Codex/mimocode 里新增的 skill：重跑一次脚本即可（新增项自动补链）
```

| 维度 | `customSkillDirs` patch | junction 链接到 `$DSH_HOME/skills`（本方案） |
|---|---|---|
| web profile 是否生效 | ❌ 命中被禁用的 host 行，静默失效 | ✅ 所有 preset 的 provider 都扫该根 |
| 是否复制文件 | 不复制（但根本没生效） | 不复制（指针，原目录仍持有唯一副本） |
| 原目录更新是否跟随 | — | ✅ 立即跟随（同一份文件） |
| 新增 skill 是否需重启 | — | ❌ 不需要（skill 根被监视） |
| 是否要改 shipped preset | — | ❌ 不需要（只往用户根加链接） |


### 5.1 客制化 agent 预设的跨电脑使用（自检自动导入）

客制化 agent 预设（`presets/<id>/`）随 pack 一起分发，第 3 步已装进 `$DSH_HOME/.agent-presets/<id>/`。别人电脑上：

1. `git clone`（或拉取）仓库 → 运行第 3 步的 `install.ps1 -Mode user`；
2. 启动 Reasonix 控制台 → `/selfcheck` 的「客制化 DSH Agent」区**自动导入**并列出 4 个预设；
3. dsh 节点的「客制化 Agent」下拉直接可选（管家 / 架构师 / 执行者 / 审查者）。

自检只负责**导入展示**已安装的预设；安装动作由 install.ps1 完成（预设目录零依赖，复制即用）。

> 如果 pack 里有自定义 cordis 插件依赖（`@deepseek-ai/...`），用 `dsh plugin --profile web add <pkg>`（web）或给 headless profile 装依赖后分发 `package.json`。skills/persona 本身零依赖，跨机复制即用。

---

## 6. 进阶：把"客制化 agent 功能"做得更重

| 想要的能力 | 做法 | 复用方式 |
|---|---|---|
| 自定义 persona / 职责 | `cordis.patch.yml`（persona） | pack 内文件 |
| 自定义技能 | `SKILL.md` | pack 内文件 |
| 自定义工具 | cordis 插件 + `dsh plugin --profile <name> add <pkg>` | npm 包 + profile 分发 |
| Code Mode（PTC） | 复制 `standard` preset 改 `tool-presentation.mode: code` | preset 目录分发 |
| 每节点独立模型 | 每模型一个专用 `DSH_HOME`（settings.yaml） | 脚本生成 + `DshHome` 支持 |
| 深自治工作流 | 把 DSH workflow 封装成 skill，dsh 节点一次调用跑完多阶段 | SKILL.md 内嵌 workflow 用法 |

---

## 7. 与本次代码接入的关系

- 执行器层（`executor=dsh`）让你**在 Reasonix 里用** DSH；
- 打包层（本方案）让你**在任意电脑复用** DSH 的自定义能力；
- 二者叠加 = "Reasonix 编排 + DSH 定义式 agent" 的完整闭环：编排器管流程，DSH 管深度，pack 管分发。
