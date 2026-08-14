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
| 用户级 | `$DSH_HOME/skills/`（默认 `~/.dsh/skills`）或 `~/.agents/skills/` | 该电脑上所有 DSH 会话 |
| 自定义 | `customSkillDirs` 配置（可加任意目录） | 按配置 |

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

# 3b)（强烈推荐）让 DSH 直接复用本机其他 agent 已下载的 skill，不重复安装：
.\install.ps1 -Mode user -SkillDirs "C:\Users\你\.codex\skills;C:\Users\你\.local\share\mimocode\builtin_skills\0.1.9\skills"

# 4) （可选）按需改模型
notepad $env:DSH_HOME\settings.yaml    # agent-default-model

# 5) 验证
dsh --profile headless "你是什么角色？有哪些 skill？"
```

> 第 3b 步把已有 skill 根写进 `$DSH_HOME/cordis.patch.yml` 的 `skill-filesystem.customSkillDirs`（幂等托管块，可重复运行），DSH 的 Web 会话和 headless 节点都能按需发现它们——codex 的 52 个社区 skill、mimocode 内置 skill、`<skill-pack 目录>` 等，一份文件两套系统消费，谁都不用再下载一遍。

### 5.1 客制化 agent 预设的跨电脑使用（自检自动导入）

客制化 agent 预设（`presets/<id>/`）随 pack 一起分发，第 3 步已装进 `$DSH_HOME/.agent-presets/<id>/`。别人电脑上：

1. `git clone`（或拉取）仓库 → 运行第 3 步的 `install.ps1 -Mode user`；
2. 启动 Reasonix 控制台 → `/selfcheck` 的「客制化 DSH Agent」区**自动导入**并列出 4 个预设；
3. dsh 节点的「客制化 Agent」下拉直接可选（前端分析师·管家 / 架构师 / 执行者 / 审查者）。

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
