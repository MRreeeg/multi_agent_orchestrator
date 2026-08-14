# dsh-agent-pack — Reasonix 角色 × DSH 客制化 Agent 样例包

一个可直接分发的 DeepSeek Harness（DSH）agent pack：把 Reasonix 编排控制台的四个角色（前端分析师 · 管家 / 架构师 / 执行者 / 审查者）沉淀为 DSH 的 **客制化 agent 预设 + skills + persona 覆盖层**，安装到任意电脑即可复用。

## 内容

```text
dsh-agent-pack/
├── README.md                 # 本文件
├── install.ps1               # 一键安装器（user / project / temp 三种模式）
├── cordis.patch.yml          # persona 覆盖层（可选，装到 $DSH_HOME 生效）
├── settings.example.yaml     # agent-default-model 示例（不含 API Key）
├── presets/                  # ★ 客制化 agent 预设（装到 $DSH_HOME/.agent-presets/<id>/）
│   ├── frontend-analyst/     # 前端分析师 · 管家：agent.cordis.yml + preset.yml + headless.patch.yml
│   ├── architect/            # 架构师：只读规划 + plan mode，无 goal
│   ├── executor/             # 执行者：实现 + 验证 + 汇报证据
│   └── reviewer/             # 审查者：只读审查，无 shell 工具
└── skills/
    ├── reasonix-frontend-analyst/SKILL.md  # 前端分析师：管家式前端分析/修改
    ├── reasonix-architect/SKILL.md         # 架构师：只读分析 + 方案设计
    ├── reasonix-executor/SKILL.md          # 执行者：实现 + 测试
    └── reasonix-reviewer/SKILL.md          # 审查者：只读审查 + pass/revise/blocked
```

## 客制化 Agent（presets/）与 Skill 的分工

| 制品 | 机制 | 生效位置 |
|---|---|---|
| `presets/<id>/agent.cordis.yml` | DSH **定义式组合**（persona 行 + 工具行裁剪，含 subagent/workflow 等能力） | DSH Web 会话直接选用该 preset |
| `presets/<id>/headless.patch.yml` | 同一职责的 **扁平化补丁**（persona 覆盖 + 工具行禁用） | Reasonix dsh 节点（`executor=dsh`）经 `--patch` 加载 |
| `skills/reasonix-*/SKILL.md` | **Prompt 式**：Reasonix 注入节点 prompt；DSH 按 `whenToUse` 按需加载 | 两套系统都消费（双通道） |

- Reasonix 自检（`/selfcheck`）会**自动导入** `$DSH_HOME/.agent-presets/*` 下的客制化 agent；dsh 节点配置面板有「客制化 Agent」下拉。
- 节点卡片上会标注实现方式：**客制化 Agent：xxx**（选中预设）或 **Prompt 式**（角色描述 / Skill 注入）。

## 双通道设计

同一个 `SKILL.md` 同时被两套系统消费：

- **Reasonix 通道**：装到 `~/.config/reasonix/skills/`（或 `REASONIX_SKILL_DIR`）→ 控制台节点 Skill 下拉出现，选中后注入节点 prompt（所有执行器一致，含 dsh）。
- **DSH 通道**：装到 `$DSH_HOME/skills/` 或 `<workspace>/.agents/skills/` → dsh 节点（`executor=dsh`）内部按 `whenToUse` 自动按需加载。

## 安装

```powershell
# 用户级（这台电脑所有 DSH 会话 + 客制化 agent 预设 + Reasonix 技能根都生效）
.\install.ps1 -Mode user

# 用户级 + 让 DSH 直接复用本机其他 agent 已下载的 skill（不重复安装）
.\install.ps1 -Mode user -SkillDirs "C:\Users\x\.codex\skills;C:\Users\x\.local\share\mimocode\builtin_skills\0.1.9\skills"

# 项目级（只装进某个 workspace 的技能根）
.\install.ps1 -Mode project -Workspace G:\work\my-project

# 临时（不落盘，直接 --patch 跑一次，验证 persona/skill 效果）
.\install.ps1 -Mode temp -Task "你是什么角色？"

# 临时用某个客制化 agent 跑一次（加载 presets/<id>/headless.patch.yml）
.\install.ps1 -Mode temp -Task "审查 C:\demo\main.go" -Preset reviewer

# 只装 Reasonix 技能根 / 不装预设
.\install.ps1 -Mode user -SkipDsh
.\install.ps1 -Mode user -SkipPresets
```

## 前置条件

- 目标电脑已装 DSH：`npm install -g @deepseek-ai/dsh`
- API Key 走环境变量 `DEEPSEEK_API_KEY` 或本机 `$DSH_HOME/.credentials.yaml`（永不进仓库）
- 模型默认 `deepseek-v4-pro`（`settings.example.yaml`，按需修改后复制为 `$DSH_HOME/settings.yaml`）

## 验证

```powershell
dsh --profile headless "列出你已加载的 skill 并简述架构师 skill 的职责"
.\install.ps1 -Mode temp -Task "用一句话自报身份" -Preset frontend-analyst   # 期望出现「前端分析师 · 管家」
```

或启动 Reasonix 控制台：`/selfcheck` → Skill 库出现 4 个 `reasonix-*` skill，且「客制化 DSH Agent」区出现 4 个本地预设；建一个 dsh 节点可在「客制化 Agent」下拉中选择。

## 详细说明

见上级目录 `03-自定义Agent打包与跨电脑复用.zh-CN.md` 与 `04-客制化Agent导入与自检.zh-CN.md`。
