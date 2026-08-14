# dsh-agent-pack — Reasonix 角色 × DSH 定义式 Agent 样例包

一个可直接分发的 DeepSeek Harness（DSH）agent pack：把 Reasonix 编排控制台的三个角色（架构师 / 执行者 / 审查者）沉淀为 DSH 的 skills + persona 覆盖层，安装到任意电脑即可复用。

## 内容

```text
dsh-agent-pack/
├── README.md                 # 本文件
├── install.ps1               # 一键安装器（user / project / temp 三种模式）
├── cordis.patch.yml          # persona 覆盖层（可选，装到 $DSH_HOME 生效）
├── settings.example.yaml     # agent-default-model 示例（不含 API Key）
└── skills/
    ├── reasonix-architect/SKILL.md   # 架构师：只读分析 + 方案设计
    ├── reasonix-executor/SKILL.md    # 执行者：实现 + 测试
    └── reasonix-reviewer/SKILL.md    # 审查者：只读审查 + pass/revise/blocked
```

## 双通道设计

同一个 `SKILL.md` 同时被两套系统消费：

- **Reasonix 通道**：装到 `~/.config/reasonix/skills/`（或 `REASONIX_SKILL_DIR`）→ 控制台节点 Skill 下拉出现，选中后注入节点 prompt（所有执行器一致，含 dsh）。
- **DSH 通道**：装到 `$DSH_HOME/skills/` 或 `<workspace>/.agents/skills/` → dsh 节点（`executor=dsh`）内部按 `whenToUse` 自动按需加载。

## 安装

```powershell
# 用户级（这台电脑所有 DSH 会话 + Reasonix 技能根都生效）
.\install.ps1 -Mode user

# 项目级（只装进某个 workspace）
.\install.ps1 -Mode project -Workspace G:\work\my-project

# 临时（不落盘，直接 --patch 跑一次，验证 persona/skill 效果）
.\install.ps1 -Mode temp -Task "你是什么角色？"

# 只装 Reasonix 技能根（不给 DSH 装）
.\install.ps1 -Mode user -SkipDsh
```

## 前置条件

- 目标电脑已装 DSH：`npm install -g @deepseek-ai/dsh`
- API Key 走环境变量 `DEEPSEEK_API_KEY` 或本机 `$DSH_HOME/.credentials.yaml`（永不进仓库）
- 模型默认 `deepseek-v4-pro`（`settings.example.yaml`，按需修改后复制为 `$DSH_HOME/settings.yaml`）

## 验证

```powershell
dsh --profile headless "列出你已加载的 skill 并简述架构师 skill 的职责"
```

或启动 Reasonix 控制台，`/selfcheck` → Skill 库应出现 `reasonix-architect` / `reasonix-executor` / `reasonix-reviewer`。

## 详细说明

见上级目录 `03-自定义Agent打包与跨电脑复用.zh-CN.md`。
