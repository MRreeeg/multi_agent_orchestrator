# 审查者维护机制（Stall → Review → Repair）设计

- 日期：2026-08-17
- 状态：已批准（含设计审计修正）
- 范围：loop 流水线（架构→实现→审查）中，执行者节点卡住时的自动维护

## 1. 背景与目标

### 问题

用户测试编排全部使用免费/便宜模型（opencode 里免费的 deepseek、免费的 mimo-v2.5）。
这些模型很容易卡住：把输出预算烧在复述计划和探索上、空 assistant turn 失败、
permission 循环、长时间无输出。当前系统对卡住只有"超时"这一个兜底（节点 10min /
审查者 3min / 迭代 30min），超时后整条 run 失败或 interrupted，只能人工接管。

### 目标

审查者节点不仅能审查结果（pass/revise/blocked），还能在**执行者中途卡住**时被唤醒
进行**辅助维护**：诊断卡住原因，输出干预指令（中断+发消息纠正 / 杀掉重跑），
由编排器执行，让执行者恢复流转。转不动时降级到现有超时 + 人工接管路径。

### 非目标（本期不做）

- 普通 pipeline（无审查者节点的 run）的维护干预——卡住仍走原超时 + 人工接管。
- 跳过节点/换模型重跑动作——审查者只输出 nudge/restart/noop 三种判断。
- 修改审查者轮末审查职责——维护者模式是独立调用、独立 JSON schema。
- dsh / run 模式的中途发消息——无 serve runtime 可干预，只能 restart。

## 2. 架构：三层分离

与现有 loop-review 模式同构（审查者输出 JSON → 编排器校验 → 编排器执行）：

```
┌─ StallDetector（编排器·确定性规则）───────┐
│  监控执行中节点的进度信号，按执行器分档     │
│  （观测源见 §4），命中 → 节点标记 stalled    │
└───────────────────────────┬───────────────┘
                            ▼
┌─ Maintenance Planner（审查者·AI 决策）─────┐
│  维护者模式调用（独立于轮末审查）：          │
│  输入现场包（§5.1）→ 输出 maintenance-      │
│  plan-v1 JSON（§5.2）→ schema 校验 →        │
│  失败 1 次系统纠正重试 → 仍失败则放弃        │
└───────────────────────────┬───────────────┘
                            ▼
┌─ Maintenance Executor（编排器·执行）───────┐
│  nudge:   SendMessage(纠正) →              │
│           WaitForTurnOutcome（新 API）→     │
│           节点结果 = 纠正 turn 输出          │
│  restart: Stop runtime / kill 子进程 →      │
│           重跑节点（新 attempt，输入带      │
│           correction）                     │
│  noop:    只是慢 → 解除挂起继续等           │
└────────────────────────────────────────────┘
```

## 3. 执行器能力分档（设计审计修正 ①）

| 档 | 执行器/模式 | 卡住检测观测源 | interrupt | message | 可用动作 |
|---|---|---|---|---|---|
| T1 | mimo serve / opencode serve / claude serve / codex serve | runtime 进度信号（事件流时间戳 / 输出长度） | ✓ | ✓ | nudge / restart |
| T2 | reasonix serve（默认执行器） | turn 时长预警（无增量事件） | ✗ | ✗ | 仅 restart |
| T3 | 任意 run 模式 / dsh | 进程输出 / turn 时长 | ✗ | ✗ | 仅 restart |

依据：
- interrupt/message 路由只覆盖 mimo→codex→claude→opencode
  （internal/serve/orchestrator.go:1234-1250），reasonix serve 无 /cancel API；
- reasonix SSE 事件只有 turn_done / ask_request，无输出增量
  （internal/orchestrator/pipeline.go:4120-4131）。

内置预设默认全部 serve（执行者 mimo serve、审查者 reasonix serve，
pipeline.go:2819-2867），用户测试场景（opencode serve + mimo serve 免费模型）
落在 T1，nudge 可用。

## 4. StallDetector：检测规则

检测挂起在节点执行等待期间（submitTask / Execute 的等待循环外围），
与执行解耦：goroutine 观察"进度信号"，主流程继续正常等待。

### 4.1 进度信号源（按档）

- T1：轮询 runtime 快照/事件流（复用现有 snapshot 数据，前端 console 已轮询
  1200ms）：记录"最后有进展时间" = 最后一个事件时间戳 / 输出文本长度变化 /
  lastActiveAt 更新。任一变化即视为有进展。
- T2：无增量 → 用 turn 时长：turn 进行中超过 `stall.turnWarnTimeout`（默认 3min）
  即触发。正常长推理 turn 由阈值兜住，误伤概率低于"无输出 90s"。
- T3：子进程 stdout 增量 / turn 时长，同上。

### 4.2 触发条件

任一命中即触发维护流程：

1. T1：无新进展 ≥ `stall.noProgressTimeout`（默认 90s）；
2. T2/T3：turn 进行中 ≥ `stall.turnWarnTimeout`（默认 3min）；
3. 执行返回错误且错误类型属于"疑似卡住"（空输出、超时类错误、快速失败信号
   如 permission 死循环——现有 mimoFastFailureReason 检测保留）。

不做独立的"重复输出文本相似度"检测（审计修正：实现复杂、弱模型下判定不可靠），
循环与否由审查者从现场包中的事件摘要判断。

### 4.3 检测不误伤

- 有进展即重置计时器；nudge 后重置；
- 正常完成（turn_done / Execute 返回）→ 检测 goroutine 退出，零开销。

## 5. Maintenance Planner：审查者维护者模式

### 5.1 现场包（注入维护调用 prompt）

```
节点: nodeId / label / type
执行器: executor / model / mode / runtimeId
任务: 本轮 task 文本
已输出摘要: 截断 2000 字（最后一段输出）
事件摘要: 最近 ≤10 条事件（时间戳 + kind，截断 500 字）
错误: 若有（err.Error()）
轮次: iteration N
```

### 5.2 maintenance-plan-v1 schema

```json
{
  "schemaVersion": "maintenance-plan-v1",
  "judgment": "nudge | restart | noop",
  "reason": "中文一句话诊断（<200 字）",
  "nudge": { "message": "给执行者的纠正消息（<500 字）" },
  "restart": { "correction": "重跑输入要注入的纠正说明（<500 字）" }
}
```

校验规则（复用 ParseReviewDecision 的"扫描最后一个完整 JSON 对象"解析策略）：
- schemaVersion 必须为 maintenance-plan-v1；
- judgment ∈ {nudge, restart, noop}；
- nudge 时 message 必填非空；restart 时 correction 必填非空；noop 时其余字段忽略；
- 输出非法 → 追加一条 `[系统纠正]` 消息重试 1 次（复用现有审查者纠错重试机制，
  loop.go:873-902 同款）→ 仍失败 → **放弃维护**，节点按原超时/fail 路径处理。

### 5.3 维护调用执行规格

- 执行器/模型 = 审查者节点配置（默认 reasonix + deepseek-flash）；
- 强制 `NeverAsk=true`（防维护调用自身挂起等权限）、`MaxSteps=1`、
  `executionMode=task`（复用审查者节点执行规格，pipeline.go:3134-3189 同款）；
- 超时 `maintenance.timeout`（默认 3min）；
- **互斥**：维护调用与轮末审查共享审查者 runtime（submitTask 串行）。维护调用
  前取审查者 runtime 互斥锁（per-reviewer-runtime mutex）；拿不到（轮末审查
  进行中或 runtime 不可用）→ 放弃维护，走原路径；
- 多节点同时 stall：维护调用串行排队（同一把锁），后到的放弃（不阻塞轮末审查）。

## 6. Maintenance Executor：指令执行

### 6.1 时序（先中断再诊断，用户已确认）

1. StallDetector 命中 → 节点挂起；
2. **先 interrupt**（T1：调用执行器 interrupt API；T2/T3：无需，进程继续观察）：
   - T1 interrupt 后 Execute 返回（opencode 恢复部分输出返回；
     mimo 返回中断错误）→ 节点进入"维护中"状态（attempt 保持 running，
     不落 failed）；
3. 组装现场包 → 维护调用（§5）→ 校验；
4. 执行指令：
   - `nudge`（仅 T1）：`SendMessage(纠正)` → **`WaitForTurnOutcome`（新 API）**
     等待纠正 turn 完成（ctx 超时 = 剩余节点预算）→ 节点结果 = 纠正 turn 输出
     （原始部分输出保留在 attempt 输出历史，最终输出以纠正 turn 为准，为空则
     用部分输出）→ 重置 stall 计时 → 回到正常等待；
   - `restart`：T1 调 Stop runtime / T2 杀 reasonix serve 进程 / T3 kill 子进程树
     → `CreateAttemptWithIteration` 建新 attempt → 以原输入 + correction 注入
     重跑节点（`executeNodeWithLoopProtocolAtWorkspace` 重入，新 attempt 记录
     Input）→ 新 attempt 走正常执行+检测循环；
   - `noop`：解除挂起，重置 stall 计时，继续等待原 turn 完成；
5. 放弃路径：维护调用失败/校验失败/超时 → 节点按原路径处理（T1 中断后部分输出
   合法则 complete，否则 failed/interrupted → 现有 resume 机制人工接管）。

### 6.2 新 API：WaitForTurnOutcome（设计审计修正 ③）

执行器层同步等待通道，四个 T1 执行器各实现一份：

```
WaitForTurnOutcome(ctx, runtimeID, turnID) (output string, err error)
```

- mimo：轮询 ACP session 事件直到该 turn 完成（30min turn 由 ctx 收敛）；
- opencode：轮询事件流 message 完成事件 / snapshot 直到 canSend 恢复 idle；
- claude：SDK 事件轮询（同前）；
- codex：WaitTurn 语义复用（app_server.go:391-403 同款）。

### 6.3 干预上限

- 每节点每轮最多 `stall.maxInterventions`（默认 2）次维护干预；
- 超过 → 放弃维护，走原路径（超时 → interrupted → 人工接管）。

## 7. 兜底与降级

| 场景 | 行为 |
|---|---|
| 维护调用失败（runtime busy / 校验失败 ×2 / 超时） | 放弃维护，原路径 |
| nudge 后纠正 turn 仍卡住 | 重置计时再次触发（≤ 上限） |
| 干预达上限仍卡 | 走原超时 → interrupted → 人工（Runtime Console / resume） |
| 用户 cancel | ctx 传播取消维护调用与节点（现有机制，无新状态） |
| run 非 interrupted 状态 resume | 不受影响（维护事件仅记录） |
| 普通 pipeline（无审查者） | 不启用维护（§1 非目标） |

## 8. 前端展示（index.html）

- 节点详情卡状态徽章：`⚠ 卡住·审查者诊断中` → `已纠正·继续` / `已重跑` / `放弃·转人工`；
- 运行分析面板新增**维护事件流**：检测时间、审查者诊断 reason、动作（nudge 消息
  内容 / restart correction）、结果（恢复 / 再次卡住 / 放弃）；
- 维护事件持久化：Run JSON 新字段 `maintenanceEvents`（数组，元素：
  nodeId / iterationId / at / reason / action / outcome / detail），resume 后可见。

## 9. 配置项

| 项 | 默认 | 说明 |
|---|---|---|
| `stall.noProgressTimeout` | 90s | T1 无新进展触发 |
| `stall.turnWarnTimeout` | 3min | T2/T3 turn 时长预警 |
| `stall.maxInterventions` | 2 | 每节点每轮干预上限 |
| `maintenance.timeout` | 3min | 审查者维护调用超时 |

环境变量 `REASONIX_*` 前缀注入，沿用现有配置读取模式。

## 10. 测试计划

1. **StallDetector 单测**：T1 无进展 90s 触发 / 有进展重置 / 完成即退出 / T2 turn 时长
   预警 / nudge 后重置；
2. **maintenance-plan-v1 校验单测**：合法三判断 / 缺字段 / 双 JSON / Markdown 围栏 /
   非 maintenance schema（沿用 ParseReviewDecision 解析策略）；
3. **集成测试**（fake 执行器 + stub 审查者，参考 concurrentStubExecutor）：
   - 卡住 → nudge：断言 interrupt + SendMessage 被调、WaitForTurnOutcome 返回、
     attempt 最终输出 = 纠正 turn 输出、run 继续；
   - 卡住 → restart：断言旧 attempt 终止、新 attempt 创建（AttemptNumber=2）、
     correction 注入 input、run 继续；
   - 审查者 2 次 JSON 失败 → 放弃维护、走原超时路径；
   - 维护调用与轮末审查互斥：审查者 busy 时放弃维护；
   - 干预达上限 → 放弃转人工；
4. **回归**：现有 loop 测试全绿（正常路径零开销）；cancel 期间维护调用取消。

## 11. 改动文件预估

- `internal/orchestrator/loop.go`：stall 检测 goroutine + 维护流程状态机 + 互斥；
- `internal/orchestrator/loop_review.go`：maintenance-plan-v1 解析/校验；
- `internal/orchestrator/loop_reviewer_prompt.go`（或 pipeline.go）：维护者模式 prompt
  + 现场包组装 + 维护调用执行规格；
- `internal/orchestrator/mimo_runtime.go` / `opencode_runtime.go` /
  `claude_runtime.go` / `codex_runtime.go`：`WaitForTurnOutcome` 各一份；
- `internal/orchestrator/types.go`：Run 加 `maintenanceEvents`；
- `internal/serve/orchestrator.go`：维护事件透出（run 快照字段）；
- `internal/serve/orchestrator_frontend/index.html`：徽章 + 维护事件流渲染 + i18n；
- 新增测试文件 + 调试记录 L-53 + 文档 04 同步。