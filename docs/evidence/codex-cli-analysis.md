# Codex CLI Analysis — P4-B-0

## 基本信息

- **路径**: C:\Users\Administrator\AppData\Roaming\npm\codex.ps1
- **版本**: codex-cli 0.145.0
- **安装方式**: npm global

## 子命令

| 命令 | 用途 | 可用 |
|------|------|------|
| `codex exec` | 非交互式执行 (替代 `run`) | ✅ |
| `codex` (无子命令) | 交互式 TUI | ✅ |
| `codex review` | 代码审查 | ✅ |
| `codex resume` | 恢复会话 | ✅ |
| `codex login/logout` | 认证管理 | ✅ |
| `codex mcp` | MCP 服务器管理 | ✅ |
| `codex exec-server` | 独立 exec 服务 (实验) | ✅ |
| `codex serve` | **不存在** | ❌ |

## 关键参数 (codex exec)

| 参数 | 说明 | 我们之前假设 |
|------|------|-------------|
| `-m, --model <MODEL>` | 模型选择 | ✅ 正确 |
| `-C, --cd <DIR>` | 工作目录 | ✅ 正确 (`--workspace`) |
| `-s, --sandbox <MODE>` | 沙箱策略 | 未假设 |
| `--dangerously-bypass-approvals-and-sandbox` | 跳过审批+沙箱 | `--trust` + `--never_ask` |
| `--json` | JSONL 输出 | 未假设 |
| `-o, --output-last-message <FILE>` | 输出到文件 | 未假设 |
| `--ephemeral` | 不保存会话 | 未假设 |
| `--skip-git-repo-check` | 允许非 Git 目录 | 未假设 |

## 不存在的参数

| 假设参数 | 实际 |
|----------|------|
| `--skill` | ❌ 不存在 |
| `--agent` | ❌ 不存在 |
| `--trust` | ❌ 用 `--dangerously-bypass-approvals-and-sandbox` |
| `--never_ask` | ❌ 用 `-a never` 或 `--dangerously-bypass-approvals-and-sandbox` |
| `--port` | ❌ 不存在 (无 serve 模式) |
| `run` 子命令 | ❌ 用 `exec` |

## 输出格式

- 默认: TUI 渲染 (不适合管道)
- `--json`: JSONL 事件流
- `-o <file>`: 最后消息写入文件

## 结论

1. **`codex exec` 是非交互式入口** — 替代我们假设的 `codex run`
2. **没有 `serve` 模式** — Codex 不提供长期运行的 HTTP 服务
3. **审批绕过**: `--dangerously-bypass-approvals-and-sandbox` 替代 `--trust` + `--never_ask`
4. **JSONL 输出**: `--json` 可用于解析结构化结果
5. **`--skill` 和 `--agent` 不存在** — 这些是 Reasonix 概念，不是 Codex 概念
