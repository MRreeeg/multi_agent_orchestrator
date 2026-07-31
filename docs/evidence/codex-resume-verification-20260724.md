# Codex exec resume 验证 — 2026-07-24

## 第一次执行

```powershell
codex exec --json "只输出 FIRST_CONTEXT_MARKER"
```

输出:
```json
{"type":"thread.started","thread_id":"019f91df-7cea-7572-800e-d9e82b2dad7c"}
{"type":"turn.started"}
{"type":"item.completed","item":{"type":"agent_message","text":"FIRST_CONTEXT_MARKER"}}
{"type":"turn.completed","usage":{"input_tokens":10394,"output_tokens":20}}
```

thread_id: `019f91df-7cea-7572-800e-d9e82b2dad7c`

## 第二次执行 (resume)

```powershell
codex exec resume 019f91df-7cea-7572-800e-d9e82b2dad7c --json "只输出 SECOND_CONTEXT_MARKER"
```

输出:
```json
{"type":"thread.started","thread_id":"019f91df-7cea-7572-800e-d9e82b2dad7c"}
{"type":"turn.started"}
{"type":"item.completed","item":{"type":"agent_message","text":"SECOND_CONTEXT_MARKER"}}
{"type":"turn.completed","usage":{"input_tokens":20808,"output_tokens":28}}
```

## 验证结论

| 检查项 | 结果 |
|--------|------|
| 第二次 thread_id 与第一次一致 | ✅ 相同: 019f91df-... |
| 第二次产生 agent_message | ✅ "SECOND_CONTEXT_MARKER" |
| 没有参数解析错误 | ✅ |
| input_tokens 翻倍 (上下文保留) | ✅ 10394 → 20808 |
| 第二次无 error event | ✅ |
| 退出码 0 | ✅ |

## 参数构造验证

第一次: `codex exec --json <prompt>`
第二次: `codex exec resume <thread_id> --json <prompt>`

参数顺序正确，与 CLI help 一致:
```
Usage: codex exec resume [OPTIONS] [SESSION_ID] [PROMPT]
```
