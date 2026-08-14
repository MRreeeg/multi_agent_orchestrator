# DSH 接入 OpenCode 模型（模型选择器直接可选）

> 版本：2026-08-14 ｜ 适用：想让 DSH（DeepSeek Harness Web/headless）直接选用
> OpenCode 里的模型（尤其免费模型，如 `deepseek-v4-flash-free`）的人
> 关联：[[02-DSH执行器接入与配置.zh-CN.md]]、[[03-自定义Agent打包与跨电脑复用.zh-CN.md]]

---

## 1. 一句话方案

OpenCode 的模型（`opencode models` 里那些）走官方 **Zen 网关**，它提供 **OpenAI 兼容端点**：

```text
baseURL = https://opencode.ai/zen/v1
端点     POST /zen/v1/chat/completions   （另有 /responses、/messages、/models/{model}）
鉴权     Bearer <OPENCODE_API_KEY>
```

DSH 的 `llm-pi-ai` 适配器支持任意 OpenAI 兼容端点 → 在 `$DSH_HOME/settings.yaml` 加一个
`opencode-zen` provider，模型列表用 `opencode models` 的真实探测结果，DSH 的模型选择器
就出现「OpenCode Zen」路由，可直接选用。

> 为什么不用 `opencode serve`？实测它的 HTTP API 是 opencode 自有协议
> （/session、/event…），**不是** OpenAI 兼容，DSH 无法直连；Zen 网关才是
> OpenAI 兼容的官方入口。

## 2. 一键接入（推荐）

```powershell
# 1) 设置 Zen 网关密钥（获取方式见 §4）
$env:OPENCODE_API_KEY = "op-..."        # 每次会话都有效；或写进系统环境变量

# 2) 生成并写入配置（模型列表来自本机 `opencode models` 探测）
.\scripts\dsh-opencode-zen.ps1

# 3) 重启 DSH（或等 settings.yaml 热加载）→ 模型选择器出现「OpenCode Zen」路由
```

脚本幂等：配置写在 `settings.yaml` 的托管块里，重复运行只更新模型列表，不会重复追加。

## 3. 手工配置（不跑脚本）

```yaml
# $DSH_HOME/settings.yaml
llm-pi-ai:
  providers:
    opencode-zen:
      displayName: OpenCode Zen
      api: openai
      baseURL: https://opencode.ai/zen/v1
      apiKeyEnv: OPENCODE_API_KEY
      models:
        - deepseek-v4-flash-free
        - hy3-free
        - mimo-v2.5-free
        - nemotron-3-ultra-free
```

> 模型名以本机 `opencode models` 输出为准（每家电脑可能不同）。`models` 是数组、
> 整体替换——脚本每次探测都会重写它。

## 4. OPENCODE_API_KEY 从哪来

Zen 网关需要真实密钥（实测假 key 返回 401）。获取方式：

- OpenCode TUI 里执行 `/login` 或 `opencode zen` 相关登录流程后，key 会写入
  `~/.local/share/opencode/`（auth.json / opencode.db）；
- 或从 OpenCode 官方文档/控制台申请 Zen API key；
- 拿到后设置环境变量（**永远不要写进 settings.yaml 或仓库**）：
  ```powershell
  $env:OPENCODE_API_KEY = "op-..."
  ```

## 5. 验证

```powershell
# headless 临时指定 opencode 模型跑一次（patch 覆盖 agent-default-model）
@'
- id: agent-default-model
  config:
    provider: opencode-zen
    model: deepseek-v4-flash-free
'@ | Set-Content "$env:TEMP\dsh-opencode.yml" -Encoding UTF8
dsh --profile headless --patch "$env:TEMP\dsh-opencode.yml" "只回复OK"
```

预期：返回 `OK`（DeepSeek 官方直连之外的免费模型通道）。Web 侧在模型选择器选
「OpenCode Zen → deepseek-v4-flash-free」即可。

## 6. 已知边界

- 免费模型有频率/额度限制，重任务建议换付费模型或 DeepSeek 官方路由；
- Zen 网关不保证推理（reasoning）字段的透传格式，思考类请求可能降级；
- 凭据只走环境变量；`settings.yaml` 与仓库里永不出现真实 key。
