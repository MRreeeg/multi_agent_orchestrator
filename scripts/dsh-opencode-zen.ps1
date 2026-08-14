# dsh-opencode-zen.ps1 —— 把 OpenCode 的模型接入 DSH（模型选择器可选用）
#
# 原理：OpenCode 的免费/付费模型走官方 Zen 网关（OpenAI 兼容）：
#   baseURL = https://opencode.ai/zen/v1 ，端点 /zen/v1/chat/completions
# 本脚本把 llm-pi-ai 的 opencode-zen provider 写进 $DSH_HOME/settings.yaml
# （幂等托管块），模型列表来自本机 `opencode models` 的真实探测结果。
#
# 用法：
#   .\scripts\dsh-opencode-zen.ps1                    # 生成配置（模型来自探测）
#   $env:OPENCODE_API_KEY = "..." ; .\scripts\dsh-opencode-zen.ps1 -Check
#
# 前置：OPENCODE_API_KEY（Zen 网关密钥）。获取方式见
# docs/deepseek-harness/05-DSH接入opencode模型.zh-CN.md。
param(
    [switch]$Check        # 只检查配置与凭据，不写文件
)

$ErrorActionPreference = 'Stop'
$dshHome = $env:DSH_HOME
if (-not $dshHome) { $dshHome = Join-Path $HOME '.dsh' }
$settingsPath = Join-Path $dshHome 'settings.yaml'
$markerStart = '# >>> dsh-agent-pack: opencode-zen (install.ps1 managed) >>>'
$markerEnd = '# <<< dsh-agent-pack: opencode-zen <<<'

Write-Host "DSH home: $dshHome"

# 1) 探测 opencode 模型列表
$models = @(& opencode models 2>$null | Where-Object { $_ -and $_ -notmatch 'Active code page' } | ForEach-Object { $_.Trim() } | Sort-Object -Unique)
if (-not $models) {
    throw '`opencode models` 没有输出。请先安装 opencode CLI 并确认其可用。'
}
Write-Host "[+] 探测到 opencode 模型 $($models.Count) 个："
$models | ForEach-Object { Write-Host "    $_" }

# 2) 凭据检查
$hasKey = [bool]([string]$env:OPENCODE_API_KEY)
if (-not $hasKey) {
    Write-Host '[!] 未设置 OPENCODE_API_KEY（Zen 网关密钥）。配置会就位，但模型调用会 401。'
    Write-Host '    获取方式：docs/deepseek-harness/05-DSH接入opencode模型.zh-CN.md'
}

if ($Check) {
    Write-Host "[check] settings.yaml: $settingsPath"
    if (Test-Path $settingsPath) {
        $content = Get-Content $settingsPath -Raw
        Write-Host "[check] opencode-zen 段已存在: $($content.Contains('opencode-zen'))"
    } else {
        Write-Host '[check] settings.yaml 不存在'
    }
    exit 0
}

# 3) 生成 llm-pi-ai providers 段（YAML）
$modelLines = ($models | ForEach-Object { "        - '$_'" }) -join "`n"
$block = @"
$markerStart
# OpenCode Zen 网关（OpenAI 兼容）：https://opencode.ai/zen/v1
# 模型列表由 scripts/dsh-opencode-zen.ps1 按本机 `opencode models` 生成。
llm-pi-ai:
  providers:
    opencode-zen:
      displayName: OpenCode Zen
      api: openai
      baseURL: https://opencode.ai/zen/v1
      apiKeyEnv: OPENCODE_API_KEY
      models:
$modelLines
$markerEnd
"@

# 4) 幂等写入 settings.yaml（替换托管块或追加）
if (-not (Test-Path $settingsPath)) {
    Set-Content -Path $settingsPath -Value $block -Encoding UTF8
    Write-Host "[+] 已创建 $settingsPath"
} else {
    $content = Get-Content $settingsPath -Raw
    $pattern = '(?s)' + [regex]::Escape($markerStart) + '.*?' + [regex]::Escape($markerEnd)
    if ($content -match [regex]::Escape($markerStart)) {
        $content = [regex]::Replace($content, $pattern, $block)
    } else {
        $content = $content.TrimEnd() + "`n`n" + $block
    }
    Set-Content -Path $settingsPath -Value $content -Encoding UTF8
    Write-Host "[+] 已更新 $settingsPath（托管块）"
}

Write-Host "`n完成。验证："
Write-Host "  1) 重启 DSH（或等 settings 热加载）→ 模型选择器出现「OpenCode Zen」路由"
Write-Host '  2) $env:OPENCODE_API_KEY="sk-..." 后：'
Write-Host "     dsh --profile headless --patch 临时补丁 模型名 '只回复OK'"
Write-Host '     临时补丁内容：- id: agent-default-model / config: {provider: opencode-zen, model: <模型>}'
