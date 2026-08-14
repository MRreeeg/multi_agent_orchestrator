# dsh-agent-pack 安装器
# 用法：
#   .\install.ps1 -Mode user                # 用户级：skills + persona + 客制化 agent 预设 → $DSH_HOME + Reasonix 技能根
#   .\install.ps1 -Mode user -SkillDirs "C:\Users\x\.codex\skills;C:\Users\x\.local\share\mimocode\builtin_skills\0.1.9\skills"
#                                          # 让 DSH 直接复用本机已有 agent 的 skill（不重复下载/安装）
#   .\install.ps1 -Mode project -Workspace G:\work\my-project   # 项目级：装进 <workspace>/.agents/skills
#   .\install.ps1 -Mode temp -Task "..."    # 临时：直接 --patch 跑一次 DSH，不落盘
#   .\install.ps1 -Mode temp -Task "..." -Preset reviewer   # 临时用某个客制化 agent 的 headless 补丁跑一次
#   .\install.ps1 -Mode user -SkipDsh       # 只装 Reasonix 技能根，不装 DSH
#   .\install.ps1 -Mode user -SkipPresets   # 不装客制化 agent 预设
param(
    [ValidateSet('user', 'project', 'temp')]
    [string]$Mode = 'user',
    [string]$Workspace = '',
    [string]$Task = '',
    [string]$Preset = '',
    [string]$SkillDirs = '',
    [switch]$SkipDsh,
    [switch]$SkipReasonix,
    [switch]$SkipPresets
)

$ErrorActionPreference = 'Stop'
$PackRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$SkillRoot = Join-Path $PackRoot 'skills'
$PresetRoot = Join-Path $PackRoot 'presets'

# 共用 skill 标记块：install.ps1 管理的 skill-filesystem.customSkillDirs 配置段。
$SkillBlockStart = '# >>> dsh-agent-pack: shared skill dirs (install.ps1 managed) >>>'
$SkillBlockEnd = '# <<< dsh-agent-pack: shared skill dirs <<<'

# 把 -SkillDirs 写进 $DSH_HOME/cordis.patch.yml 的托管标记块内（幂等）。
# 效果：DSH（Web 会话与 headless 节点）直接复用本机其他 agent 已下载的 skill。
function Update-SharedSkillBlock([string]$patchPath, [string]$dirs) {
    $block = $SkillBlockStart + "`n# DSH 直接复用本机其他 agent 已下载/解压的 skill（codex / mimocode / skillpack），`n# 不重复安装。改后重启 DSH 生效。用 install.ps1 -Mode user -SkillDirs 重新生成。`n- id: skill-filesystem`n  config:`n    customSkillDirs:`n"
    $block += (($dirs -split ';' | Where-Object { $_.Trim() }) | ForEach-Object { "      - '" + $_.Trim() + "'" }) -join "`n"
    $block += "`n" + $SkillBlockEnd
    $content = ''
    if (Test-Path $patchPath) { $content = Get-Content $patchPath -Raw }
    $pattern = '(?s)' + [regex]::Escape($SkillBlockStart) + '.*?' + [regex]::Escape($SkillBlockEnd)
    if ($content -match [regex]::Escape($SkillBlockStart)) {
        $content = [regex]::Replace($content, $pattern, $block)
    } else {
        $content = $content.TrimEnd() + "`n`n" + $block + "`n"
    }
    Set-Content -Path $patchPath -Value $content -Encoding UTF8
    Write-Host "[+] 共用 skill 目录 -> $patchPath"
}

function Copy-SkillsTo([string]$destRoot) {
    if (-not $destRoot) { return }
    New-Item -ItemType Directory -Force -Path $destRoot | Out-Null
    Get-ChildItem -Directory $SkillRoot | ForEach-Object {
        Copy-Item -Recurse -Force $_.FullName (Join-Path $destRoot $_.Name)
        Write-Host "[+] skill: $($_.Name) -> $destRoot"
    }
}

# 把 pack 里的客制化 agent 预设装进 $DSH_HOME/.agent-presets/<id>/。
# DSH Web 会话直接选用该目录；Reasonix dsh 节点通过 <id>/headless.patch.yml 使用。
function Copy-PresetsTo([string]$dshHome) {
    if (-not (Test-Path $PresetRoot)) { return }
    $dest = Join-Path $dshHome '.agent-presets'
    New-Item -ItemType Directory -Force -Path $dest | Out-Null
    Get-ChildItem -Directory $PresetRoot | ForEach-Object {
        $target = Join-Path $dest $_.Name
        Copy-Item -Recurse -Force $_.FullName $target
        Write-Host "[+] 客制化 agent 预设: $($_.Name) -> $target"
    }
    Write-Host "[+] Reasonix 自检 /selfcheck 会自动导入上述预设；dsh 节点配置面板可选。"
}

switch ($Mode) {
    'user' {
        $dshHome = $env:DSH_HOME
        if (-not $dshHome) { $dshHome = Join-Path $HOME '.dsh' }
        if (-not $SkipDsh) {
            Copy-SkillsTo (Join-Path $dshHome 'skills')
            if (-not $SkipPresets) {
                Copy-PresetsTo $dshHome
            }
            # persona 覆盖层（合并追加，不覆盖已有 home 层）
            $homePatch = Join-Path $dshHome 'cordis.patch.yml'
            $packPatch = Join-Path $PackRoot 'cordis.patch.yml'
            if (Test-Path $packPatch) {
                $content = Get-Content $packPatch -Raw
                if (Test-Path $homePatch) {
                    Add-Content -Path $homePatch -Value "`n$content"
                } else {
                    Copy-Item $packPatch $homePatch
                }
                Write-Host "[+] persona overlay -> $homePatch"
            }
            if ($SkillDirs) {
                Update-SharedSkillBlock $homePatch $SkillDirs
            }
            Write-Host "[+] DSH home: $dshHome"
        }
        if (-not $SkipReasonix) {
            $reasonixSkills = $env:REASONIX_SKILL_DIR
            if (-not $reasonixSkills) { $reasonixSkills = Join-Path $HOME '.config\reasonix\skills' }
            Copy-SkillsTo $reasonixSkills
            Write-Host "[+] Reasonix skill root: $reasonixSkills"
            Write-Host "    提示：可用 `$env:REASONIX_SKILL_DIR 指向其他位置"
        }
        Write-Host "`n完成。验证： dsh --profile headless ""列出已加载的 skill"""
    }
    'project' {
        if (-not $Workspace) { throw '-Mode project 需要 -Workspace <目录>' }
        $agentsSkills = Join-Path $Workspace '.agents\skills'
        Copy-SkillsTo $agentsSkills
        Write-Host "[+] 项目级技能根: $agentsSkills（该工作区内的 DSH 会话自动发现）"
        Write-Host "提示：客制化 agent 预设是用户级（`$DSH_HOME/.agent-presets），项目级不复制；用 -Mode user 安装。"
        if (-not $SkipReasonix) {
            Write-Host "提示：Reasonix 技能根是全局的，项目级默认只装 DSH 侧。用 -Mode user -SkipDsh 装 Reasonix 侧。"
        }
    }
    'temp' {
        if (-not $Task) { throw '-Mode temp 需要 -Task "<任务>"' }
        $cmd = 'dsh --profile headless'
        if ($Preset) {
            $presetPatch = Join-Path $PresetRoot "$Preset\headless.patch.yml"
            if (-not (Test-Path $presetPatch)) { throw "客制化 agent 预设不存在或无 headless 补丁: $Preset" }
            $cmd += " --patch `"$presetPatch`""
        } else {
            $patch = Join-Path $PackRoot 'cordis.patch.yml'
            if (Test-Path $patch) { $cmd += " --patch `"$patch`"" }
        }
        $cmd += " `"$Task`""
        Write-Host "[run] $cmd"
        Invoke-Expression $cmd
    }
}
