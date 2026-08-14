# dsh-agent-pack 安装器
# 用法：
#   .\install.ps1 -Mode user                # 用户级：装进 $DSH_HOME + ~/.config/reasonix/skills
#   .\install.ps1 -Mode project -Workspace G:\work\my-project   # 项目级：装进 <workspace>/.agents/skills
#   .\install.ps1 -Mode temp -Task "..."    # 临时：直接 --patch 跑一次 DSH，不落盘
#   .\install.ps1 -Mode user -SkipDsh       # 只装 Reasonix 技能根，不装 DSH
param(
    [ValidateSet('user', 'project', 'temp')]
    [string]$Mode = 'user',
    [string]$Workspace = '',
    [string]$Task = '',
    [switch]$SkipDsh,
    [switch]$SkipReasonix
)

$ErrorActionPreference = 'Stop'
$PackRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$SkillRoot = Join-Path $PackRoot 'skills'

function Copy-SkillsTo([string]$destRoot) {
    if (-not $destRoot) { return }
    New-Item -ItemType Directory -Force -Path $destRoot | Out-Null
    Get-ChildItem -Directory $SkillRoot | ForEach-Object {
        Copy-Item -Recurse -Force $_.FullName (Join-Path $destRoot $_.Name)
        Write-Host "[+] skill: $($_.Name) -> $destRoot"
    }
}

switch ($Mode) {
    'user' {
        $dshHome = $env:DSH_HOME
        if (-not $dshHome) { $dshHome = Join-Path $HOME '.dsh' }
        if (-not $SkipDsh) {
            Copy-SkillsTo (Join-Path $dshHome 'skills')
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
            Write-Host "[+] DSH home: $dshHome"
        }
        if (-not $SkipReasonix) {
            $reasonixSkills = $env:REASONIX_SKILL_DIR
            if (-not $reasonixSkills) { $reasonixSkills = Join-Path $HOME '.config\reasonix\skills' }
            Copy-SkillsTo $reasonixSkills
            Write-Host "[+] Reasonix skill root: $reasonixSkills"
            Write-Host "    提示：可用 `$env:REASONIX_SKILL_DIR 指向其他位置"
        }
        Write-Host "`n完成。验证： dsh --profile headless \"列出已加载的 skill\""
    }
    'project' {
        if (-not $Workspace) { throw '-Mode project 需要 -Workspace <目录>' }
        $agentsSkills = Join-Path $Workspace '.agents\skills'
        Copy-SkillsTo $agentsSkills
        Write-Host "[+] 项目级技能根: $agentsSkills（该工作区内的 DSH 会话自动发现）"
        if (-not $SkipReasonix) {
            Write-Host "提示：Reasonix 技能根是全局的，项目级默认只装 DSH 侧。用 -Mode user -SkipDsh 装 Reasonix 侧。"
        }
    }
    'temp' {
        if (-not $Task) { throw '-Mode temp 需要 -Task "<任务>"' }
        $patch = Join-Path $PackRoot 'cordis.patch.yml'
        $cmd = 'dsh --profile headless'
        if (Test-Path $patch) { $cmd += " --patch `"$patch`"" }
        $cmd += " `"$Task`""
        Write-Host "[run] $cmd"
        Invoke-Expression $cmd
    }
}
