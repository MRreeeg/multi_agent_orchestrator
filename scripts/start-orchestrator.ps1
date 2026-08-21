<#
.SYNOPSIS
一键启动 Multi-Agent / Orchestrator 控制台。

.DESCRIPTION
在当前独立目录内编译并启动 Reasonix Web 服务，默认：
- 监听 127.0.0.1:8788
- 使用 .data\orchestrator 保存控制台历史
- 使用当前 pack 目录作为工作区
- 启动后打开 /orchestrator

示例：
  .\scripts\start-orchestrator.ps1
  .\scripts\start-orchestrator.ps1 -Port 8789 -NoBrowser
  .\scripts\start-orchestrator.ps1 -WorkspaceDir G:\work\demo
  .\scripts\start-orchestrator.ps1 -DataDir D:\reasonix-data
#>

param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8788,
    [switch]$NoBrowser,
    [string]$DataDir = "",
    [string]$WorkspaceDir = ""
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $PSCommandPath
$RepoRoot = [IO.Path]::GetFullPath((Split-Path -Parent $ScriptDir))
$Addr = "127.0.0.1:${Port}"
$BaseUrl = "http://${Addr}"
$OrchestratorUrl = "${BaseUrl}/orchestrator"
$RuntimeBinRoot = Join-Path $RepoRoot "bin\orchestrator-runtimes"
$EntryRecordFile = Join-Path $RuntimeBinRoot "last-entry.txt"

# ── 入口二进制解析 ──
# 前端管家不一定叫 reasonix（例如 orchestrator-app）：优先使用上次启动
# 记录的入口名（写于 last-entry.txt），其次检测 bin 下最新的候选，最后回退 reasonix。
function Get-PackEntryName {
    param([string]$RecordFile)
    $recorded = ""
    if (Test-Path -LiteralPath $RecordFile) {
        $recorded = ((Get-Content -LiteralPath $RecordFile -Raw -ErrorAction SilentlyContinue) -replace '\s', '')
        if ($recorded) {
            $binPath = Join-Path $RepoRoot "bin\$recorded.exe"
            $cmdPath = Join-Path $RepoRoot "cmd\$recorded"
            if (-not ((Test-Path -LiteralPath $binPath) -or (Test-Path -LiteralPath $cmdPath -PathType Container))) {
                $recorded = ""
            }
        }
    }
    if (-not $recorded) {
        foreach ($cand in @('orchestrator-app', 'reasonix')) {
            if (Test-Path -LiteralPath (Join-Path $RepoRoot "bin\$cand.exe")) { $recorded = $cand; break }
        }
    }
    if (-not $recorded) { $recorded = 'reasonix' }
    $cmdDir = Join-Path $RepoRoot "cmd\$recorded"
    if (-not (Test-Path -LiteralPath $cmdDir -PathType Container)) {
        throw "入口 cmd\$recorded 不存在（已记录或检测到二进制 $recorded.exe，但源码目录缺失）。"
    }
    return $recorded
}
$EntryName = Get-PackEntryName -RecordFile $EntryRecordFile

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "未找到 Go。请安装 Go 1.25 或更高版本，并确保 go 已加入 PATH。"
}

if ([string]::IsNullOrWhiteSpace($DataDir)) {
    $DataDir = Join-Path $RepoRoot ".data\orchestrator"
}
$DataDir = [IO.Path]::GetFullPath($DataDir)
New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
$env:REASONIX_ORCHESTRATOR_DATA_DIR = $DataDir

if ([string]::IsNullOrWhiteSpace($WorkspaceDir)) {
    $WorkspaceDir = $RepoRoot
}
$WorkspaceDir = [IO.Path]::GetFullPath($WorkspaceDir)
if (-not (Test-Path -LiteralPath $WorkspaceDir -PathType Container)) {
    New-Item -ItemType Directory -Path $WorkspaceDir -Force | Out-Null
}

Write-Host "Multi-Agent Orchestrator" -ForegroundColor Cyan
Write-Host "  project:    $RepoRoot" -ForegroundColor Gray
Write-Host "  workspace:  $WorkspaceDir" -ForegroundColor Gray
Write-Host "  persistence:$DataDir" -ForegroundColor Gray
Write-Host "  address:    $OrchestratorUrl" -ForegroundColor Gray

# 只停止本 pack 自己启动的旧进程，避免误杀原项目或其他服务。
$repoRootPrefix = $RepoRoot.TrimEnd('\') + '\'
$oldPids = @(Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique)
foreach ($oldPid in $oldPids) {
    try {
        $oldProc = Get-Process -Id ([int]$oldPid) -ErrorAction Stop
        # 入口名不固定（reasonix / orchestrator-app 等）：只按"路径位于本
        # pack 仓库内"判断，避免误杀原项目或其他服务。
        $oldPath = $oldProc.Path
        if ([string]::IsNullOrWhiteSpace($oldPath)) { continue }
        $oldPathFull = [IO.Path]::GetFullPath($oldPath)
        if ($oldPathFull.StartsWith($repoRootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
            Write-Host "停止本 pack 的旧服务 (PID $oldPid)" -ForegroundColor Yellow
            Stop-Process -Id ([int]$oldPid) -Force -ErrorAction Stop
        }
    } catch {
        Write-Warning "无法检查/停止旧服务 PID ${oldPid}: $_"
    }
}
Start-Sleep -Milliseconds 300

$buildStamp = Get-Date -Format "yyyyMMdd-HHmmss-fff"
$runtimeBinDir = Join-Path $RuntimeBinRoot "runtime-${buildStamp}-$PID"
$entryBin = Join-Path $runtimeBinDir "$EntryName.exe"
New-Item -ItemType Directory -Path $runtimeBinDir -Force | Out-Null

# 复用已有二进制，避免每次启动都重编 60MB 副本（runtime 目录只是进程执行体，
# 重启后无用；运行时数据在 .data\orchestrator\sessions 下）。
$reuseSource = ""
$latestSrc = Get-ChildItem -Path $RepoRoot -Recurse -Filter *.go -ErrorAction SilentlyContinue |
    Where-Object { $_.FullName -notmatch '\\(bin|node_modules|\.git|\.data)\\' } |
    Sort-Object LastWriteTime -Descending | Select-Object -First 1
$staleBin = Join-Path $RepoRoot "bin\$EntryName.exe"
if (Test-Path -LiteralPath $staleBin) { $reuseSource = $staleBin }
if (-not $reuseSource) {
    $existing = @(Get-ChildItem -Path $RuntimeBinRoot -Directory -Filter "runtime-*" -ErrorAction SilentlyContinue |
        ForEach-Object { Get-ChildItem -Path $_.FullName -Filter "*.exe" -ErrorAction SilentlyContinue } |
        Where-Object { Test-Path -LiteralPath $_.FullName })
    if ($existing.Count -gt 0) {
        $reuseSource = $existing | Sort-Object LastWriteTime -Descending | Select-Object -First 1 -ExpandProperty FullName
    }
}
if ($reuseSource -and $latestSrc -and
    ((Get-Item -LiteralPath $reuseSource).LastWriteTime -ge $latestSrc.LastWriteTime)) {
    Write-Host "复用已有二进制: $reuseSource" -ForegroundColor Yellow
    Copy-Item -LiteralPath $reuseSource -Destination $entryBin -Force
} else {
    Write-Host "编译当前源码 (cmd/$EntryName)..." -ForegroundColor Yellow
    Push-Location $RepoRoot
    try {
        & go build -o $entryBin "./cmd/$EntryName"
        if ($LASTEXITCODE -ne 0) {
            throw "go build 失败，退出码 $LASTEXITCODE"
        }
    } finally {
        Pop-Location
    }
}

Write-Host "启动服务..." -ForegroundColor Green
$proc = Start-Process -FilePath $entryBin `
    -ArgumentList @("serve", "--addr", $Addr, "--auth", "none") `
    -WorkingDirectory $WorkspaceDir `
    -NoNewWindow -PassThru

$ready = $false
$foreignService = $false
for ($i = 0; $i -lt 30; $i++) {
    Start-Sleep -Milliseconds 500
    try {
        $response = Invoke-WebRequest -Uri "${BaseUrl}/status" -UseBasicParsing -TimeoutSec 2
        if ($response.StatusCode -eq 200) {
            $status = $response.Content | ConvertFrom-Json
            $reportedRoot = if ($status.workspaceRoot) { [IO.Path]::GetFullPath([string]$status.workspaceRoot) } else { "" }
            $expectedRoot = [IO.Path]::GetFullPath($RepoRoot)
            if ($reportedRoot -and $reportedRoot.Equals($expectedRoot, [StringComparison]::OrdinalIgnoreCase)) {
                $ready = $true
                break
            }
            Write-Warning "端口 $Port 已有其他 Reasonix 服务，workspaceRoot=$reportedRoot；请换端口。"
            $foreignService = $true
            break
        }
    } catch {
        if ($proc.HasExited) { break }
    }
}

if (-not $ready) {
    if ($foreignService) {
        if (-not $proc.HasExited) { Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue }
        throw "端口 $Port 被其他工作区占用，请使用 -Port 指定空闲端口。"
    }
    if ($proc.HasExited) {
        throw "服务启动失败，$EntryName 已退出（PID $($proc.Id)）。请直接运行 go run ./cmd/$EntryName serve --addr $Addr --auth none 查看错误。"
    }
    Write-Warning "服务仍在启动中，请稍后访问 $OrchestratorUrl"
} else {
    Write-Host "服务已就绪 (PID $($proc.Id))" -ForegroundColor Green
    # 记录本次使用的入口名，下次启动优先复用同一二进制（前端管家可能是
    # reasonix / orchestrator-app 等不同入口）。
    try {
        [IO.File]::WriteAllText($EntryRecordFile, $EntryName, [System.Text.UTF8Encoding]::new($false))
    } catch {
        Write-Warning "无法写入入口记录 $EntryRecordFile : $_"
    }
    try {
        $r = Invoke-WebRequest -Uri "${BaseUrl}/auto-approve-tools" -Method POST `
            -Body '{"on":true}' -ContentType "application/json" -UseBasicParsing -TimeoutSec 3
        if ($r.StatusCode -eq 204) {
            Write-Host "工具自动审批已开启" -ForegroundColor Green
        }
    } catch {
        Write-Warning "自动审批开启失败，可在页面中手动开启 YOLO：$_"
    }
}

if (-not $NoBrowser) {
    Start-Process $OrchestratorUrl
}

Write-Host "控制台: $OrchestratorUrl" -ForegroundColor Yellow
Write-Host "按 Ctrl+C 停止服务。" -ForegroundColor Gray
try {
    while (-not $proc.HasExited) {
        Start-Sleep -Seconds 1
    }
} finally {
    if (-not $proc.HasExited) {
        Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    }
}



