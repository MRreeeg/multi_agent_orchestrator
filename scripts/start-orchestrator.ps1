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
        if ($oldProc.ProcessName -ne 'reasonix') { continue }
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
$reasonixBin = Join-Path $runtimeBinDir "reasonix.exe"
New-Item -ItemType Directory -Path $runtimeBinDir -Force | Out-Null

Write-Host "编译当前源码..." -ForegroundColor Yellow
Push-Location $RepoRoot
try {
    & go build -o $reasonixBin ./cmd/reasonix
    if ($LASTEXITCODE -ne 0) {
        throw "go build 失败，退出码 $LASTEXITCODE"
    }
} finally {
    Pop-Location
}

Write-Host "启动服务..." -ForegroundColor Green
$proc = Start-Process -FilePath $reasonixBin `
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
        throw "服务启动失败，reasonix 已退出（PID $($proc.Id)）。请直接运行 go run ./cmd/reasonix serve --addr $Addr --auth none 查看错误。"
    }
    Write-Warning "服务仍在启动中，请稍后访问 $OrchestratorUrl"
} else {
    Write-Host "服务已就绪 (PID $($proc.Id))" -ForegroundColor Green
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



