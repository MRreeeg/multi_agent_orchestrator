# start-desktop.ps1 — 构建并启动「多智能体管家」桌面应用（WebView2）
# 双击运行，或 PowerShell: .\scripts\start-desktop.ps1
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
Set-Location $root
if (-not $env:GOPROXY) { $env:GOPROXY = "https://goproxy.cn,direct" }
# Persist orchestrator history in <repo>/.data/orchestrator, same location as
# the browser start script, so desktop and browser share the same history.
if (-not $env:REASONIX_ORCHESTRATOR_DATA_DIR) {
    $env:REASONIX_ORCHESTRATOR_DATA_DIR = Join-Path $root ".data\orchestrator"
}
if (-not (Test-Path "$root\bin")) { New-Item -ItemType Directory -Path "$root\bin" | Out-Null }
Write-Host "[start-desktop] building orchestrator-app ..." -ForegroundColor Cyan
go build -ldflags "-H=windowsgui" -o "$root\bin\orchestrator-app.exe" ./cmd/orchestrator-app
if ($LASTEXITCODE -ne 0) { Write-Host "[start-desktop] build failed" -ForegroundColor Red; exit 1 }
# reasonix.exe must live next to the app: the model subprocess calls
# (analyze/expand/understand) resolve bin\reasonix.exe relative to the exe.
Write-Host "[start-desktop] building reasonix.exe ..." -ForegroundColor Cyan
go build -o "$root\bin\reasonix.exe" ./cmd/reasonix
if ($LASTEXITCODE -ne 0) { Write-Host "[start-desktop] reasonix build failed" -ForegroundColor Red; exit 1 }
Write-Host "[start-desktop] launching 多智能体管家 ..." -ForegroundColor Green
& "$root\bin\orchestrator-app.exe"
