param(
    [ValidateRange(1, 65535)]
    [int]$Port = 8788,
    [switch]$NoBrowser,
    [string]$DataDir = "",
    [string]$WorkspaceDir = ""
)
& (Join-Path $PSScriptRoot 'scripts\start-orchestrator.ps1') @PSBoundParameters
exit $LASTEXITCODE
