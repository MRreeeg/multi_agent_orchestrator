@echo off
setlocal
set "ERROR_LOG=%~dp0startup-error.log"
if exist "%ERROR_LOG%" del /q "%ERROR_LOG%" >nul 2>&1

powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0start.ps1" %* 2> "%ERROR_LOG%"
set "EXIT_CODE=%ERRORLEVEL%"

if "%EXIT_CODE%"=="0" (
    if exist "%ERROR_LOG%" del /q "%ERROR_LOG%" >nul 2>&1
) else (
    echo.
    echo Multi-Agent Orchestrator startup failed. Exit code: %EXIT_CODE%
    if exist "%ERROR_LOG%" (
        echo.
        echo Error details:
        type "%ERROR_LOG%"
    )
    echo.
    echo Press any key to close this window...
    pause >nul
)

exit /b %EXIT_CODE%