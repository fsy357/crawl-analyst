@echo off
rem Stop all crawl_analyst services (uvicorn 8000 / Redis 6380 / celery / cloudflared)

"%SystemRoot%\System32\WindowsPowerShell\v1.0\powershell.exe" -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop_all.ps1"

echo.
echo [check] port 8000 / 6380 listening (empty = closed):
netstat -ano | findstr ":8000" | findstr "LISTENING"
netstat -ano | findstr ":6380" | findstr "LISTENING"
echo Done.
pause