@echo off
rem ============================================================
rem  Crawl Analyst - temporary public demo link (free, no account)
rem  Runs a cloudflared "quick tunnel" to the local web app.
rem  Keep this window open while demoing; the link dies when closed.
rem  NOTE: file must stay ASCII + CRLF.
rem ============================================================
setlocal
cd /d "%~dp0"

set "CF=cloudflared.exe"
where cloudflared.exe >nul 2>nul
if errorlevel 1 (
  if exist "D:\cloudflared\cloudflared.exe" (
    set "CF=D:\cloudflared\cloudflared.exe"
  ) else (
    echo [ERROR] cloudflared.exe not found.
    echo Download: https://github.com/cloudflare/cloudflared/releases
    pause
    exit /b 1
  )
)

echo Checking local service on port 8000 ...
netstat -ano | findstr ":8000" | findstr LISTENING >nul
if errorlevel 1 (
  echo [WARN] port 8000 is not listening. Start start_all.cmd first.
  pause
)

echo.
echo Starting tunnel to http://127.0.0.1:8000
echo Copy the https://xxxx.trycloudflare.com address printed below.
echo Basic auth is still enforced by .env, so only share with people you trust.
echo.
"%CF%" tunnel --url http://127.0.0.1:8000

pause
