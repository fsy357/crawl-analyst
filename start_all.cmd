@echo off
rem ============================================================
rem  crawl_analyst launcher  (ASCII + CRLF; keep it that way)
rem  Portable: everything is resolved from this script's own folder.
rem  Order: Redis(6380) -> Celery worker -> FastAPI(8000)
rem
rem  Optional overrides (set before running):
rem    REDIS_HOME        folder containing redis-server.exe
rem    REDIS_PASSWORD    must match REDIS_URL in .env (default: changeme)
rem ============================================================

set "APP_HOME=%~dp0"
if "%APP_HOME:~-1%"=="\" set "APP_HOME=%APP_HOME:~0,-1%"
set "VENV=%APP_HOME%\venv\Scripts"
if not defined REDIS_PASSWORD set "REDIS_PASSWORD=changeme"

rem --- use local copies if present (keeps everything on one drive)
if exist "%APP_HOME%\playwright_browsers" set "PLAYWRIGHT_BROWSERS_PATH=%APP_HOME%\playwright_browsers"
if exist "%APP_HOME%\crawl4ai_home" set "CRAWL4_AI_BASE_DIRECTORY=%APP_HOME%\crawl4ai_home"
set "CRAWL4AI_NO_CLOUD_NOTICE=1"
set "PYTHONIOENCODING=utf-8"

if not exist "%VENV%\uvicorn.exe" (
  echo [ERROR] venv not found: %VENV%
  echo         create it first:
  echo           python -m venv "%APP_HOME%\venv"
  echo           "%VENV%\pip install -r requirements.txt"
  echo           "%VENV%\playwright install chromium"
  pause
  exit /b 1
)

rem --- locate Redis: REDIS_HOME env > .\redis > D:\Redis8 > PATH
if defined REDIS_HOME goto :redis_ready
if exist "%APP_HOME%\redis\redis-server.exe" set "REDIS_HOME=%APP_HOME%\redis" & goto :redis_ready
if exist "D:\Redis8\redis-server.exe" set "REDIS_HOME=D:\Redis8" & goto :redis_ready
for %%I in (redis-server.exe) do if not "%%~$PATH:I"=="" set "REDIS_HOME=%%~dp$PATH:I"

:redis_ready
echo [1/3] Redis on port 6380
if not defined REDIS_HOME (
  echo [WARN] redis-server.exe not found.
  echo        install Redis 6+ and set REDIS_HOME, or start it yourself:
  echo          redis-server --port 6380 --requirepass %REDIS_PASSWORD%
  echo        ^(password must match REDIS_URL in .env^)
) else (
  pushd "%REDIS_HOME%"
  if exist "redis-6380.conf" (
    start "Redis-6380" /min "%REDIS_HOME%\redis-server.exe" "redis-6380.conf"
  ) else (
    start "Redis-6380" /min "%REDIS_HOME%\redis-server.exe" --port 6380 --requirepass %REDIS_PASSWORD%
  )
  popd
  ping -n 3 127.0.0.1 >nul
)

echo [2/3] Celery worker
start "Celery-worker" /D "%APP_HOME%" "%SystemRoot%\System32\cmd.exe" /k ""%VENV%\celery.exe" -A src.celery_worker worker --pool=solo --loglevel=info"
ping -n 4 127.0.0.1 >nul

echo [3/3] FastAPI on port 8000
start "FastAPI-8000" /D "%APP_HOME%" "%SystemRoot%\System32\cmd.exe" /k ""%VENV%\uvicorn.exe" src.main:app --host 127.0.0.1 --port 8000"

ping -n 6 127.0.0.1 >nul
start "" "http://127.0.0.1:8000/"

echo.
echo Started. Keep the 3 windows open; close them with stop_all.cmd.
echo Health: http://127.0.0.1:8000/api/health
echo Login: see PUBLIC_USER / PUBLIC_PASS in .env  ^(PUBLIC_AUTH=0 disables it^)
echo.
pause
