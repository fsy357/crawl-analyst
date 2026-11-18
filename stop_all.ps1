# Stop crawl_analyst services: uvicorn(8000), Redis8(6380), celery worker, cloudflared
$ErrorActionPreference = 'SilentlyContinue'
foreach ($port in @(8000, 6380)) {
    Get-NetTCPConnection -LocalPort $port -State Listen |
        Select-Object -ExpandProperty OwningProcess -Unique |
        ForEach-Object { Stop-Process -Id $_ -Force }
}
Get-CimInstance Win32_Process -Filter "Name='python.exe'" |
    Where-Object { $_.CommandLine -match 'celery' } |
    ForEach-Object { Stop-Process -Id $_.ProcessId -Force }
Get-Process cloudflared | Stop-Process -Force
Start-Sleep -Seconds 2
Write-Output "stopped."