@echo off
REM kill_worker_demo.bat – Simulasi kegagalan Worker untuk demo fault tolerance
REM Mematikan proses worker.py berdasarkan Worker ID (Komponen e)
REM Penggunaan: kill_worker_demo.bat [WORKER_ID]

set WORKER_ID=%~1
if "%WORKER_ID%"=="" set WORKER_ID=w

echo [FAULT-TOLERANCE DEMO] Mematikan Worker: %WORKER_ID%...

REM Cari PID proses python yang menjalankan worker.py dengan ID ini
for /f "tokens=2" %%a in ('tasklist /fi "IMAGENAME eq python.exe" /fo list ^| findstr /i "PID"') do (
    set PID=%%a
)

REM Gunakan WMIC untuk menemukan proses yang lebih spesifik
wmic process where "name='python.exe' and commandline like '%%worker.py%%'" get processid /format:list 2>nul | findstr ProcessId

echo.
echo Untuk mematikan Worker secara manual, gunakan:
echo   taskkill /F /PID ^<PID^>
echo.
echo Atau jalankan Worker dengan --crash-after N untuk simulasi otomatis.
pause
