@echo off
REM start_worker.bat – Jalankan Worker Node di Windows
REM Penggunaan: start_worker.bat <MASTER_HOST> [WORKER_ID] [PROCS]
REM Contoh: start_worker.bat 192.168.1.10 w01 4

setlocal

set MASTER_HOST=%~1
if "%MASTER_HOST%"=="" (
    echo ERROR: Harus menyertakan IP Master!
    echo Penggunaan: start_worker.bat ^<MASTER_HOST^> [WORKER_ID] [PROCS]
    pause & exit /b 1
)

set WORKER_ID=%~2
if "%WORKER_ID%"=="" set WORKER_ID=w-%COMPUTERNAME%

set PROCS=%~3
if "%PROCS%"=="" set PROCS=0

set MASTER_PORT=9000

echo ================================================================
echo  WORKER NODE – Distributed Data Compression
echo ================================================================
echo  Master : %MASTER_HOST%:%MASTER_PORT%
echo  Worker : %WORKER_ID%
echo  Procs  : %PROCS% (0 = auto = jumlah CPU core)
echo ================================================================

cd /d "%~dp0.."

if "%PROCS%"=="0" (
    python worker\worker.py ^
        --master-host "%MASTER_HOST%" ^
        --master-port %MASTER_PORT% ^
        --worker-id "%WORKER_ID%" ^
        --log-level INFO
) else (
    python worker\worker.py ^
        --master-host "%MASTER_HOST%" ^
        --master-port %MASTER_PORT% ^
        --worker-id "%WORKER_ID%" ^
        --procs %PROCS% ^
        --log-level INFO
)

pause
