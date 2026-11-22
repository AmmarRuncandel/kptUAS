@echo off
REM start_worker.bat – Jalankan Worker Node di Windows
REM Penggunaan: start_worker.bat [PASSCODE] [WORKER_ID] [PROCS]
REM Contoh: start_worker.bat 839201 w01 4
REM Atau jalankan tanpa argumen: start_worker.bat (akan diminta memasukkan sandi di layar)

setlocal

set PASSCODE=%~1
set WORKER_ID=%~2
if "%WORKER_ID%"=="" set WORKER_ID=w-%COMPUTERNAME%

set PROCS=%~3
if "%PROCS%"=="" set PROCS=0

echo ================================================================
echo  WORKER NODE – Distributed Data Compression (Auto-Discovery)
echo ================================================================
echo  Worker ID : %WORKER_ID%
echo  Procs     : %PROCS% (0 = auto = jumlah CPU core)
if not "%PASSCODE%"=="" (
    echo  Passcode  : %PASSCODE%
)
echo ================================================================

cd /d "%~dp0.."

set CMD=python worker.py --worker-id "%WORKER_ID%"

if not "%PASSCODE%"=="" (
    set CMD=%CMD% --passcode "%PASSCODE%"
)

if not "%PROCS%"=="0" (
    set CMD=%CMD% --procs %PROCS%
)

%CMD%

pause
