@echo off
REM start_master.bat – Jalankan Master Node di Windows
REM Penggunaan: start_master.bat [input_dir] [mode]
REM Contoh: start_master.bat data\sample dynamic

setlocal

set INPUT_DIR=%~1
if "%INPUT_DIR%"=="" set INPUT_DIR=data\sample

set MODE=%~2
if "%MODE%"=="" set MODE=dynamic

set OUTPUT_DIR=results\output
set PORT=9000
set ALGORITHM=lzma
set LEVEL=6
set CHUNK_SIZE=32

echo ================================================================
echo  MASTER NODE – Distributed Data Compression
echo ================================================================
echo  Input  : %INPUT_DIR%
echo  Output : %OUTPUT_DIR%
echo  Mode   : %MODE%
echo  Port   : %PORT%
echo ================================================================

cd /d "%~dp0.."
python master\master.py ^
    --input-dir "%INPUT_DIR%" ^
    --output-dir "%OUTPUT_DIR%" ^
    --host 0.0.0.0 ^
    --port %PORT% ^
    --algorithm %ALGORITHM% ^
    --level %LEVEL% ^
    --chunk-size %CHUNK_SIZE% ^
    --mode %MODE% ^
    --transfer socket ^
    --log-level INFO

pause
