@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo 正在啟動 TWSE 資料抓取器 GUI...
python reader_gui.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [錯誤] 程式異常結束 (Error Level: %ERRORLEVEL%)
    pause
)
