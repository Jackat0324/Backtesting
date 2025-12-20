@echo off
:: 切換到 batch 檔所在的目錄
cd /d "%~dp0"

echo 正在啟動 TWSE 資料抓取器 GUI...
python reader_gui.py

if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [錯誤] 程式異常結束 (Error Level: %ERRORLEVEL%)
    echo 請確認已安裝 Python 並將其加入系統 PATH 環境變數中。
    pause
)
