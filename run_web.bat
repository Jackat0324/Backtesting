@echo off
cd /d "%~dp0"
echo 正在啟動策略回測系統網頁版...
streamlit run app_web.py
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo [錯誤] 程式異常結束 (Error Level: %ERRORLEVEL%)
    pause
)
