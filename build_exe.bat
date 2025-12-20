@echo off
echo Installing PyInstaller...
pip install pyinstaller

echo Cleaning up previous builds...
rmdir /s /q build
rmdir /s /q dist
del *.spec

echo Building Executable...
:: --onefile: Generate a single .exe file
:: --noconsole: Do not show a console window (GUI only)
:: --name: Name of the output executable
:: --hidden-import: Ensure dependencies are found (pandas/matplotlib usually handled, but adding for safety if needed)
:: Note: Data folder is external, so we don't need --add-data for it.
pyinstaller --noconsole --onefile --name TWSE_Strategy_App strategy_gui.py

echo.
echo ========================================================
echo Build Complete!
echo The executable is located in the 'dist' folder.
echo.
echo To share with your colleague:
echo 1. Copy 'dist\TWSE_Strategy_App.exe'
echo 2. Copy the 'data' folder (containing twse_data.db)
echo 3. Put them in the same folder on your colleague's PC.
echo ========================================================
pause
