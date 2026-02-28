@echo off
echo Running full data refresh...
cd /d "%~dp0"
call venv\Scripts\activate.bat
python scheduler\manual_refresh.py --all
echo.
echo Refresh complete!
pause
