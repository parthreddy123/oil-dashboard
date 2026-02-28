@echo off
echo Starting Oil and Gas Intelligence Dashboard...
cd /d "%~dp0"
call venv\Scripts\activate.bat
venv\Scripts\streamlit.exe run dashboard\app.py --server.port 8501
pause
