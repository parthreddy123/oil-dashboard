@echo off
title Scenario Engine Report Server
cd /d "%~dp0"
echo Starting Scenario Engine at http://localhost:8050 ...
python serve_report.py
pause
