@echo off
chcp 65001 >nul
cd /d "D:\Prophet_Agent_System_예언자\scalper-agent"
"C:\Program Files\Python31312\python.exe" collect_daily.py >> logs\collect_%date:~0,4%%date:~5,2%%date:~8,2%.log 2>&1
