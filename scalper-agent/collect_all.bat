@echo off
chcp 65001 >nul
cd /d "D:\Prophet_Agent_System_예언자\scalper-agent"
set PYTHONIOENCODING=utf-8
"C:\Program Files\Python31312\python.exe" collect_all.py --force
