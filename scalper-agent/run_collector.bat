@echo off
chcp 65001 >nul
cd /d "D:\Prophet_Agent_System_예언자\scalper-agent"
"C:\Program Files\Python31312\python.exe" data\daily_collector.py >> data_store\collector_output.log 2>&1
