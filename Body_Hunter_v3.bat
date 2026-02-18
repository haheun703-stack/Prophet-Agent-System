@echo off
chcp 65001 >nul
title Body Hunter v3
echo ============================================
echo   Body Hunter v3 텔레그램 봇 시작
echo   %date% %time%
echo ============================================
echo.
cd /d "D:\Prophet_Agent_System_예언자\scalper-agent"
echo   봇 실행중...
"C:\Program Files\Python31312\python.exe" run_bot.py
echo.
echo   봇이 종료되었습니다 (종료코드: %errorlevel%)
pause
