@echo off
title Body Hunter v4
echo ============================================
echo   Body Hunter v4 - Starting...
echo   %date% %time%
echo ============================================
echo.
cd /d "%~dp0scalper-agent"
echo   Running bot from: %cd%
python run_bot.py
echo.
echo   Bot stopped (exit code: %errorlevel%)
pause
