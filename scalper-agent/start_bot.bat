@echo off
chcp 65001 >nul
title Body Hunter v5 - Telegram Bot
cd /d "D:\Prophet_Agent_System_예언자\scalper-agent"

echo [%date% %time%] Body Hunter v5 Bot Starting...
set PYTHONIOENCODING=utf-8

python -u bot/telegram_bot.py

echo.
echo [%date% %time%] Bot Stopped.
pause
