@echo off
chcp 65001 >nul
echo 분봉 수집기 작업 스케줄러 등록...
schtasks /create /tn "Prophet_분봉수집기" /tr "D:\Prophet_Agent_System_예언자\scalper-agent\run_collector.bat" /sc daily /st 16:00 /f
echo.
echo 등록 완료! 매일 16:00에 자동 실행됩니다.
echo 확인: schtasks /query /tn "Prophet_분봉수집기"
pause
