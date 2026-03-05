@echo off
chcp 65001 >nul
echo [1/2] 기존 작업 삭제 (있으면)...
schtasks /delete /tn "BodyHunter_DailyCollect" /f 2>nul

echo [2/2] 매일 16:10 데이터 수집 작업 등록...
schtasks /create /tn "BodyHunter_DailyCollect" /tr "D:\Prophet_Agent_System_예언자\scalper-agent\collect_all.bat" /sc daily /st 16:10 /f

echo.
echo 등록 확인:
schtasks /query /tn "BodyHunter_DailyCollect" /fo list
echo.
echo 완료! 매일 16:10에 자동 수집이 실행됩니다.
pause
