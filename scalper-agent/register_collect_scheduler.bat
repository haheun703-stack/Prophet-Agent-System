@echo off
chcp 65001 >nul
echo ============================================
echo  Body Hunter 데이터 수집 스케줄러 등록
echo  (관리자 권한 필요 — 우클릭 → 관리자로 실행)
echo ============================================
echo.

echo [1/3] 기존 작업 삭제 (있으면)...
schtasks /delete /tn "BodyHunter_DailyCollect" /f 2>nul

echo [2/3] 매일 16:10 데이터 수집 작업 등록 (절전 해제 포함)...
schtasks /create /tn "BodyHunter_DailyCollect" /xml "%~dp0scheduler_task.xml" /f

if %errorlevel% neq 0 (
    echo.
    echo [!] XML 등록 실패 — 기본 등록 시도...
    schtasks /create /tn "BodyHunter_DailyCollect" /tr "D:\Prophet_Agent_System_예언자\scalper-agent\collect_all.bat" /sc daily /st 16:10 /rl HIGHEST /f
)

echo.
echo [3/3] 등록 확인:
schtasks /query /tn "BodyHunter_DailyCollect" /fo list
echo.
echo 완료! 매일 16:10에 자동 수집 (절전 해제 포함)
pause
