@echo off
chcp 65001 >nul
echo ============================================
echo  Sector Reversal Shadow 자동 적재 스케줄러 등록
echo  (6/9 사장님: 스케줄표에 맞게 자동 적재)
echo  관리자 권한 불필요 — 현재 사용자 작업
echo ============================================
echo.

set "VBS=D:\Prophet_Agent_System_예언자\scalper-agent\run_shadow_hidden.vbs"

echo [1/3] 기존 작업 삭제 (있으면)...
schtasks /delete /tn "BodyHunter_ShadowDaily" /f 2>nul

echo [2/3] 매일 15:50 적재 작업 등록...
schtasks /create /tn "BodyHunter_ShadowDaily" /tr "wscript.exe \"%VBS%\"" /sc daily /st 15:50 /f

echo [3/3] self-heal 설정 (놓친 실행 따라잡기 + 절전 해제)...
powershell -NoProfile -Command "$t=Get-ScheduledTask -TaskName 'BodyHunter_ShadowDaily'; $t.Settings.StartWhenAvailable=$true; $t.Settings.WakeToRun=$true; $t.Settings.ExecutionTimeLimit='PT30M'; Set-ScheduledTask -InputObject $t | Out-Null; Write-Host 'StartWhenAvailable + WakeToRun 적용'"

echo.
echo === 등록 확인 ===
schtasks /query /tn "BodyHunter_ShadowDaily" /fo list 2>nul
echo.
echo 완료! 매일 15:50 자동 적재 (꺼져 있었으면 켤 때 따라잡기, 봇 OFF 유지, 관측 전용)
echo  - 러너: tools\run_sector_reversal_shadow_daily.py (6/9 시작일~오늘 미기록 거래일만 채움)
echo  - 로그: logs\shadow_YYYYMMDD.log
pause
