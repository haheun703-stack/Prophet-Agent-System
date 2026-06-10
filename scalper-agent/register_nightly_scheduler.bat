@echo off
chcp 65001 >nul
echo ============================================
echo  Nightly Observation Pipeline 자동화 등록
echo  (6/10 사장님: 18:00 통합 - fill 후 shadow forward 지연 해결)
echo  관리자 권한 불필요 - 현재 사용자 작업
echo ============================================
echo.

set "VBS=D:\Prophet_Agent_System_예언자\scalper-agent\run_nightly_hidden.vbs"

echo [1/4] 기존 작업 삭제 (있으면)...
schtasks /delete /tn "BodyHunter_NightlyPipeline" /f 2>nul

echo [2/4] 매일 18:00 통합 파이프라인 등록...
schtasks /create /tn "BodyHunter_NightlyPipeline" /tr "wscript.exe \"%VBS%\"" /sc daily /st 18:00 /f

echo [3/4] self-heal (놓친 실행 따라잡기 + 절전 해제 + 2시간 제한)...
powershell -NoProfile -Command "$t=Get-ScheduledTask -TaskName 'BodyHunter_NightlyPipeline'; $t.Settings.StartWhenAvailable=$true; $t.Settings.WakeToRun=$true; $t.Settings.ExecutionTimeLimit='PT120M'; Set-ScheduledTask -InputObject $t | Out-Null; Write-Host 'StartWhenAvailable + WakeToRun + 120M'"

echo [4/4] 충돌 방지: ShadowDaily(15:50) 비활성=18:00 흡수 / VPSSync(18:30) 비활성=VPS 구버전 덮어쓰기 방지...
schtasks /change /tn "BodyHunter_ShadowDaily" /disable 2>nul
schtasks /change /tn "BodyHunter_VPSSync" /disable 2>nul

echo.
echo === 등록 확인 ===
schtasks /query /tn "BodyHunter_NightlyPipeline" /fo list 2>nul
echo.
echo 완료! 매일 18:00 통합 (fill-step6-shadow-paper-supply-market-nationality)
echo  - 러너: tools\run_nightly_pipeline.py / 로그: logs\nightly_YYYYMMDD.log
echo  - ShadowDaily(15:50)/VPSSync(18:30) 비활성 (충돌 방지)
pause
