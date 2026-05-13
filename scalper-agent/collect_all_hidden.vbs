' collect_all_hidden.vbs - 자동 수집 + 크래시 자동복구 래퍼
' ============================================================
' python.exe 사용 (pythonw.exe는 콘솔 없어서 에러 삼킴)
' stdout/stderr를 로그로 리다이렉트하여 디버깅 가능
' 1차: 전체 수집 실행 (force 없이 - 캐시 활용)
' 2차: 체크포인트 잔존 시 --resume 으로 자동 재개 (최대 1회)
' ============================================================

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
myDir = fso.GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = myDir

Set env = WshShell.Environment("Process")
env("PYTHONIOENCODING") = "utf-8"

pythonExe = "python.exe"
scriptPath = myDir & "\collect_all.py"
checkpointPath = myDir & "\data_store\_collect_checkpoint.json"
logDir = myDir & "\logs"

' logs 디렉토리 보장
If Not fso.FolderExists(logDir) Then fso.CreateFolder(logDir)

' 에러 로그 경로 (scheduler_YYYYMMDD.log)
Dim dtStr
dtStr = Year(Now) & Right("0" & Month(Now), 2) & Right("0" & Day(Now), 2)
errorLog = logDir & "\scheduler_" & dtStr & ".log"

' Chr(34) = 쌍따옴표 — cmd.exe 인용부호 중첩 문제 방지
Dim Q
Q = Chr(34)

' 1차 실행: 일반 수집 (캐시 활용, force 불필요)
Dim cmd1
cmd1 = "cmd.exe /c " & pythonExe & " " & Q & scriptPath & Q & " >> " & Q & errorLog & Q & " 2>&1"
WshShell.Run cmd1, 0, True

' 크래시 복구: 체크포인트가 남아있으면 --resume 으로 재시도
If fso.FileExists(checkpointPath) Then
    Dim cmd2
    cmd2 = "cmd.exe /c " & pythonExe & " " & Q & scriptPath & Q & " --resume >> " & Q & errorLog & Q & " 2>&1"
    WshShell.Run cmd2, 0, True
End If
