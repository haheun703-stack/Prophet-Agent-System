' collect_all_hidden.vbs - 자동 수집 + 크래시 자동복구 래퍼
' ============================================================
' 1차: --force 로 전체 수집 실행
' 2차: 체크포인트 잔존 시 --resume 으로 자동 재개 (최대 1회)
' ============================================================

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
myDir = fso.GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = myDir

Set env = WshShell.Environment("Process")
env("PYTHONIOENCODING") = "utf-8"

pythonExe = "pythonw.exe"
scriptPath = myDir & "\collect_all.py"
checkpointPath = myDir & "\data_store\_collect_checkpoint.json"

' 1차 실행: --force
WshShell.Run pythonExe & " """ & scriptPath & """ --force", 0, True

' 크래시 복구: 체크포인트가 남아있으면 --resume 으로 재시도
If fso.FileExists(checkpointPath) Then
    WshShell.Run pythonExe & " """ & scriptPath & """ --resume", 0, True
End If
