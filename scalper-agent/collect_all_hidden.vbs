' collect_all_hidden.vbs - Auto Collect + Crash Recovery Wrapper
' ============================================================
' NOTE: This file MUST stay ASCII-only. Korean comments cause
'       wscript/cscript to fail under cp949 (Expected statement
'       error at line N, col 1). Do NOT add non-ASCII characters.
' ============================================================
' Uses python.exe (pythonw.exe has no console -> swallows errors)
' Redirects stdout/stderr to log for debugging
' 1st run: full collect (no --force, uses cache)
' 2nd run: --resume if checkpoint remains
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

' Ensure logs directory exists
If Not fso.FolderExists(logDir) Then fso.CreateFolder(logDir)

' Error log path (scheduler_YYYYMMDD.log)
Dim dtStr
dtStr = Year(Now) & Right("0" & Month(Now), 2) & Right("0" & Day(Now), 2)
errorLog = logDir & "\scheduler_" & dtStr & ".log"

' Chr(34) = double-quote (avoid cmd.exe nested-quote issue)
Dim Q
Q = Chr(34)

' 1st run: regular collect (cache enabled, no --force)
Dim cmd1
cmd1 = "cmd.exe /c " & pythonExe & " " & Q & scriptPath & Q & " >> " & Q & errorLog & Q & " 2>&1"
WshShell.Run cmd1, 0, True

' Crash recovery: retry with --resume if checkpoint exists
If fso.FileExists(checkpointPath) Then
    Dim cmd2
    cmd2 = "cmd.exe /c " & pythonExe & " " & Q & scriptPath & Q & " --resume >> " & Q & errorLog & Q & " 2>&1"
    WshShell.Run cmd2, 0, True
End If
