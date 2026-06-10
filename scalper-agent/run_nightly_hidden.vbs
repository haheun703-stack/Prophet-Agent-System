' run_nightly_hidden.vbs - Nightly observation pipeline (hidden launcher)
' ============================================================
' NOTE: This file MUST stay ASCII-only. Korean comments cause
'       wscript to fail under cp949. Do NOT add non-ASCII characters.
' ============================================================
' Called by Task Scheduler (daily 18:00 + StartWhenAvailable catch-up).
' Runs tools/run_nightly_pipeline.py hidden, logs to logs/nightly_YYYYMMDD.log.
' Order: fill -> step6 -> shadow(build+forward) -> paper -> supply -> market -> nationality.
' Read-only data/observation (no order / no SAJANG / bot stays OFF).
' ============================================================

Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
myDir = fso.GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = myDir

Set env = WshShell.Environment("Process")
env("PYTHONIOENCODING") = "utf-8"

pythonExe = "python.exe"
scriptPath = myDir & "\tools\run_nightly_pipeline.py"
logDir = myDir & "\logs"

If Not fso.FolderExists(logDir) Then fso.CreateFolder(logDir)

Dim dtStr
dtStr = Year(Now) & Right("0" & Month(Now), 2) & Right("0" & Day(Now), 2)
errorLog = logDir & "\nightly_" & dtStr & ".log"

Dim Q
Q = Chr(34)

Dim cmd1
cmd1 = "cmd.exe /c " & pythonExe & " " & Q & scriptPath & Q & " >> " & Q & errorLog & Q & " 2>&1"
WshShell.Run cmd1, 0, True
