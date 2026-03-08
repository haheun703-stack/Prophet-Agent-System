Set WshShell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
myDir = fso.GetParentFolderName(WScript.ScriptFullName)
WshShell.CurrentDirectory = myDir
Set env = WshShell.Environment("Process")
env("PYTHONIOENCODING") = "utf-8"
WshShell.Run "pythonw.exe """ & myDir & "\data\daily_collector.py""", 0, True