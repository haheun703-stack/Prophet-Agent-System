Set WshShell = CreateObject("WScript.Shell")
Set env = WshShell.Environment("Process")
env("PYTHONIOENCODING") = "utf-8"
WshShell.CurrentDirectory = "D:\Prophet_Agent_System_예언자\scalper-agent"
WshShell.Run """C:\Program Files\Python31312\pythonw.exe"" ""D:\Prophet_Agent_System_예언자\scalper-agent\collect_all.py"" --force", 0, True
