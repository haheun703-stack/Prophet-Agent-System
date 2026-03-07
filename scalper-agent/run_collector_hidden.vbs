Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "D:\Prophet_Agent_System_예언자\scalper-agent"
WshShell.Run """C:\Program Files\Python31312\pythonw.exe"" ""D:\Prophet_Agent_System_예언자\scalper-agent\data\daily_collector.py""", 0, True
