Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "D:\Prophet_Agent_System_예언자\scalper-agent"
WshShell.Run "python run_bot.py", 0, False
