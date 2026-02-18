# -*- coding: utf-8 -*-
"""바탕화면 바로가기 생성 (Python + COM)"""
import os
import sys

try:
    import win32com.client
except ImportError:
    # pywin32 없으면 PowerShell 방식
    import subprocess
    desktop = subprocess.check_output(
        ['powershell', '-Command', '[Environment]::GetFolderPath("Desktop")'],
        text=True
    ).strip()

    ps_script = f'''
$WshShell = New-Object -ComObject WScript.Shell
$lnk = $WshShell.CreateShortcut("{desktop}\\Body Hunter v3.lnk")
$lnk.TargetPath = "D:\\Prophet_Agent_System_\uc608\uc5b8\uc790\\Body_Hunter_v3.bat"
$lnk.WorkingDirectory = "D:\\Prophet_Agent_System_\uc608\uc5b8\uc790\\scalper-agent"
$lnk.IconLocation = "D:\\Prophet_Agent_System_\uc608\uc5b8\uc790\\scalper-agent\\body_hunter_v3.ico,0"
$lnk.Description = "Body Hunter v3"
$lnk.Save()
'''
    # UTF-8 BOM 으로 임시 파일 작성
    tmp = os.path.join(os.environ['TEMP'], 'make_lnk.ps1')
    with open(tmp, 'w', encoding='utf-8-sig') as f:
        f.write(ps_script)

    result = subprocess.run(
        ['powershell', '-ExecutionPolicy', 'Bypass', '-File', tmp],
        capture_output=True, text=True
    )
    print(result.stdout)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    else:
        print(f"Shortcut created at: {desktop}\\Body Hunter v3.lnk")

    # 기존 .bat 삭제
    bat_path = os.path.join(desktop, "Body_Hunter_v3.bat")
    if os.path.exists(bat_path):
        os.remove(bat_path)
        print("Old .bat removed")
    sys.exit(0)

# pywin32 있는 경우
import subprocess
desktop = subprocess.check_output(
    ['powershell', '-Command', '[Environment]::GetFolderPath("Desktop")'],
    text=True
).strip()

shell = win32com.client.Dispatch("WScript.Shell")
lnk_path = os.path.join(desktop, "Body Hunter v3.lnk")
shortcut = shell.CreateShortcut(lnk_path)
shortcut.TargetPath = r"D:\Prophet_Agent_System_예언자\Body_Hunter_v3.bat"
shortcut.WorkingDirectory = r"D:\Prophet_Agent_System_예언자\scalper-agent"
shortcut.IconLocation = r"D:\Prophet_Agent_System_예언자\scalper-agent\body_hunter_v3.ico,0"
shortcut.Description = "Body Hunter v3"
shortcut.Save()
print(f"Shortcut created: {lnk_path}")

bat_path = os.path.join(desktop, "Body_Hunter_v3.bat")
if os.path.exists(bat_path):
    os.remove(bat_path)
    print("Old .bat removed")
