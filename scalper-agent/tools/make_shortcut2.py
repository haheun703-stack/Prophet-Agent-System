# -*- coding: utf-8 -*-
"""바탕화면 바로가기 생성 — VBS 방식"""
import subprocess, os, tempfile

# 바탕화면 경로 (cp949)
raw = subprocess.check_output(
    ['powershell', '-Command', '[Environment]::GetFolderPath("Desktop")'],
    encoding='cp949'
).strip()
desktop = raw
print(f"Desktop: {desktop}")

bat_path = r"D:\Prophet_Agent_System_예언자\Body_Hunter_v3.bat"
ico_path = r"D:\Prophet_Agent_System_예언자\scalper-agent\body_hunter_v3.ico"
work_dir = r"D:\Prophet_Agent_System_예언자\scalper-agent"
lnk_path = os.path.join(desktop, "Body Hunter v3.lnk")

# VBS 스크립트로 바로가기 생성
vbs = f'''
Set WshShell = CreateObject("WScript.Shell")
Set oLink = WshShell.CreateShortcut("{lnk_path}")
oLink.TargetPath = "{bat_path}"
oLink.WorkingDirectory = "{work_dir}"
oLink.IconLocation = "{ico_path}, 0"
oLink.Description = "Body Hunter v3"
oLink.Save
'''

vbs_file = os.path.join(tempfile.gettempdir(), "make_lnk.vbs")
with open(vbs_file, "w", encoding="cp949") as f:
    f.write(vbs)

result = subprocess.run(["cscript", "//nologo", vbs_file], capture_output=True, text=True, encoding="cp949")
if result.returncode == 0:
    print(f"Shortcut created: {lnk_path}")
else:
    print(f"Error: {result.stderr}")

# 기존 .bat 제거
old_bat = os.path.join(desktop, "Body_Hunter_v3.bat")
if os.path.exists(old_bat):
    os.remove(old_bat)
    print("Old .bat removed")

# 확인
print(f"LNK exists: {os.path.exists(lnk_path)}")
