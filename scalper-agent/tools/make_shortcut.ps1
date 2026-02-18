$desktop = [Environment]::GetFolderPath('Desktop')
$WshShell = New-Object -ComObject WScript.Shell
$lnk = $WshShell.CreateShortcut("$desktop\Body Hunter v3.lnk")
$lnk.TargetPath = 'D:\Prophet_Agent_System_예언자\Body_Hunter_v3.bat'
$lnk.WorkingDirectory = 'D:\Prophet_Agent_System_예언자\scalper-agent'
$lnk.IconLocation = 'D:\Prophet_Agent_System_예언자\scalper-agent\body_hunter_v3.ico,0'
$lnk.Description = 'Body Hunter v3'
$lnk.Save()
Write-Host "Shortcut created at: $desktop\Body Hunter v3.lnk"

Remove-Item "$desktop\Body_Hunter_v3.bat" -Force -ErrorAction SilentlyContinue
Write-Host "Old .bat removed from desktop"
