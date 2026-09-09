# Registers the daily 7:30am health check for the Acumatica connectors.
# Run once, as your normal user (no admin needed):
#   powershell -ExecutionPolicy Bypass -File setup_monitor_task.ps1
# Requires .fivetran_key (gitignored) next to monitor.py — see monitor.py docstring.

$repo = Split-Path $MyInvocation.MyCommand.Path
$py = (Get-Command python).Source
$pyw = Join-Path (Split-Path $py) 'pythonw.exe'
if (-not (Test-Path $pyw)) { $pyw = $py }

$action = New-ScheduledTaskAction -Execute $pyw `
    -Argument ('"' + (Join-Path $repo 'monitor.py') + '"') `
    -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Daily -At 7:30AM
# StartWhenAvailable: if the laptop was asleep/off at 7:30, run at next logon.
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable

Register-ScheduledTask -TaskName 'BigTop Acumatica connector health check' `
    -Action $action -Trigger $trigger -Settings $settings `
    -Description 'Daily Fivetran health check for the two BigTop Acumatica connectors; pops a desktop alert on problems. Script: monitor.py in the acumatica-manufacturing-api repo.' `
    -Force | Select-Object TaskName, State

Write-Host "Done. Test it any time with: python monitor.py"
