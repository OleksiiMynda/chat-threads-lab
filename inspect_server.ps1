<#
.SYNOPSIS
  Показує PID сервера чату, порт, що він слухає, та TCP-з'єднання (netstat -ano / Get-NetTCPConnection).

.EXAMPLE
  .\inspect_server.ps1                 # знайти server.py автоматично й вивести стан один раз
  .\inspect_server.ps1 -ServerPid 1234 # вказати PID вручну
  .\inspect_server.ps1 -Watch 2        # повторювати кожні 2 с (Ctrl+C - вихід)
#>
param(
    [int]$ServerPid = 0,
    [int]$Watch = 0
)

function Find-ServerPid {
    # python із Microsoft Store запускає проміжний процес-лаунчер з тією ж командною
    # стрічкою, тому беремо той процес, який справді слухає TCP-порт.
    $candidates = Get-CimInstance Win32_Process |
        Where-Object { $_.Name -like 'python*' -and $_.CommandLine -match 'server\.py' }
    foreach ($c in $candidates) {
        if (Get-NetTCPConnection -OwningProcess $c.ProcessId -State Listen -ErrorAction SilentlyContinue) {
            return [int]$c.ProcessId
        }
    }
    return 0
}

function Show-State([int]$ProcId) {
    Write-Host ("=" * 70)
    Write-Host ("Час: {0:HH:mm:ss}   PID сервера: {1}" -f (Get-Date), $ProcId)

    # 1. Порт, який слухає сервер
    $listen = Get-NetTCPConnection -OwningProcess $ProcId -State Listen -ErrorAction SilentlyContinue
    if (-not $listen) { Write-Host "Сервер із таким PID не слухає жодного TCP-порту."; return }
    $port = ($listen | Select-Object -First 1).LocalPort
    Write-Host ("Слухає: {0}:{1}" -f ($listen | Select-Object -First 1).LocalAddress, $port)

    # 2. netstat -ano: усі рядки, де порт сервера є локальним або віддаленим
    Write-Host "`n--- netstat -ano (рядки з портом $port) ---"
    netstat -ano -p TCP | Select-String -Pattern "^\s*Proto|:$port\s"

    # 3. Те саме через PowerShell, з іменами процесів обох сторін з'єднання
    Write-Host "`n--- Get-NetTCPConnection (обидва кінці з'єднань) ---"
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -eq $port -or $_.RemotePort -eq $port } |
        Sort-Object State, LocalPort |
        Select-Object @{n='Local';e={"$($_.LocalAddress):$($_.LocalPort)"}},
                      @{n='Remote';e={"$($_.RemoteAddress):$($_.RemotePort)"}},
                      State,
                      @{n='PID';e={$_.OwningProcess}},
                      @{n='Process';e={(Get-Process -Id $_.OwningProcess -ErrorAction SilentlyContinue).ProcessName}} |
        Format-Table -AutoSize

    # 4. Зведення за станами
    Write-Host "--- кількість з'єднань за станами ---"
    Get-NetTCPConnection -ErrorAction SilentlyContinue |
        Where-Object { $_.LocalPort -eq $port -or $_.RemotePort -eq $port } |
        Group-Object State | Select-Object Name, Count | Format-Table -AutoSize
}

if ($ServerPid -eq 0) { $ServerPid = Find-ServerPid }
if ($ServerPid -eq 0) { Write-Host "Не знайдено процес server.py. Запустіть сервер або вкажіть -ServerPid."; exit 1 }

do {
    Show-State $ServerPid
    if ($Watch -gt 0) { Start-Sleep -Seconds $Watch }
} while ($Watch -gt 0)
