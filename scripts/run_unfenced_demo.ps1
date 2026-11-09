param(
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$RunId = 'unfenced-20261106-09',

    [ValidateRange(1, 300)]
    [int]$PauseSeconds = 45
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot

$runLogDir = Join-Path $projectRoot (Join-Path 'logs\hw4' $RunId)
if (Test-Path -LiteralPath $runLogDir) {
    throw "Run ID already has logs; choose a fresh RunId: $RunId"
}

$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Project Python not found: $python"
}

$rustfsId = & docker compose -f ..\ITMO-Data-Engineering\compose.yaml ps -q rustfs-home
if ($LASTEXITCODE -ne 0 -or -not $rustfsId) {
    throw 'Could not find the RustFS container through the existing HW3 Compose project.'
}
$rawEnvironment = & docker inspect $rustfsId --format '{{range .Config.Env}}{{println .}}{{end}}'
if ($LASTEXITCODE -ne 0) {
    throw 'Could not read the RustFS container configuration.'
}
$runtimeEnvironment = @{}
foreach ($entry in $rawEnvironment) {
    $pair = $entry -split '=', 2
    if ($pair.Count -eq 2) {
        $runtimeEnvironment[$pair[0]] = $pair[1]
    }
}
if (-not $runtimeEnvironment['RUSTFS_ACCESS_KEY'] -or -not $runtimeEnvironment['RUSTFS_SECRET_KEY']) {
    throw 'RustFS access key or secret was not present in the running container configuration.'
}

$env:S3_BACKEND = 'boto3'
$env:S3_ENDPOINT = 'http://127.0.0.1:9010'
$env:S3_BUCKET = 'hh-lake'
$env:VACANCIES_KEY = 'raw/vacancies.csv'
$env:S3_ACCESS_KEY = $runtimeEnvironment['RUSTFS_ACCESS_KEY']
$env:S3_SECRET_KEY = $runtimeEnvironment['RUSTFS_SECRET_KEY']
$env:ZK_HOSTS = '127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183'
$env:RUN_ID = $RunId

Write-Host 'Checking source object in RustFS...'
& $python -c "import sys; sys.path.insert(0,'src'); from common import get_s3_client, VACANCIES_KEY; info=get_s3_client().head_object(VACANCIES_KEY); print(info); assert info is not None"
if ($LASTEXITCODE -ne 0) {
    throw 'S3 input check failed; coordinators were not started.'
}

New-Item -ItemType Directory -Force -Path $runLogDir | Out-Null
$coordinator1Stdout = Join-Path $runLogDir 'coordinator-1.stdout.log'
$coordinator1Stderr = Join-Path $runLogDir 'coordinator-1.stderr.log'
$coordinator2Stdout = Join-Path $runLogDir 'coordinator-2.stdout.log'
$coordinator2Stderr = Join-Path $runLogDir 'coordinator-2.stderr.log'

$env:COORDINATOR_ID = 'coordinator-1'
$coordinator1 = Start-Process -FilePath $python -ArgumentList 'src\coordinator.py' `
    -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $coordinator1Stdout -RedirectStandardError $coordinator1Stderr
Write-Host "Started coordinator-1 PID=$($coordinator1.Id)"

Start-Sleep -Milliseconds 500
$env:COORDINATOR_ID = 'coordinator-2'
$coordinator2 = Start-Process -FilePath $python -ArgumentList 'src\coordinator.py' `
    -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $coordinator2Stdout -RedirectStandardError $coordinator2Stderr
Write-Host "Started coordinator-2 PID=$($coordinator2.Id)"

Write-Host 'Waiting for the standby and checkpoint, then suspending coordinator-1...'
& (Join-Path $projectRoot 'scripts\pause_leader_when_ready.ps1') `
    -RunId $RunId -Seconds $PauseSeconds -WaitTimeoutSeconds 90

Write-Host 'Waiting for both coordinator processes to finish...'
$coordinator1Log = Join-Path $runLogDir 'coordinator-1.stderr.log'
$coordinator2Log = Join-Path $runLogDir 'coordinator-2.stderr.log'
$deadline = [DateTime]::UtcNow.AddSeconds(120)
do {
    $coordinator1Complete = (Test-Path -LiteralPath $coordinator1Log) -and
        (Get-Content -LiteralPath $coordinator1Log -Raw) -match 'run complete at '
    $coordinator2Complete = (Test-Path -LiteralPath $coordinator2Log) -and
        (Get-Content -LiteralPath $coordinator2Log -Raw) -match 'run complete at '
    if ($coordinator1Complete -and $coordinator2Complete) {
        break
    }
    if ([DateTime]::UtcNow -ge $deadline) {
        throw 'Timed out waiting for both coordinators to log run completion.'
    }
    Start-Sleep -Seconds 1
} while ($true)

Write-Host 'Coordinator event summary:'
Select-String -Path (Join-Path $runLogDir '*.log') `
    -Pattern 'starting coordinator|joined election|became leader|waiting for predecessor|restored_checkpoints|checkpoint restored|wrote result|run complete|already complete' `
    -ErrorAction SilentlyContinue | ForEach-Object { $_.Line }

$backupSummaryPath = Join-Path $projectRoot (Join-Path "data\results_hw4\$RunId\coordinator-2" 'summary.json')
if (-not (Test-Path -LiteralPath $backupSummaryPath)) {
    throw "Backup coordinator summary is missing: $backupSummaryPath"
}
$summary = Get-Content -LiteralPath $backupSummaryPath -Raw | ConvertFrom-Json
if ($summary.n_skills_1 -ne 1401 -or $summary.n_skills_2 -ne 20454 -or $summary.n_skills_3 -ne 144024) {
    throw 'Backup summary counts do not match the HW3 reference.'
}
foreach ($k in 1..3) {
    $reference = Join-Path $projectRoot "data\reference\results_v2\skills_$k.csv"
    $output = Join-Path $projectRoot "data\results_hw4\$RunId\coordinator-2\skills_$k.csv"
    if ((Get-FileHash -LiteralPath $reference -Algorithm SHA256).Hash -ne
        (Get-FileHash -LiteralPath $output -Algorithm SHA256).Hash) {
        throw "skills_$k.csv does not match the HW3 reference."
    }
}

$verifyPointer = @'
import json, os
from kazoo.client import KazooClient

client = KazooClient(hosts=os.environ["ZK_HOSTS"], timeout=10)
client.start(timeout=10)
try:
    path = f"/hw4/runs/{os.environ['RUN_ID']}/result"
    data, stat = client.get(path)
    pointer = json.loads(data.decode("utf-8"))
    print(f"pointer_version={stat.version} coordinator_id={pointer['coordinator_id']} epoch={pointer['epoch']}")
    if stat.version != 2:
        raise SystemExit("Expected result pointer version 2")
finally:
    client.stop()
    client.close()
'@
& $python -c $verifyPointer
if ($LASTEXITCODE -ne 0) {
    throw 'Final ZooKeeper pointer check failed.'
}
Write-Host "Run verified: $RunId"
