param(
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$RunId = 'fenced-20261109-01',

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

$runningServices = @(docker compose ps --status running --services)
if ($LASTEXITCODE -ne 0 -or $runningServices.Count -ne 3) {
    throw 'All three ZooKeeper services must be running before this experiment.'
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
$env:FENCING = '1'

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
Write-Host "Started coordinator-1 (launcher PID=$($coordinator1.Id))"

Start-Sleep -Milliseconds 500
$env:COORDINATOR_ID = 'coordinator-2'
$coordinator2 = Start-Process -FilePath $python -ArgumentList 'src\coordinator.py' `
    -WorkingDirectory $projectRoot -WindowStyle Hidden -PassThru `
    -RedirectStandardOutput $coordinator2Stdout -RedirectStandardError $coordinator2Stderr
Write-Host "Started coordinator-2 (launcher PID=$($coordinator2.Id))"

Write-Host 'Waiting for coordinator-1 checkpoint and coordinator-2 standby, then pausing coordinator-1...'
& (Join-Path $projectRoot 'scripts\pause_leader_when_ready.ps1') `
    -RunId $RunId -Seconds $PauseSeconds -WaitTimeoutSeconds 90 `
    -UntilBackupComplete -MaxPauseSeconds 300

$coordinator1Log = Join-Path $runLogDir 'coordinator-1.log'
$coordinator2Log = Join-Path $runLogDir 'coordinator-2.log'
$deadline = [DateTime]::UtcNow.AddSeconds(150)
do {
    $coordinator1Text = if (Test-Path -LiteralPath $coordinator1Log) { Get-Content -LiteralPath $coordinator1Log -Raw } else { '' }
    $coordinator2Text = if (Test-Path -LiteralPath $coordinator2Log) { Get-Content -LiteralPath $coordinator2Log -Raw } else { '' }
    $backupComplete = $coordinator2Text -match 'run complete at '
    $staleWriteRejected = $coordinator1Text -match 'result write rejected .*error=BadVersionError'
    if ($backupComplete -and $staleWriteRejected) {
        break
    }
    if ([DateTime]::UtcNow -ge $deadline) {
        throw 'Timed out waiting for backup completion and stale leader BadVersionError.'
    }
    Start-Sleep -Seconds 1
} while ($true)

Write-Host 'Coordinator event summary:'
Select-String -Path $coordinator1Log, $coordinator2Log `
    -Pattern 'starting coordinator|joined election|became leader|waiting for predecessor|captured fencing token|restored_checkpoints|checkpoint restored|wrote result|result write rejected|run complete' `
    | ForEach-Object { $_.Line }

$pauseText = Get-Content -LiteralPath (Join-Path $runLogDir 'pause.log') -Raw
$suspendedMatch = [regex]::Match($pauseText, 'suspended_at=(\S+)')
$resumedMatch = [regex]::Match($pauseText, 'resumed_at=(\S+)')
$backupLeaderMatch = [regex]::Match($coordinator2Text, 'became leader epoch=2 at (\S+)')
$backupCompleteMatch = [regex]::Match($coordinator2Text, 'run complete at (\S+)')
if (-not ($suspendedMatch.Success -and $resumedMatch.Success -and $backupLeaderMatch.Success -and $backupCompleteMatch.Success)) {
    throw 'Missing timestamps for suspension, failover, completion or resumption.'
}
$suspendedAt = [DateTimeOffset]::Parse($suspendedMatch.Groups[1].Value)
$resumedAt = [DateTimeOffset]::Parse($resumedMatch.Groups[1].Value)
$backupLeaderAt = [DateTimeOffset]::Parse($backupLeaderMatch.Groups[1].Value)
$backupCompleteAt = [DateTimeOffset]::Parse($backupCompleteMatch.Groups[1].Value)
if ($backupCompleteAt -ge $resumedAt) {
    throw 'The old leader resumed before the backup completed; repeat with a fresh RunId.'
}
$failoverSeconds = ($backupLeaderAt - $suspendedAt).TotalSeconds
Write-Host "Verified failover time: $failoverSeconds seconds; backup finished before leader resumed."

if ($coordinator1Text -match 'wrote result coordinator=coordinator-1') {
    throw 'Fencing failed: the resumed old leader logged a successful result write.'
}

$backupSummaryPath = Join-Path $projectRoot (Join-Path "data\results_hw4\$RunId\coordinator-2" 'summary.json')
if (-not (Test-Path -LiteralPath $backupSummaryPath)) {
    throw "Backup coordinator summary is missing: $backupSummaryPath"
}
$summary = Get-Content -LiteralPath $backupSummaryPath -Raw | ConvertFrom-Json
if ($summary.n_skills_1 -ne 1401 -or $summary.n_skills_2 -ne 20454 -or $summary.n_skills_3 -ne 144024) {
    throw 'Backup summary counts do not match the HW3 reference.'
}
Write-Host "Verified HW3 counts: k1=$($summary.n_skills_1) k2=$($summary.n_skills_2) k3=$($summary.n_skills_3)"
foreach ($k in 1..3) {
    $reference = Join-Path $projectRoot "data\reference\results_v2\skills_$k.csv"
    $output = Join-Path $projectRoot "data\results_hw4\$RunId\coordinator-2\skills_$k.csv"
    if ((Get-FileHash -LiteralPath $reference -Algorithm SHA256).Hash -ne
        (Get-FileHash -LiteralPath $output -Algorithm SHA256).Hash) {
        throw "skills_$k.csv does not match the HW3 reference."
    }
    Write-Host "Verified skills_$k.csv SHA256 against HW3 reference"
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
    if stat.version != 1 or pointer['coordinator_id'] != 'coordinator-2' or pointer['epoch'] != 2:
        raise SystemExit("Expected fenced pointer from coordinator-2 at epoch 2 and version 1")
    fence, fence_stat = client.get(f"/hw4/runs/{os.environ['RUN_ID']}/fence")
    print(f"registered_fence_epoch={fence.decode('ascii')} fence_version={fence_stat.version}")
    if fence != b'2':
        raise SystemExit("Expected registered fencing epoch 2")
finally:
    client.stop()
    client.close()
'@
$verifyPointer | & $python -
if ($LASTEXITCODE -ne 0) {
    throw 'Final ZooKeeper pointer check failed.'
}

Write-Host "Fenced run verified: $RunId"
