param(
    [Parameter(Mandatory = $true)]
    [ValidatePattern('^[A-Za-z0-9_.-]+$')]
    [string]$RunId,

    [ValidateRange(1, 300)]
    [int]$Seconds = 45,

    [ValidateRange(5, 300)]
    [int]$WaitTimeoutSeconds = 60,

    [switch]$UntilBackupComplete,

    [ValidateRange(1, 600)]
    [int]$MaxPauseSeconds = 300
)

$ErrorActionPreference = 'Stop'
$projectRoot = Split-Path -Parent $PSScriptRoot
Set-Location -LiteralPath $projectRoot

$python = Join-Path $projectRoot '.venv\Scripts\python.exe'
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) {
    throw "Project Python not found: $python"
}

$runLogDir = Join-Path $projectRoot (Join-Path 'logs\hw4' $RunId)
$leaderLog = Join-Path $runLogDir 'coordinator-1.log'
$standbyLog = Join-Path $runLogDir 'coordinator-2.log'
$pauseLog = Join-Path $runLogDir 'pause.log'
$deadline = [DateTime]::UtcNow.AddSeconds($WaitTimeoutSeconds)
$leaderProcessId = $null

Write-Host "Waiting for coordinator-1 checkpoint and coordinator-2 election wait (run=$RunId)..."
while ([DateTime]::UtcNow -lt $deadline) {
    if ((Test-Path -LiteralPath $leaderLog) -and (Test-Path -LiteralPath $standbyLog)) {
        $first = Get-Content -LiteralPath $leaderLog -Raw
        $second = Get-Content -LiteralPath $standbyLog -Raw
        $pidMatch = [regex]::Match($first, 'starting coordinator=coordinator-1 pid=(\d+)')
        $leaderReady = $first -match 'became leader epoch=' -and
                       $first -match 'checkpoint saved chunk=\d+' -and
                       $first -notmatch 'wrote result'
        $standbyReady = $second -match 'waiting for predecessor'

        if ($pidMatch.Success -and $leaderReady -and $standbyReady) {
            $candidatePid = [int]$pidMatch.Groups[1].Value
            if (Get-Process -Id $candidatePid -ErrorAction SilentlyContinue) {
                $leaderProcessId = $candidatePid
                break
            }
        }
    }
    Start-Sleep -Milliseconds 200
}

if ($null -eq $leaderProcessId) {
    throw "Could not find a running leader with a saved checkpoint and a waiting standby within $WaitTimeoutSeconds seconds. Check logs under $runLogDir and start a fresh RUN_ID."
}

if ($UntilBackupComplete -and $Seconds -gt $MaxPauseSeconds) {
    throw 'Seconds cannot exceed MaxPauseSeconds.'
}
$pauseArguments = @('src\pause_process.py', '--pid', "$leaderProcessId", '--seconds', "$Seconds")
if ($UntilBackupComplete) {
    $pauseArguments += @('--until-log', $standbyLog, '--timeout-seconds', "$MaxPauseSeconds")
    Write-Host "Pausing coordinator-1 PID $leaderProcessId until coordinator-2 finishes (at least $Seconds seconds; at most $MaxPauseSeconds seconds)."
} else {
    Write-Host "Pausing coordinator-1 PID $leaderProcessId for $Seconds seconds."
}
Write-Host "Pause timestamps are saved to $pauseLog."
& $python @pauseArguments 2>&1 | Tee-Object -FilePath $pauseLog
if ($LASTEXITCODE -ne 0) {
    throw "pause_process.py exited with code $LASTEXITCODE"
}
