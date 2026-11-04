# MQCluster Backend Startup Script (Windows / PowerShell)
# Solves: a terminal can inherit an old JAVA_HOME (Java 17), which makes
# `spring-boot:run` fail with UnsupportedClassVersionError (class file version 65.0).
# Usage: run from the project root  ->  .\start-backend.ps1

$ErrorActionPreference = "Stop"

function Test-Java21([string]$home) {
    if (-not $home) { return $false }
    $exe = Join-Path $home "bin\java.exe"
    if (-not (Test-Path $exe)) { return $false }
    $version = & $exe -version 2>&1 | Select-Object -First 1
    return ($version -match '"(21|2[3-9])\.')
}

# 1) Prefer an already-correct JAVA_HOME, 2) probe the usual install locations.
$javaHome = $null
if (Test-Java21 $env:JAVA_HOME) {
    $javaHome = $env:JAVA_HOME
} else {
    $candidates = @(
        (Get-ChildItem "$env:USERPROFILE\.jdks" -Directory -Filter "*21*" -ErrorAction SilentlyContinue),
        (Get-ChildItem "C:\Program Files\Microsoft" -Directory -Filter "jdk-21*" -ErrorAction SilentlyContinue),
        (Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Directory -Filter "jdk-21*" -ErrorAction SilentlyContinue),
        (Get-ChildItem "C:\Program Files\Java" -Directory -Filter "*21*" -ErrorAction SilentlyContinue)
    ) | Where-Object { $_ } | ForEach-Object { $_.FullName }

    foreach ($candidate in $candidates) {
        if (Test-Java21 $candidate) { $javaHome = $candidate; break }
    }
}

if (-not $javaHome) {
    Write-Error @"
Java 21 was not found.

Install it with one of:
  winget install Microsoft.OpenJDK.21
  https://learn.microsoft.com/java/openjdk/

Then either reopen the terminal (JAVA_HOME is picked up automatically) or set:
  `$env:JAVA_HOME = "C:\Path\To\jdk-21"
"@
    exit 1
}

$env:JAVA_HOME = $javaHome
$env:PATH = "$javaHome\bin;$env:PATH"

Write-Host "Using Java:" -ForegroundColor Cyan
& "$javaHome\bin\java.exe" -version
Write-Host ""

# Kill any process still holding the backend port.
$port = 8088
$old = Get-NetTCPConnection -LocalPort $port -State Listen -ErrorAction SilentlyContinue
if ($old) {
    Write-Host "Port $port in use, cleaning up..." -ForegroundColor Yellow
    $old | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
    Start-Sleep -Seconds 2
}

# Clean old embedded MQ stores (avoid store lock / stale KRaft metadata).
# run/rocketmq-connection.properties (UI connection config) is kept on purpose.
Set-Location "$PSScriptRoot\java"
$storeDirs = @("run\pseudo-cluster", "run\kafka-cluster", "run\rabbitmq-cluster")
foreach ($dir in $storeDirs) {
    if (Test-Path $dir) {
        Write-Host "Cleaning $dir ..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force $dir
    }
}

Write-Host "Starting backend on http://localhost:8088 ..." -ForegroundColor Green
Write-Host "Start the frontend in another terminal: cd next; npm run dev" -ForegroundColor DarkGray
.\mvnw.cmd spring-boot:run
