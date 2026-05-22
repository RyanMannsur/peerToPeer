<#
.SYNOPSIS
  Wrapper PowerShell para executar um teste P2P real com logs e resultado em JSON.

.EXAMPLE
  .\scripts\run_multi.ps1 -Peers 2 -BlockSize 1024 -FileSize 10240 -Timeout 30
#>

param(
    [int]$Peers = 2,
    [int]$BlockSize = 1024,
    [int]$FileSize = 10240,
    [int]$Timeout = 30,
    [int]$BasePort = 11000
)

$ErrorActionPreference = 'Stop'
$root = $PSScriptRoot | Split-Path -Parent

Write-Host "Running real P2P test via Python runner..."
python (Join-Path $root 'scripts\run_real_tests.py') --peers $Peers --block-size $BlockSize --file-size $FileSize --timeout $Timeout --base-port $BasePort
