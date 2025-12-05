# PowerShell helper to run pipeline stream ingestion
param(
    [string]$Units = '16977',
    [string]$StorageRoot = '',
    [string]$Token = '',
    [string]$WialonHost = '',
    [double]$Interval = 5,
    [string]$SourceKind = ''
)

Set-Location -Path "C:\bots\mybot"
# Ensure python can resolve the local 'pipeline' package even if caller runs the script from elsewhere.
$env:PYTHONPATH = if ($env:PYTHONPATH) { "C:\bots\mybot;$env:PYTHONPATH" } else { "C:\bots\mybot" }

if ($Token) { $env:PIPELINE_WIALON_TOKEN = $Token }
if ($WialonHost) { $env:PIPELINE_WIALON_HOST = $WialonHost }
if ($SourceKind) { $env:PIPELINE_SOURCE_KIND = $SourceKind }

$storageArg = if ($StorageRoot) { "--storage-root `"$StorageRoot`"" } else { '' }
$sourceArg = if ($SourceKind) { "--source-kind $SourceKind" } else { '' }
$cmd = "python -m pipeline.cli.run_stream --units $Units --interval $Interval $storageArg $sourceArg"
Write-Host "Starting pipeline stream: $cmd" -ForegroundColor Cyan
Invoke-Expression $cmd
