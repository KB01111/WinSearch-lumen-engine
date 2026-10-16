param([string]$ArchivePath = (Join-Path $PSScriptRoot '../artifacts/dependencies/pdfium-win-x64-8086.tgz'))
$ErrorActionPreference = 'Stop'
$testRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ('../artifacts/pdf-fetch-check-' + [guid]::NewGuid().ToString('N'))))
[IO.Directory]::CreateDirectory($testRoot) | Out-Null
. (Join-Path $PSScriptRoot 'fetch-pdfium.ps1') -ArchivePath $ArchivePath -OutputDirectory (Join-Path $testRoot 'verified')
if (-not (Test-Path -LiteralPath (Join-Path $testRoot 'verified/bin/pdfium.dll')) -or
    -not (Test-Path -LiteralPath (Join-Path $testRoot 'verified/LICENSE')) -or
    @(Get-ChildItem -LiteralPath (Join-Path $testRoot 'verified/licenses') -File).Count -eq 0) { throw 'Verified dependency or notices absent' }
foreach ($unsafe in @('../outside','a/../../outside','/absolute','C:/absolute','C:relative','\\host\share','a\..\outside','')) {
    $rejected = $false
    try { Assert-SafeArchivePath $unsafe } catch { $rejected = $true }
    if (-not $rejected) { throw "Unsafe archive entry accepted: $unsafe" }
}
foreach ($safe in @('bin/pdfium.dll','./LICENSE','licenses/pdfium.txt')) { Assert-SafeArchivePath $safe }
$bad = Join-Path $testRoot 'tampered.tgz'; [IO.File]::WriteAllText($bad,'tampered')
$rejected = $false
try { & (Join-Path $PSScriptRoot 'fetch-pdfium.ps1') -ArchivePath $bad -OutputDirectory (Join-Path $testRoot 'rejected') } catch { $rejected = $_.Exception.Message.Contains('SHA-256 mismatch') }
if (-not $rejected -or (Test-Path -LiteralPath (Join-Path $testRoot 'rejected'))) { throw 'Tampered archive extracted or not rejected' }
$rejected = $false
try { & (Join-Path $PSScriptRoot 'fetch-pdfium.ps1') -ArchivePath $ArchivePath -OutputDirectory (Join-Path $testRoot 'verified') } catch { $rejected = $_.Exception.Message.Contains('already exists') }
if (-not $rejected) { throw 'Existing output overwritten' }
Write-Output 'PDFium fetch checks: verified extraction/notices; 8 unsafe paths rejected; 3 safe paths accepted; tampered hash rejected before extraction; existing output preserved.'
