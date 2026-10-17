param([string]$ArchivePath = (Join-Path $PSScriptRoot '../artifacts/dependencies/ffmpeg-win64-lgpl-shared-2026-09-30.zip'))
$ErrorActionPreference = 'Stop'
$testRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot ('../artifacts/ffmpeg-fetch-check-' + [guid]::NewGuid().ToString('N'))))
[IO.Directory]::CreateDirectory($testRoot) | Out-Null
. (Join-Path $PSScriptRoot 'fetch-ffmpeg.ps1') -ArchivePath $ArchivePath -OutputDirectory (Join-Path $testRoot 'verified')
foreach ($unsafe in @('../outside','a/../../outside','/absolute','C:/absolute','C:relative','\\host\share','a\..\outside','','a/./b','a//b','a./file','a /file','bin/NUL','bin/con.txt','bin/COM1.log','bin/a?b')) {
    $rejected = $false
    try { Assert-SafeDecoderPath $unsafe } catch { $rejected = $true }
    if (-not $rejected) { throw "Unsafe archive entry accepted: $unsafe" }
}
foreach ($safe in @('bin/ffmpeg.exe','LICENSE.txt','doc/ffmpeg.html')) { Assert-SafeDecoderPath $safe }
foreach ($mode in @(0xA000,0x1000,0x2000,0x6000)) {
    $rejected = $false
    try { Assert-SafeDecoderType ($mode -shl 16) } catch { $rejected = $true }
    if (-not $rejected) { throw "Unsafe archive type accepted: $mode" }
}
$bad = Join-Path $testRoot 'tampered.zip'
[IO.File]::WriteAllText($bad, 'tampered')
$rejected = $false
try { & (Join-Path $PSScriptRoot 'fetch-ffmpeg.ps1') -ArchivePath $bad -OutputDirectory (Join-Path $testRoot 'rejected') } catch { $rejected = $_.Exception.Message.Contains('SHA-256 mismatch') }
if (-not $rejected -or (Test-Path -LiteralPath (Join-Path $testRoot 'rejected'))) { throw 'Tampered archive extracted or not rejected' }
$rejected = $false
try { & (Join-Path $PSScriptRoot 'fetch-ffmpeg.ps1') -ArchivePath $ArchivePath -OutputDirectory (Join-Path $testRoot 'verified') } catch { $rejected = $_.Exception.Message.Contains('already exists') }
if (-not $rejected) { throw 'Existing output overwritten' }
Write-Output 'FFmpeg fetch checks: verified extraction/binaries/notices; 16 unsafe paths and 4 link/special types rejected; tampered hash rejected; existing output preserved.'
