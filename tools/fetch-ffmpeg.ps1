param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot '../artifacts/dependencies/ffmpeg-2026-09-30'),
    [string]$ArchivePath
)
$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.IO.Compression
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot '../src/engine/third_party/ffmpeg.json') -Raw | ConvertFrom-Json
$target = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $target) { throw "Output already exists: $target" }
$parent = Split-Path -Parent $target
[IO.Directory]::CreateDirectory($parent) | Out-Null
function Assert-NoDecoderReparse([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force
    while ($null -ne $item) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "Reparse path rejected: $($item.FullName)" }
        $item = $item.Parent
    }
}
function Assert-SafeDecoderPath([string]$Name) {
    $normalized = $Name.Replace('\','/')
    if ([string]::IsNullOrWhiteSpace($Name) -or $normalized.StartsWith('/') -or $normalized.Contains(':') -or
        $Name -match '[\x00-\x1f]') { throw "Unsafe decoder archive path: $Name" }
    foreach ($part in $normalized.TrimEnd('/').Split('/')) {
        if ($part -in @('','..','.') -or $part.EndsWith('.') -or $part.EndsWith(' ') -or
            $part -match '[<>"|?*]' -or
            $part -match '^(?i:con|prn|aux|nul|conin\$|conout\$|com[1-9\u00b9\u00b2\u00b3]|lpt[1-9\u00b9\u00b2\u00b3])(?:\.|$)') { throw "Unsafe decoder archive path: $Name" }
    }
}
function Assert-SafeDecoderType([long]$Attributes) {
    $kind = ($Attributes -shr 16) -band 0xF000
    if ($kind -notin @(0,0x4000,0x8000)) { throw 'Decoder archive link or special entry rejected' }
}
Assert-NoDecoderReparse $parent
$download = $null; $stream = $null; $zip = $null
$stage = Join-Path $parent ('.ffmpeg-' + [guid]::NewGuid().ToString('N'))
try {
    if ([string]::IsNullOrEmpty($ArchivePath)) {
        $download = Join-Path $parent ('.ffmpeg-' + [guid]::NewGuid().ToString('N') + '.zip')
        Invoke-WebRequest -Uri $manifest.url -OutFile $download -UseBasicParsing
        $archive = $download
    } else { $archive = (Resolve-Path -LiteralPath $ArchivePath).Path }
    Assert-NoDecoderReparse $archive
    $stream = [IO.File]::OpenRead($archive)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $hash = [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-','').ToLowerInvariant() }
    finally { $sha.Dispose() }
    if ($hash -ne $manifest.sha256) { throw 'FFmpeg SHA-256 mismatch; archive not extracted' }
    $stream.Position = 0
    $zip = [IO.Compression.ZipArchive]::new($stream, [IO.Compression.ZipArchiveMode]::Read, $true)
    if ($zip.Entries.Count -eq 0 -or $zip.Entries.Count -gt 20000) { throw 'Decoder archive entry count exceeds bound' }
    $names = [Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $total = [long]0
    foreach ($entry in $zip.Entries) {
        Assert-SafeDecoderPath $entry.FullName
        Assert-SafeDecoderType $entry.ExternalAttributes
        if (-not $entry.FullName.StartsWith(($manifest.directory + '/'), [StringComparison]::Ordinal) -or
            -not $names.Add($entry.FullName.Replace('\','/').TrimEnd('/'))) { throw 'Unexpected/duplicate decoder archive root or entry' }
        $total += $entry.Length
        if ($entry.Length -lt 0 -or $entry.Length -gt 256MB -or $total -gt 2GB) { throw 'Decoder archive expansion exceeds bound' }
    }
    [IO.Directory]::CreateDirectory($stage) | Out-Null
    $stagePrefix = [IO.Path]::GetFullPath($stage) + [IO.Path]::DirectorySeparatorChar
    $buffer = [byte[]]::new(65536)
    foreach ($entry in $zip.Entries) {
        $path = [IO.Path]::GetFullPath((Join-Path $stage $entry.FullName))
        if (-not $path.StartsWith($stagePrefix, [StringComparison]::OrdinalIgnoreCase)) { throw 'Decoder archive escaped staging root' }
        if ($entry.FullName.EndsWith('/')) { [IO.Directory]::CreateDirectory($path) | Out-Null; continue }
        [IO.Directory]::CreateDirectory([IO.Path]::GetDirectoryName($path)) | Out-Null
        $entryStream = $entry.Open(); $output = $null
        try {
            $output = [IO.File]::Open($path, [IO.FileMode]::CreateNew, [IO.FileAccess]::Write, [IO.FileShare]::None)
            $written = [long]0
            while (($read = $entryStream.Read($buffer, 0, $buffer.Length)) -gt 0) {
                $written += $read
                if ($written -gt $entry.Length) { throw 'Decoder archive declared length exceeded' }
                $output.Write($buffer, 0, $read)
            }
            if ($written -ne $entry.Length) { throw 'Decoder archive entry truncated' }
        } finally { if ($null -ne $output) { $output.Dispose() }; $entryStream.Dispose() }
    }
    $build = Join-Path $stage $manifest.directory
    foreach ($binary in $manifest.required_binaries) {
        if (-not (Test-Path -LiteralPath (Join-Path $build ('bin/' + $binary)) -PathType Leaf)) { throw "Decoder archive lacks $binary" }
    }
    if (-not (Test-Path -LiteralPath (Join-Path $build 'LICENSE.txt') -PathType Leaf)) { throw 'Decoder LGPL license absent' }
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../src/engine/third_party/licenses/FFmpeg-GPLv3.txt') -Destination (Join-Path $build 'COPYING.GPLv3')
    Copy-Item -LiteralPath (Join-Path $PSScriptRoot '../src/engine/third_party/ffmpeg-source.md') -Destination (Join-Path $build 'SOURCE-INSTRUCTIONS.md')
    Assert-NoDecoderReparse $stage
    Move-Item -LiteralPath $stage -Destination $target
    Write-Output "Verified FFmpeg $($manifest.version): $target (binaries, shared DLLs and notices retained)"
} finally {
    if ($null -ne $zip) { $zip.Dispose() }
    if ($null -ne $stream) { $stream.Dispose() }
    if (Test-Path -LiteralPath $stage) {
        if ([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($stage)) -ne $parent) { throw 'Invalid decoder staging cleanup path' }
        Assert-NoDecoderReparse $stage
        Remove-Item -LiteralPath $stage -Recurse -Force
    }
    if ($null -ne $download -and (Test-Path -LiteralPath $download)) { Remove-Item -LiteralPath $download -Force }
}
