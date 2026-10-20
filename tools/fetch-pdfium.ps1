param(
    [string]$OutputDirectory = (Join-Path $PSScriptRoot '../artifacts/dependencies/pdfium-8086'),
    [string]$ArchivePath
)
$ErrorActionPreference = 'Stop'
$manifest = Get-Content -LiteralPath (Join-Path $PSScriptRoot '../src/engine/third_party/pdfium.json') -Raw | ConvertFrom-Json
$target = [IO.Path]::GetFullPath($OutputDirectory)
if (Test-Path -LiteralPath $target) { throw "Output already exists: $target" }
$parent = Split-Path -Parent $target
[IO.Directory]::CreateDirectory($parent) | Out-Null
function Assert-NoReparse([string]$Path) {
    $item = Get-Item -LiteralPath $Path -Force
    while ($null -ne $item) {
        if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) { throw "Reparse path rejected: $($item.FullName)" }
        $item = $item.Parent
    }
}
Assert-NoReparse $parent
function Assert-SafeArchivePath([string]$Name) {
    $normalized = $Name.Replace('\','/')
    if ([string]::IsNullOrWhiteSpace($Name) -or $normalized.StartsWith('/') -or $normalized.Contains(':') -or
        $normalized.Split('/') -contains '..' -or $Name.IndexOf([char]0) -ge 0) { throw "Unsafe archive path: $Name" }
}
$download = $null
$stream = $null
$stage = Join-Path $parent ('.pdfium-' + [guid]::NewGuid().ToString('N'))
try {
    if ([string]::IsNullOrEmpty($ArchivePath)) {
        $download = Join-Path $parent ('.pdfium-' + [guid]::NewGuid().ToString('N') + '.tgz')
        Invoke-WebRequest -Uri $manifest.url -OutFile $download -UseBasicParsing
        $archive = $download
    } else { $archive = (Resolve-Path -LiteralPath $ArchivePath).Path }
    Assert-NoReparse (Split-Path -Parent $archive)
    $stream = [IO.File]::OpenRead($archive)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { $hash = [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-','').ToLowerInvariant() }
    finally { $sha.Dispose() }
    if ($hash -ne $manifest.sha256) { throw 'PDFium SHA-256 mismatch; archive not extracted' }
    $entries = @(& tar -tzf $archive)
    if ($LASTEXITCODE -ne 0 -or $entries.Count -eq 0) { throw 'Cannot list PDFium archive' }
    foreach ($entry in $entries) { Assert-SafeArchivePath $entry }
    $details = @(& tar -tvzf $archive)
    if ($LASTEXITCODE -ne 0) { throw 'Cannot inspect PDFium archive types' }
    foreach ($entry in $details) {
        if ($entry -notmatch '^[-d]') { throw "Archive link or special entry rejected: $entry" }
    }
    [IO.Directory]::CreateDirectory($stage) | Out-Null
    & tar -xzf $archive -C $stage --no-same-owner --no-same-permissions
    if ($LASTEXITCODE -ne 0) { throw 'PDFium extraction failed' }
    foreach ($required in @('bin/pdfium.dll','LICENSE','licenses')) {
        if (-not (Test-Path -LiteralPath (Join-Path $stage $required))) { throw "PDFium archive lacks $required" }
    }
    Assert-NoReparse $stage
    Move-Item -LiteralPath $stage -Destination $target
    Write-Output "Verified PDFium $($manifest.version): $target (DLL, LICENSE and licenses/ retained)"
} finally {
    if ($null -ne $stream) { $stream.Dispose() }
    # Only exact, freshly generated staging/download paths in the validated parent.
    if (Test-Path -LiteralPath $stage) {
        if ([IO.Path]::GetDirectoryName([IO.Path]::GetFullPath($stage)) -ne $parent) { throw 'Invalid staging cleanup path' }
        Remove-Item -LiteralPath $stage -Recurse -Force
    }
    if ($null -ne $download -and (Test-Path -LiteralPath $download)) { Remove-Item -LiteralPath $download -Force }
}
