param(
    [string]$Python = 'python',
    [string]$BinaryDirectory
)
$ErrorActionPreference = 'Stop'
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
function FileHash([string]$Path) {
    $stream = [IO.File]::OpenRead($Path)
    $sha = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-', '') }
    finally { $sha.Dispose(); $stream.Dispose() }
}
function Run([string]$Command, [string[]]$Arguments) {
    $executable = (Get-Command -Name $Command -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $ErrorActionPreference = 'Continue'
    $global:LASTEXITCODE = -1
    & $executable @Arguments 2>&1 | ForEach-Object { $_.ToString() } | Write-Host
    $code = $global:LASTEXITCODE
    $ErrorActionPreference = 'Stop'
    if ($code -ne 0) { throw "$Command failed with exit code $code" }
}
$venv = Join-Path $repo 'artifacts/native-test-python'
$fixturePython = Join-Path $venv 'Scripts/python.exe'
if (-not (Test-Path -LiteralPath $fixturePython)) { Run $Python @('-m', 'venv', $venv) }
$requirements = Join-Path $PSScriptRoot 'native-test-requirements.txt'
$pin = FileHash $requirements
$marker = Join-Path $venv 'requirements.sha256'
if (-not (Test-Path -LiteralPath $marker) -or (Get-Content -LiteralPath $marker -Raw).Trim() -ne $pin) {
    Run $fixturePython @('-m', 'pip', 'install', '-r', $requirements)
    Set-Content -LiteralPath $marker -Value $pin -Encoding ASCII
}
foreach ($component in @('ffmpeg-2026-09-30', 'pdfium-8086')) {
    $output = Join-Path $repo ('artifacts/dependencies/' + $component)
    $inventory = Join-Path $output 'native-test-hashes.json'
    if (-not (Test-Path -LiteralPath $output)) {
        $fetch = if ($component.StartsWith('ffmpeg')) { 'fetch-ffmpeg.ps1' } else { 'fetch-pdfium.ps1' }
        Run powershell.exe @('-NoProfile', '-File', (Join-Path $PSScriptRoot $fetch))
    }
    $hashes = @(Get-ChildItem -LiteralPath $output -File -Recurse | Where-Object Name -ne 'native-test-hashes.json' | Sort-Object FullName | ForEach-Object {
        [ordered]@{ path = $_.FullName.Substring($output.Length + 1); sha256 = (FileHash $_.FullName) }
    })
    if (Test-Path -LiteralPath $inventory) {
        $previous = (Get-Content -LiteralPath $inventory -Raw | ConvertFrom-Json)
        if ($hashes.Count -ne $previous.Count) { throw "Native test dependency file count changed: $component" }
        for ($i = 0; $i -lt $hashes.Count; $i++) {
            if ($hashes[$i].path -cne $previous[$i].path -or $hashes[$i].sha256 -cne $previous[$i].sha256) { throw "Native test dependency changed: $component ($($hashes[$i].path))" }
        }
    } else { $hashes | ConvertTo-Json | Set-Content -LiteralPath $inventory -Encoding UTF8 }
}
if (-not (Test-Path -LiteralPath (Join-Path $repo 'artifacts/spoken-fixtures'))) {
    Run powershell.exe @('-NoProfile', '-File', (Join-Path $PSScriptRoot 'generate-spoken-fixtures.ps1'))
}
if (-not (Test-Path -LiteralPath (Join-Path $repo 'artifacts/spoken-fixtures/two-passages-65s.wav'))) {
    Run $fixturePython @((Join-Path $PSScriptRoot 'assemble-spoken-fixtures.py'))
}
if (-not (Test-Path -LiteralPath (Join-Path $repo 'artifacts/media-fixtures'))) {
    Run $fixturePython @((Join-Path $PSScriptRoot 'generate-media-fixtures.py'))
}
if (-not (Test-Path -LiteralPath (Join-Path $repo 'artifacts/pdf-fixtures/over-page-limit.pdf'))) {
    Run $fixturePython @((Join-Path $PSScriptRoot 'generate-pdf-fixtures.py'))
}
if ($BinaryDirectory) {
    $target = (Resolve-Path -LiteralPath $BinaryDirectory).Path
    $ffmpeg = Get-Content -LiteralPath (Join-Path $repo 'src/engine/third_party/ffmpeg.json') -Raw | ConvertFrom-Json
    $decoder = Join-Path $repo ('artifacts/dependencies/ffmpeg-2026-09-30/' + $ffmpeg.directory + '/bin')
    foreach ($name in $ffmpeg.required_binaries) { Copy-Item -LiteralPath (Join-Path $decoder $name) -Destination (Join-Path $target $name) -Force }
    Copy-Item -LiteralPath (Join-Path $repo 'artifacts/dependencies/pdfium-8086/bin/pdfium.dll') -Destination (Join-Path $target 'pdfium.dll') -Force
}
Write-Output 'Native fixture prerequisites prepared; no model or hardware qualification is implied.'
