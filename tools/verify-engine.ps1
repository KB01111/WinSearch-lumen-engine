param(
    [ValidateSet('contracts', 'lint', 'test', 'release', 'all')]
    [string]$Suite = 'contracts',
    [string]$TargetDirectory,
    [string]$ReportDirectory = (Join-Path $PSScriptRoot ('../artifacts/verification-' + [DateTime]::UtcNow.ToString('yyyyMMdd-HHmmss')))
)
$ErrorActionPreference = 'Stop'
$repo = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$report = [IO.Path]::GetFullPath($ReportDirectory)
New-Item -ItemType Directory -Path $report -Force | Out-Null
$checks = [Collections.Generic.List[object]]::new()
$cargoTarget = @()
if ($TargetDirectory) { $cargoTarget = @('--target-dir', [IO.Path]::GetFullPath($TargetDirectory)) }
$head = (& git -C $repo rev-parse HEAD).Trim()
if ($LASTEXITCODE -ne 0) { throw 'Cannot identify source revision' }
$sourceStatus = @(& git -C $repo status --porcelain)
$sourceDiff = (& git -c core.safecrlf=false -C $repo diff --binary HEAD) -join "`n"
$sha = [Security.Cryptography.SHA256]::Create()
try { $diffHash = [BitConverter]::ToString($sha.ComputeHash([Text.Encoding]::UTF8.GetBytes($sourceDiff))).Replace('-', '').ToLowerInvariant() }
finally { $sha.Dispose() }
$untracked = @(& git -C $repo ls-files --others --exclude-standard | ForEach-Object {
    $path = $_
    $stream = [IO.File]::OpenRead((Join-Path $repo $path))
    $sha = [Security.Cryptography.SHA256]::Create()
    try { [ordered]@{ path = $path; sha256 = [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-', '').ToLowerInvariant() } }
    finally { $sha.Dispose(); $stream.Dispose() }
})

function Invoke-Check([string]$Name, [string]$Directory, [string]$Command, [string[]]$Arguments) {
    $executable = (Get-Command -Name $Command -CommandType Application -ErrorAction Stop | Select-Object -First 1).Source
    $started = [DateTime]::UtcNow
    Push-Location $Directory
    try {
        # Windows PowerShell treats native stderr as errors, including cargo progress.
        $ErrorActionPreference = 'Continue'
        try {
            $global:LASTEXITCODE = -1
            & $executable @Arguments 2>&1 | ForEach-Object { $_.ToString() } | Tee-Object -FilePath (Join-Path $report ($Name + '.log'))
            $code = $global:LASTEXITCODE
        } finally { $ErrorActionPreference = 'Stop' }
        $checks.Add([ordered]@{ name = $Name; command = $Command; arguments = $Arguments;
            cwd = $Directory; startedUtc = $started.ToString('o'); completedUtc = [DateTime]::UtcNow.ToString('o'); exitCode = $code })
        if ($code -ne 0) { throw "$Name failed with exit code $code" }
    } finally { Pop-Location }
}

try {
    $engine = Join-Path $repo 'src/engine'
    if ($Suite -in @('contracts', 'all')) {
        Invoke-Check 'client-independent' $engine cargo (@('check', '--locked', '-p', 'ws-client') + $cargoTarget)
        Invoke-Check 'python-contracts' $repo python @('-m', 'unittest', 'discover', '-s', 'tools', '-p', 'test_*.py', '-v')
        Invoke-Check 'dotnet-contracts' $repo dotnet @('test', 'WinSearch.slnx', '--nologo')
    }
    if ($Suite -in @('lint', 'all')) {
        Invoke-Check 'format' $engine cargo @('fmt', '--all', '--', '--check')
        Invoke-Check 'clippy' $engine cargo (@('clippy', '--locked', '--workspace', '--all-targets', '--all-features') + $cargoTarget + @('--', '-D', 'warnings'))
    }
    if ($Suite -in @('test', 'all')) {
        Invoke-Check 'debug-binaries' $engine cargo (@('build', '--locked', '--workspace') + $cargoTarget)
        Push-Location $engine
        try {
            $metadataJson = & cargo metadata --no-deps --format-version 1
            if ($LASTEXITCODE -ne 0) { throw 'Cannot resolve Cargo target directory' }
            $metadata = $metadataJson | ConvertFrom-Json
        } finally { Pop-Location }
        $profile = if ($TargetDirectory) { Join-Path ([IO.Path]::GetFullPath($TargetDirectory)) 'debug' } else { Join-Path $metadata.target_directory 'debug' }
        Invoke-Check 'native-prerequisites' $repo powershell.exe @('-NoProfile', '-File', (Join-Path $PSScriptRoot 'prepare-native-tests.ps1'), '-BinaryDirectory', $profile)
        Invoke-Check 'rust-tests' $engine cargo (@('test', '--locked', '--workspace', '--all-targets', '--all-features') + $cargoTarget + @('--', '--test-threads=1'))
    }
    if ($Suite -in @('release', 'all')) {
        Invoke-Check 'release-binaries' $engine cargo (@('build', '--locked', '--release', '--workspace') + $cargoTarget)
        Invoke-Check 'winui-release' $repo dotnet @('build', 'src/app/WinSearch.App/WinSearch.App.csproj', '-c', 'Release', '-p:Platform=x64', '--nologo')
    }
} finally {
    [ordered]@{ schemaVersion = 1; sourceCommit = $head; sourceStatus = $sourceStatus; sourceDiffSha256 = $diffHash; untrackedFiles = $untracked; suite = $Suite; checks = @($checks.ToArray());
        acceptanceScope = 'Source checks only. Ignored model, hardware, copied-package and installed gates require explicit separate runs.' } |
        ConvertTo-Json -Depth 8 | Set-Content -LiteralPath (Join-Path $report 'checks.json') -Encoding UTF8
}
