param([string]$Daemon = (Join-Path $PSScriptRoot '..\src\engine\target\debug\winsearchd.exe'))
$ErrorActionPreference = 'Stop'
$fixture = Join-Path ([IO.Path]::GetTempPath()) ('winsearch-isolation-' + [Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($fixture) | Out-Null
$owned = @()
$connections = @()
function Send-Frame($pipe, $message) {
    $bytes = [Text.Encoding]::UTF8.GetBytes(($message | ConvertTo-Json -Depth 10 -Compress))
    $length = [BitConverter]::GetBytes([int]$bytes.Length)
    $pipe.Write($length, 0, 4); $pipe.Write($bytes, 0, $bytes.Length); $pipe.Flush()
}
function Read-Bytes($pipe, [int]$length) {
    $bytes = [byte[]]::new($length); $offset = 0
    while ($offset -lt $length) {
        $count = $pipe.Read($bytes, $offset, $length - $offset)
        if ($count -eq 0) { throw 'fixture pipe closed unexpectedly' }
        $offset += $count
    }
    return ,$bytes
}
function Read-Reply($pipe, [long]$id) {
    while ($true) {
        $length = [BitConverter]::ToInt32((Read-Bytes $pipe 4), 0)
        if ($length -lt 0 -or $length -gt 16777216) { throw 'invalid frame length' }
        $message = [Text.Encoding]::UTF8.GetString((Read-Bytes $pipe $length)) | ConvertFrom-Json
        if ($message.id -eq $id) { return $message }
    }
}
try {
    $sid = [Security.Principal.WindowsIdentity]::GetCurrent().User.Value
    $ids = @(('a-' + [Guid]::NewGuid().ToString('N')), ('b-' + [Guid]::NewGuid().ToString('N')))
    for ($i = 0; $i -lt 2; $i++) {
        $start = [Diagnostics.ProcessStartInfo]::new([IO.Path]::GetFullPath($Daemon))
        $start.UseShellExecute = $false; $start.CreateNoWindow = $true
        $start.EnvironmentVariables['WINSEARCH_DATA_DIR'] = Join-Path $fixture "data-$i"
        $start.EnvironmentVariables['WINSEARCH_INSTANCE_ID'] = $ids[$i]
        $process = [Diagnostics.Process]::Start($start); $owned += $process
        $pipe = [IO.Pipes.NamedPipeClientStream]::new('.', "winsearch-$sid-$($ids[$i])", [IO.Pipes.PipeDirection]::InOut)
        $pipe.Connect(15000); $connections += $pipe
        Send-Frame $pipe @{type='hello'; protocol_version=1; client='isolation-fixture'; client_version='1'}
        Send-Frame $pipe @{type='get_indexing_config'; id=1}
        $fresh = Read-Reply $pipe 1
        if ($fresh.type -ne 'indexing_config' -or $fresh.config.roots.Count -ne 0) { throw 'fresh daemon registered non-consented roots' }
        $content = Join-Path $fixture "content-$i"; [IO.Directory]::CreateDirectory($content) | Out-Null
        [IO.File]::WriteAllText((Join-Path $content "unique-$i.txt"), "fixture-$i")
        $fresh.config.roots = @($content); $fresh.config.paused = $true
        Send-Frame $pipe @{type='set_indexing_config'; id=2; config=$fresh.config}
        $saved = Read-Reply $pipe 2
        if ($saved.type -ne 'indexing_config' -or $saved.config.roots[0] -ne $content) { throw 'isolated policy update failed' }
    }
    for ($i = 0; $i -lt 2; $i++) {
        if ($owned[$i].HasExited) { throw "daemon $i exited or crossed instance mutexes" }
        Send-Frame $connections[$i] @{type='get_indexing_config'; id=3}
        $reply = Read-Reply $connections[$i] 3
        if ($reply.config.roots[0] -ne (Join-Path $fixture "content-$i")) { throw 'policy crossed instance boundary' }
    }
    $connections[0].Dispose(); $owned[0].Kill(); $owned[0].WaitForExit()
    Send-Frame $connections[1] @{type='ping'; id=4}
    if ((Read-Reply $connections[1] 4).type -ne 'pong') { throw 'stopping one daemon affected the other' }
    [pscustomobject]@{ fixture=$fixture; instances=2; freshRoots=0; isolatedPolicy=$true; independentShutdown=$true } | ConvertTo-Json
}
finally {
    foreach ($pipe in $connections) { $pipe.Dispose() }
    foreach ($process in $owned) {
        if (-not $process.HasExited) { $process.Kill(); $process.WaitForExit() }
        $process.Dispose()
    }
}
