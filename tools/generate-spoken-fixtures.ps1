$ErrorActionPreference = 'Stop'
Add-Type -AssemblyName System.Speech
$taskRepository = Split-Path -Parent $PSScriptRoot
$taskArtifacts = [System.IO.Path]::GetFullPath((Join-Path $taskRepository 'artifacts'))
$taskOutput = [System.IO.Path]::GetFullPath((Join-Path $taskArtifacts 'spoken-fixtures'))
if (-not $taskOutput.StartsWith($taskArtifacts + [System.IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) { throw 'Unexpected fixture output' }
if (Test-Path -LiteralPath $taskOutput) { throw 'Fixture output already exists; inspect before replacing' }
[void][System.IO.Directory]::CreateDirectory($taskOutput)
$taskTexts = [ordered]@{
    'railway.wav' = 'At the railway station, the train to Stockholm leaves from platform five at noon. Please keep your ticket ready.'
    'kitchen.wav' = 'In the kitchen, the chef mixes flour, butter, eggs and sugar to bake a chocolate cake. The oven is hot.'
}
$taskSynth = New-Object System.Speech.Synthesis.SpeechSynthesizer
try {
    $taskSynth.SetOutputToNull()
    $taskSynth.SelectVoice('Microsoft Zira Desktop')
    $taskSynth.Rate = 0
    $taskFormat = [System.Speech.AudioFormat.SpeechAudioFormatInfo]::new(16000, [System.Speech.AudioFormat.AudioBitsPerSample]::Sixteen, [System.Speech.AudioFormat.AudioChannel]::Mono)
    foreach ($taskName in $taskTexts.Keys) {
        $taskSynth.SetOutputToWaveFile((Join-Path $taskOutput $taskName), $taskFormat)
        $taskSynth.Speak($taskTexts[$taskName])
        $taskSynth.SetOutputToNull()
    }
    $taskManifest = [ordered]@{ generatedAt = [DateTimeOffset]::Now.ToString('o'); voice = $taskSynth.Voice.Name; culture = $taskSynth.Voice.Culture.Name; rate = $taskSynth.Rate; samplesPerSecond = 16000; bitsPerSample = 16; channels = 1; texts = $taskTexts; method = 'Local installed System.Speech.Synthesis voice, file-only output; no playback, voice installation, external service, or personal content' }
    [System.IO.File]::WriteAllText((Join-Path $taskOutput 'generation.json'), ($taskManifest | ConvertTo-Json -Depth 5), (New-Object System.Text.UTF8Encoding($false)))
    $taskManifest | ConvertTo-Json -Depth 5
} finally { $taskSynth.Dispose() }
