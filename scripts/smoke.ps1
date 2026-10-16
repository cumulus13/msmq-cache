# End-to-end smoke test: needs MSMQ installed. Starts the server, exercises TCP via mcache-cli, then COM via regasm.
param([string]$Bin = 'dist\server', [string]$Cli = 'dist\cli\mcache-cli.exe')
$ErrorActionPreference = 'Stop'
function Fail($m) { Write-Host "SMOKE FAIL: $m"; exit 1 }
$conf = Join-Path $env:TEMP 'mcache-smoke.conf'
"port 6391`nqueue-prefix smoke`nloglevel debug" | Set-Content $conf
$p = Start-Process (Join-Path $Bin 'mcache-server.exe') -ArgumentList "--config `"$conf`"" -PassThru -RedirectStandardOutput "$env:TEMP\mcache-smoke.log" -RedirectStandardError "$env:TEMP\mcache-smoke.err"
try {
  Start-Sleep 5
  function C { & $Cli -p 6391 @args }
  if ((C PING) -ne 'PONG') { Fail 'PING' }
  C SET k v EX 30 | Out-Null
  if ((C GET k) -ne '"v"') { Fail 'GET after SET' }
  if ((C INCR n) -ne '(integer) 1') { Fail 'INCR' }
  C QPUSH q1 a PRIORITY 5 | Out-Null
  if ((C QPOP q1) -ne '"a"') { Fail 'QPOP' }
  C DEL k | Out-Null
  if ((C GET k) -ne '(nil)') { Fail 'GET after DEL' }
  $regasm = "$env:windir\Microsoft.NET\Framework64\v4.0.30319\regasm.exe"
  & $regasm /codebase (Join-Path $Bin '..\com\MsmqCache.Com.dll') | Out-Null
  $o = New-Object -ComObject MsmqCache.Client; $o.Connect('.', 'smoke'); $o.Set('ck', 'cv', 30)
  if ($o.Get('ck') -ne 'cv') { Fail 'COM Get' }
  if ((C GET ck) -ne '"cv"') { Fail 'COM write not visible via TCP' }
  Write-Host 'SMOKE OK'
} finally { Stop-Process $p -Force -ErrorAction SilentlyContinue; Get-Content "$env:TEMP\mcache-smoke.log","$env:TEMP\mcache-smoke.err" -ErrorAction SilentlyContinue | Select-Object -Last 40 }
