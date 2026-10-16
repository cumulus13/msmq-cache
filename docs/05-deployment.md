# 5. Deployment and operations

## 5.1 Server (Windows)
1. Enable MSMQ: `Enable-WindowsOptionalFeature -Online -FeatureName MSMQ-Server -All` (Server: *Message Queuing* feature). Workgroup mode is sufficient; the design uses private queues and `DIRECT=OS:` format names, no Active Directory.
2. Copy the release zip, edit `mcache.conf`.
3. Console: `mcache-server.exe --config mcache.conf`.
4. Service (admin): `sc.exe create MsmqCache binPath= "\"C:\mcache\mcache-server.exe\" --config \"C:\mcache\mcache.conf\"" start= auto depend= MSMQ`
5. Firewall: TCP port from `port`; for remote COM/MSMQ clients also MSMQ's TCP 1801 (and the RPC ports MSMQ documents for remote queue management).

## 5.2 COM registration (elevated, per bitness)
```bat
%windir%\Microsoft.NET\Framework64\v4.0.30319\regasm.exe /codebase MsmqCache.Com.dll /tlb
%windir%\Microsoft.NET\Framework\v4.0.30319\regasm.exe   /codebase MsmqCache.Com.dll /tlb   :: 32-bit hosts
```
Unregister with `regasm /u`. `MsmqCache.Com.dll` needs `MsmqCache.Core.dll` next to it.

```vbs
Set c = CreateObject("MsmqCache.Client")
c.Connect ".", "mcache"          ' or a remote machine name
c.Set "session:1", "alice", 300
WScript.Echo c.Get("session:1")
WScript.Echo c.Execute("INCR hits")
```
```powershell
$c = New-Object -ComObject MsmqCache.Client; $c.Connect(".", "mcache"); $c.Get("session:1")
```

## 5.3 Remote COM/MSMQ clients
The client machine needs MSMQ too (it owns the reply queue). Grant the client account Send on the ingress queue (`ingress-send-account`), and allow the server's account to send to the client's `prefix.reply.com`. Prefer TCP for cross-OS or locked-down networks.

## 5.4 Operations
* Inspect: Computer Management > Message Queuing > Private Queues; `INFO` command.
* Queue names / flags are fixed at creation: if `.data` was created non-transactional (or vice versa) the server refuses to start and tells you to delete the queue.
* Back up: stop the service and copy MSMQ storage (`%windir%\System32\msmq\storage`), or export keys through the client.
* Exit codes: 0 ok, 1 config error, 2 usage, 3 fatal.
