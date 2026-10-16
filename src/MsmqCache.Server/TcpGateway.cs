// File: src\MsmqCache.Server\TcpGateway.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: TCP front door. It does NOT touch the cache: each client command is wrapped into an MSMQ
//              message and sent to the ingress queue; the reply comes back through a reply queue and is
//              written to the socket.
// License: MIT

// TCP front door. It does NOT touch the cache: each client command is wrapped into an MSMQ message and sent
// to the ingress queue; the reply comes back through a reply queue and is written to the socket.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Messaging;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MsmqCache.Core;
using MsmqCache.Core.Msmq;

namespace MsmqCache.Server
{
    sealed class TcpGateway : IDisposable
    {
        readonly ServerConfig _c;
        readonly CancellationTokenSource _cts = new CancellationTokenSource();
        readonly ConcurrentDictionary<int, TaskCompletionSource<byte[]>> _pending = new ConcurrentDictionary<int, TaskCompletionSource<byte[]>>();
        readonly object _sendLock = new object();
        TcpListener _l;
        MessageQueue _out, _in, _replyRef;
        Thread _pump;
        int _ticket, _clients;

        public TcpGateway(ServerConfig c) { _c = c; }

        public void Start()
        {
            IPAddress ip;
            if (!IPAddress.TryParse(_c.Bind, out ip)) throw new ConfigException("bind must be an IP address, got '" + _c.Bind + "'");
            _out = new MessageQueue(_c.IngressPath, QueueAccessMode.Send);
            _in = QueueTools.Ensure(_c.TcpReplyPath, false, _c.MaxQueueKb);
            _in.MessageReadPropertyFilter.Body = true;
            _in.MessageReadPropertyFilter.AppSpecific = true;
            _in.Purge();   // replies from a previous run belong to nobody
            _replyRef = new MessageQueue(QueueTools.DirectFormat(".", _c.QueuePrefix + ".reply.tcp"));
            _pump = new Thread(Pump) { Name = "mcache-tcp-replies", IsBackground = true };
            _pump.Start();
            _l = new TcpListener(ip, _c.Port);
            _l.Start();
            Task.Run(AcceptLoop);
            Log.Info("TCP gateway listening on " + _c.Bind + ":" + _c.Port);
        }

        async Task AcceptLoop()
        {
            while (!_cts.IsCancellationRequested)
            {
                TcpClient c;
                try { c = await _l.AcceptTcpClientAsync().ConfigureAwait(false); }
                catch (Exception) { if (_cts.IsCancellationRequested) return; continue; }
                if (Interlocked.Increment(ref _clients) > _c.MaxClients)
                {
                    Interlocked.Decrement(ref _clients);
                    try { var e = Resp.Error("ERR max number of clients reached"); c.GetStream().Write(e, 0, e.Length); } catch { }
                    c.Close();
                    continue;
                }
                var cc = c;
                var ignored = Task.Run(() => Handle(cc));
            }
        }

        async Task Handle(TcpClient c)
        {
            var ct = _cts.Token;
            string remote = "?";
            try { remote = c.Client.RemoteEndPoint.ToString(); } catch { }
            Log.Debug("client connected " + remote);
            try
            {
                using (c)
                {
                    c.NoDelay = true;
                    var stream = c.GetStream();
                    var reader = new RespReader(stream, _c.MaxValueBytes + 1024);
                    bool authed = _c.RequirePass.Length == 0;
                    while (!ct.IsCancellationRequested)
                    {
                        List<byte[]> args;
                        try { args = await reader.ReadCommandAsync(ct).ConfigureAwait(false); }
                        catch (ProtocolException pe)
                        {
                            var e = Resp.Error("ERR protocol: " + pe.Message);
                            await stream.WriteAsync(e, 0, e.Length, ct).ConfigureAwait(false);
                            break;
                        }
                        if (args == null) break;
                        if (args.Count == 0) continue;
                        string cmd = Encoding.ASCII.GetString(args[0]).ToUpperInvariant();
                        byte[] reply;
                        if (cmd == "QUIT") { await stream.WriteAsync(Resp.Ok, 0, Resp.Ok.Length, ct).ConfigureAwait(false); break; }
                        else if (cmd == "AUTH")
                        {
                            if (_c.RequirePass.Length == 0) reply = Resp.Error("ERR AUTH called without any password configured");
                            else if (args.Count >= 2 && FixedEquals(Encoding.UTF8.GetString(args[args.Count - 1]), _c.RequirePass)) { authed = true; reply = Resp.Ok; }
                            else reply = Resp.Error("WRONGPASS invalid password");
                        }
                        else if (!authed) reply = Resp.Error("NOAUTH Authentication required.");
                        else reply = await Dispatch(args, ct).ConfigureAwait(false);
                        await stream.WriteAsync(reply, 0, reply.Length, ct).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) { }
            catch (Exception e) { Log.Debug("client " + remote + " closed: " + e.Message); }
            finally { Interlocked.Decrement(ref _clients); }
        }

        static bool FixedEquals(string a, string b)
        {
            int d = a.Length ^ b.Length;
            for (int i = 0; i < a.Length && i < b.Length; i++) d |= a[i] ^ b[i];
            return d == 0;
        }

        async Task<byte[]> Dispatch(List<byte[]> args, CancellationToken ct)
        {
            int ticket = Interlocked.Increment(ref _ticket);
            var tcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
            _pending[ticket] = tcs;
            try
            {
                string label = Encoding.ASCII.GetString(args[0]).ToUpperInvariant();
                if (label.Length > 64) label = label.Substring(0, 64);
                using (var m = new Message
                {
                    Label = label,
                    Recoverable = _c.Recoverable,
                    TimeToBeReceived = TimeSpan.FromSeconds(_c.RequestTtlSeconds),
                    ResponseQueue = _replyRef,
                    AppSpecific = ticket,
                    UseDeadLetterQueue = false,
                    AttachSenderId = false
                })
                {
                    m.BodyStream = new MemoryStream(Resp.EncodeCommand(args), false);
                    lock (_sendLock) _out.Send(m);
                }
                using (var delayCts = CancellationTokenSource.CreateLinkedTokenSource(ct))
                {
                    var done = await Task.WhenAny(tcs.Task, Task.Delay(_c.CommandTimeoutMs, delayCts.Token)).ConfigureAwait(false);
                    delayCts.Cancel();
                    if (done == tcs.Task) return tcs.Task.Result;
                }
                return Resp.Error("ERR timeout after " + _c.CommandTimeoutMs + " ms waiting for the MSMQ processor; the command may still be executed");
            }
            catch (MessageQueueException e)
            {
                Log.Error("send to ingress failed: " + e.MessageQueueErrorCode + ": " + e.Message);
                return Resp.Error("ERR MSMQ send failed: " + e.MessageQueueErrorCode + ": " + e.Message);
            }
            finally { TaskCompletionSource<byte[]> x; _pending.TryRemove(ticket, out x); }
        }

        void Pump()
        {
            while (!_cts.IsCancellationRequested)
            {
                Message m = null;
                try { m = _in.Receive(TimeSpan.FromMilliseconds(500)); }
                catch (MessageQueueException e) when (e.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { continue; }
                catch (Exception e) { Log.Error("reply receive failed: " + e.Message); Thread.Sleep(1000); continue; }
                using (m)
                {
                    TaskCompletionSource<byte[]> t;
                    if (_pending.TryGetValue(m.AppSpecific, out t)) t.TrySetResult(QueueTools.ReadAll(m.BodyStream));
                    else Log.Debug("late reply dropped (ticket " + m.AppSpecific + ")");
                }
            }
        }

        public void Dispose()
        {
            _cts.Cancel();
            if (_l != null) _l.Stop();
            if (_pump != null) _pump.Join(2000);
            if (_out != null) _out.Dispose();
            if (_in != null) _in.Dispose();
            if (_replyRef != null) _replyRef.Dispose();
        }
    }
}
