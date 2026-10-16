// File: src\MsmqCache.Server\Engine.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: The single consumer of the ingress queue. Every command - from TCP or COM - arrives here as an MSMQ message.
// License: MIT

// The single consumer of the ingress queue. Every command - from TCP or COM - arrives here as an MSMQ message.

using System;
using System.IO;
using System.Messaging;
using System.Threading;
using MsmqCache.Core;
using MsmqCache.Core.Msmq;

namespace MsmqCache.Server
{
    sealed class Engine : IDisposable
    {
        readonly ServerConfig _c;
        readonly CommandProcessor _p;
        readonly CacheStore _store;
        MessageQueue _ingress;
        Thread _t;
        volatile bool _stop;

        public Engine(ServerConfig c, CommandProcessor p, CacheStore store) { _c = c; _p = p; _store = store; }

        public void Start()
        {
            _ingress = QueueTools.Ensure(_c.IngressPath, false, _c.MaxQueueKb);
            var f = _ingress.MessageReadPropertyFilter;
            f.Body = true; f.Id = true; f.AppSpecific = true; f.ResponseQueue = true;
            foreach (var acct in _c.IngressSendAccounts)
            {
                _ingress.SetPermissions(acct, MessageQueueAccessRights.WriteMessage, AccessControlEntryType.Allow);
                Log.Info("granted WriteMessage on " + _c.IngressPath + " to '" + acct + "'");
            }
            _t = new Thread(Loop) { Name = "mcache-engine", IsBackground = false };
            _t.Start();
            Log.Info("engine consuming " + _c.IngressPath);
        }

        void Loop()
        {
            DateTime nextSweep = DateTime.UtcNow.AddSeconds(_c.SweepIntervalSeconds);
            while (!_stop)
            {
                Message m = null;
                try { m = _ingress.Receive(TimeSpan.FromMilliseconds(500)); }
                catch (MessageQueueException e) when (e.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { }
                catch (Exception e) { Log.Error("ingress receive failed: " + e.Message); Thread.Sleep(1000); }

                if (m != null) { using (m) Handle(m); }

                if (DateTime.UtcNow >= nextSweep)
                {
                    nextSweep = DateTime.UtcNow.AddSeconds(_c.SweepIntervalSeconds);
                    try { int n = _store.Sweep(); if (n > 0) Log.Debug("swept " + n + " expired keys"); }
                    catch (Exception e) { Log.Warn("sweep failed: " + e.Message); }
                }
            }
        }

        void Handle(Message m)
        {
            byte[] reply;
            try
            {
                var args = RespReader.ParseCommand(QueueTools.ReadAll(m.BodyStream));
                reply = _p.Execute(args);
            }
            catch (Exception e)
            {
                Log.Error("bad ingress message " + m.Id + ": " + e.Message);
                reply = Resp.Error("ERR protocol: " + e.Message);
            }

            MessageQueue rq = null;
            try { rq = m.ResponseQueue; } catch (Exception) { }
            if (rq == null) return;   // fire-and-forget (COM Post)
            try
            {
                using (rq)
                using (var r = new Message
                {
                    CorrelationId = m.Id,
                    AppSpecific = m.AppSpecific,
                    Label = "reply",
                    Recoverable = false,
                    TimeToBeReceived = TimeSpan.FromSeconds(_c.ReplyTtlSeconds),
                    UseDeadLetterQueue = false,
                    AttachSenderId = false
                })
                {
                    r.BodyStream = new MemoryStream(reply, false);
                    rq.Send(r);
                }
            }
            catch (Exception e) { Log.Warn("could not deliver reply to " + rq.FormatName + ": " + e.Message); }
        }

        public void Dispose()
        {
            _stop = true;
            if (_t != null) _t.Join(5000);
            if (_ingress != null) _ingress.Dispose();
        }
    }
}
