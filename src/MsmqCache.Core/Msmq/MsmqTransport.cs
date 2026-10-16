// File: src\MsmqCache.Core\Msmq\MsmqTransport.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: MsmqCache.Core (net48 only) - shared MSMQ helpers: queue creation and a synchronous
//              request/reply requester used by the COM component. The server's TCP gateway uses 
//              the same message format.
// License: MIT

// MsmqCache.Core (net48 only) - shared MSMQ helpers: queue creation and a synchronous request/reply requester
// used by the COM component. The server's TCP gateway uses the same message format.

using System;
using System.Collections.Generic;
using System.IO;
using System.Messaging;
using System.Text;

namespace MsmqCache.Core.Msmq
{
    public static class QueueTools
    {
        /// <summary>Creates the local private queue if missing and opens it. Fails loudly if the Transactional flag differs.</summary>
        public static MessageQueue Ensure(string path, bool transactional, long maxKb)
        {
            if (!MessageQueue.Exists(path))
            {
                try
                {
                    using (var created = MessageQueue.Create(path, transactional))
                        if (maxKb > 0) created.MaximumQueueSize = maxKb;
                }
                catch (MessageQueueException) { if (!MessageQueue.Exists(path)) throw; }
            }
            var q = new MessageQueue(path, QueueAccessMode.SendAndReceive);
            if (q.Transactional != transactional)
            {
                q.Dispose();
                throw new InvalidOperationException("Queue " + path + " exists with Transactional=" + !transactional +
                    " but " + transactional + " is required. Delete the queue (or change queue-prefix) and restart.");
            }
            return q;
        }

        public static byte[] ReadAll(Stream s)
        {
            if (s.CanSeek) s.Position = 0;
            using (var ms = new MemoryStream()) { s.CopyTo(ms); return ms.ToArray(); }
        }

        /// <summary>DIRECT=OS format name usable for local and remote machines (works in workgroup mode, no AD needed).</summary>
        public static string DirectFormat(string host, string queueName)
        {
            if (string.IsNullOrEmpty(host) || host == ".") host = Environment.MachineName;
            return @"FormatName:DIRECT=OS:" + host + @"\private$\" + queueName;
        }
    }

    /// <summary>
    /// Synchronous request/reply over MSMQ: command -> ingress queue; reply collected from a shared local reply
    /// queue with ReceiveByCorrelationId (so many clients on one machine share one queue and no queue is leaked).
    /// </summary>
    public sealed class MsmqRequester : IDisposable
    {
        readonly MessageQueue _ingress;
        readonly MessageQueue _replies;
        readonly MessageQueue _replyRef;
        readonly bool _recoverable;
        readonly int _requestTtl;
        readonly object _lock = new object();

        public MsmqRequester(string serverHost, string prefix, bool recoverable = true, int requestTtlSeconds = 60)
        {
            _recoverable = recoverable;
            _requestTtl = requestTtlSeconds;
            _ingress = new MessageQueue(QueueTools.DirectFormat(serverHost, prefix + ".ingress"), QueueAccessMode.Send);
            string replyName = prefix + ".reply.com";
            _replies = QueueTools.Ensure(@".\private$\" + replyName, false, 0);
            _replyRef = new MessageQueue(QueueTools.DirectFormat(".", replyName));
        }

        Message Build(IList<byte[]> args, bool wantReply)
        {
            var m = new Message
            {
                Label = Encoding.ASCII.GetString(args[0]).ToUpperInvariant(),
                Recoverable = _recoverable,
                TimeToBeReceived = TimeSpan.FromSeconds(_requestTtl),
                UseDeadLetterQueue = false,
                AttachSenderId = false
            };
            if (wantReply) m.ResponseQueue = _replyRef;
            m.BodyStream = new MemoryStream(Resp.EncodeCommand(args));
            return m;
        }

        /// <summary>Fire and forget: the server does not send a reply.</summary>
        public void Post(IList<byte[]> args)
        {
            lock (_lock) { using (var m = Build(args, false)) _ingress.Send(m); }
        }

        /// <summary>Sends the command and waits for the matching reply (RESP bytes).</summary>
        public byte[] Request(IList<byte[]> args, int timeoutMs)
        {
            lock (_lock)
            {
                using (var m = Build(args, true))
                {
                    _ingress.Send(m);   // m.Id is assigned by MSMQ on send
                    try
                    {
                        using (var r = _replies.ReceiveByCorrelationId(m.Id, TimeSpan.FromMilliseconds(timeoutMs)))
                            return QueueTools.ReadAll(r.BodyStream);
                    }
                    catch (MessageQueueException e) when (e.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout)
                    {
                        throw new TimeoutException("No reply within " + timeoutMs + " ms. The command may still be executed by the server.");
                    }
                }
            }
        }

        public void Dispose()
        {
            _ingress.Dispose(); _replies.Dispose(); _replyRef.Dispose();
        }
    }
}
