// File: src\MsmqCache.Server\WorkQueues.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: Native MSMQ work queues exposed as QPUSH/QPOP/...: one real MSMQ private queue per
//              logical queue (non-transactional so MSMQ message priority works; transactional queues
//              ignore priority).
// License: MIT

// Native MSMQ work queues exposed as QPUSH/QPOP/...: one real MSMQ private queue per logical queue
// (non-transactional so MSMQ message priority works; transactional queues ignore priority).

using System;
using System.Collections.Generic;
using System.IO;
using System.Messaging;
using System.Text.RegularExpressions;
using MsmqCache.Core;
using MsmqCache.Core.Msmq;

namespace MsmqCache.Server
{
    sealed class WorkQueues : IDisposable
    {
        static readonly Regex NameRx = new Regex("^[A-Za-z0-9_.-]{1,64}$", RegexOptions.Compiled);
        readonly ServerConfig _c;
        readonly Dictionary<string, MessageQueue> _open = new Dictionary<string, MessageQueue>(StringComparer.OrdinalIgnoreCase);

        public WorkQueues(ServerConfig c) { _c = c; }

        static void Check(string n) { if (!NameRx.IsMatch(n)) throw new ArgumentException("invalid queue name (allowed: A-Z a-z 0-9 _ . - , max 64)"); }

        MessageQueue Open(string name, bool create)
        {
            Check(name);
            MessageQueue q;
            if (_open.TryGetValue(name, out q)) return q;
            string path = _c.WorkQueuePrefix + name;
            if (!create && !MessageQueue.Exists(path)) return null;
            q = QueueTools.Ensure(path, false, _c.MaxQueueKb);
            q.MessageReadPropertyFilter.Body = true;
            _open[name] = q;
            return q;
        }

        public void Push(string name, byte[] val, int priority, int ttlSeconds)
        {
            var q = Open(name, true);
            using (var m = new Message
            {
                Label = name,
                Recoverable = _c.Recoverable,
                Priority = (MessagePriority)priority,
                UseDeadLetterQueue = false,
                AttachSenderId = false
            })
            {
                m.BodyStream = new MemoryStream(val, false);
                if (ttlSeconds > 0) m.TimeToBeReceived = TimeSpan.FromSeconds(ttlSeconds);
                q.Send(m);
            }
        }

        public byte[] Pop(string name, bool remove)
        {
            var q = Open(name, false);
            if (q == null) return null;
            try
            {
                using (var m = remove ? q.Receive(TimeSpan.Zero) : q.Peek(TimeSpan.Zero))
                    return QueueTools.ReadAll(m.BodyStream);
            }
            catch (MessageQueueException e) when (e.MessageQueueErrorCode == MessageQueueErrorCode.IOTimeout) { return null; }
        }

        /// <summary>O(n): MSMQ has no cheap message counter in System.Messaging, so the queue is enumerated.</summary>
        public long Len(string name)
        {
            Check(name);
            string path = _c.WorkQueuePrefix + name;
            if (!MessageQueue.Exists(path)) return 0;
            long n = 0;
            using (var mq = new MessageQueue(path))
            {
                mq.MessageReadPropertyFilter.ClearAll();
                using (var en = mq.GetMessageEnumerator2()) while (en.MoveNext()) n++;
            }
            return n;
        }

        public bool Purge(string name)
        {
            var q = Open(name, false);
            if (q == null) return false;
            q.Purge();
            return true;
        }

        public bool Delete(string name)
        {
            Check(name);
            string path = _c.WorkQueuePrefix + name;
            MessageQueue q;
            if (_open.TryGetValue(name, out q)) { q.Dispose(); _open.Remove(name); }
            if (!MessageQueue.Exists(path)) return false;
            MessageQueue.Delete(path);
            return true;
        }

        public List<string> List()
        {
            string pre = (@"private$\" + _c.QueuePrefix + ".q.").ToLowerInvariant();
            var res = new List<string>();
            foreach (var q in MessageQueue.GetPrivateQueuesByMachine("."))
            {
                string n = q.QueueName;
                if (n.ToLowerInvariant().StartsWith(pre)) res.Add(n.Substring(pre.Length));
                q.Dispose();
            }
            res.Sort(StringComparer.OrdinalIgnoreCase);
            return res;
        }

        public void Dispose() { foreach (var q in _open.Values) q.Dispose(); _open.Clear(); }
    }
}
