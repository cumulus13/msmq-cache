// File: src\MsmqCache.Server\CacheStore.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: 
// License: MIT

// Key/value cache whose DATA lives in an MSMQ transactional queue:
//   one message per key  -> Body = value, Extension = [expireTicks(8) + key UTF-8], Label = key (truncated)
//   TTL                  -> MSMQ TimeToBeReceived (MSMQ itself drops the message when it expires)
//   durability           -> transactional queue (always disk backed)
//   SET                  -> one MSMQ transaction: send new message + receive (remove) the old one
// An in-memory index key -> LookupId is rebuilt from the queue at startup. Only the single processor thread touches it.

using System;
using System.Collections.Generic;
using System.IO;
using System.Messaging;
using System.Text;
using MsmqCache.Core;
using MsmqCache.Core.Msmq;

namespace MsmqCache.Server
{
    sealed class CacheStore : IDisposable
    {
        struct Entry { public long LookupId; public long ExpireTicks; }

        readonly MessageQueue _q;      // reads with body
        readonly MessageQueue _meta;   // enumeration / lookup without body
        readonly Dictionary<string, Entry> _idx = new Dictionary<string, Entry>(StringComparer.Ordinal);

        public CacheStore(ServerConfig cfg)
        {
            _q = QueueTools.Ensure(cfg.DataPath, true, cfg.MaxQueueKb);
            var f = _q.MessageReadPropertyFilter;
            f.Body = true; f.Extension = true; f.LookupId = true; f.Id = true;
            _meta = new MessageQueue(cfg.DataPath, QueueAccessMode.SendAndReceive);
            _meta.MessageReadPropertyFilter.ClearAll();
            _meta.MessageReadPropertyFilter.Extension = true;
            _meta.MessageReadPropertyFilter.LookupId = true;
            _meta.MessageReadPropertyFilter.Id = true;
            Rebuild();
        }

        public int Count() { Sweep(); return _idx.Count; }

        // ---------- recovery ----------
        void Rebuild()
        {
            int n = 0, dup = 0, expired = 0;
            var drop = new List<long>();
            long now = DateTime.UtcNow.Ticks;
            using (var en = _meta.GetMessageEnumerator2())
            {
                while (en.MoveNext())
                {
                    Message m = en.Current;
                    byte[] ext = m.Extension;
                    if (ext == null || ext.Length < 8) { Log.Warn("data queue holds a message without extension (lookupId " + m.LookupId + "); ignored"); continue; }
                    long exp = BitConverter.ToInt64(ext, 0);
                    string key = Encoding.UTF8.GetString(ext, 8, ext.Length - 8);
                    if (exp != 0 && exp <= now) { drop.Add(m.LookupId); expired++; continue; }
                    Entry old;
                    if (_idx.TryGetValue(key, out old)) { drop.Add(old.LookupId); dup++; }   // newer message wins (queue is in arrival order)
                    _idx[key] = new Entry { LookupId = m.LookupId, ExpireTicks = exp };
                    n++;
                }
            }
            foreach (long id in drop) RemoveByLookup(id);
            Log.Info("data queue scanned: " + _idx.Count + " live keys, " + expired + " expired and " + dup + " superseded messages removed (" + n + " messages read)");
        }

        // ---------- helpers ----------
        static bool IsNotFound(Exception e) { return e is InvalidOperationException; }

        void RemoveByLookup(long lookupId)
        {
            try { using (_q.ReceiveByLookupId(MessageLookupAction.Current, lookupId, MessageQueueTransactionType.Single)) { } }
            catch (Exception e) when (IsNotFound(e)) { /* already gone (MSMQ expired it) */ }
        }

        bool TryLive(string key, out Entry e)
        {
            if (!_idx.TryGetValue(key, out e)) return false;
            if (e.ExpireTicks != 0 && e.ExpireTicks <= DateTime.UtcNow.Ticks)
            {
                RemoveByLookup(e.LookupId);
                _idx.Remove(key);
                return false;
            }
            return true;
        }

        Message Build(string key, byte[] val, long expireTicks)
        {
            byte[] kb = Encoding.UTF8.GetBytes(key);
            byte[] ext = new byte[8 + kb.Length];
            BitConverter.GetBytes(expireTicks).CopyTo(ext, 0);
            kb.CopyTo(ext, 8);
            var m = new Message
            {
                Label = key.Length > 200 ? key.Substring(0, 200) : key,
                Extension = ext,
                UseDeadLetterQueue = false,
                UseJournalQueue = false,
                AttachSenderId = false
            };
            m.BodyStream = new MemoryStream(val, false);
            if (expireTicks != 0)
            {
                double secs = Math.Ceiling((expireTicks - DateTime.UtcNow.Ticks) / (double)TimeSpan.TicksPerSecond);
                if (secs < 1) secs = 1;
                if (secs > 4294967000d) secs = 4294967000d;
                m.TimeToBeReceived = TimeSpan.FromSeconds(secs);
            }
            return m;
        }

        // ---------- operations ----------
        public byte[] Get(string key)
        {
            Entry e;
            if (!TryLive(key, out e)) return null;
            try
            {
                using (var m = _q.PeekByLookupId(MessageLookupAction.Current, e.LookupId))
                    return QueueTools.ReadAll(m.BodyStream);
            }
            catch (Exception ex) when (IsNotFound(ex)) { _idx.Remove(key); return null; }
        }

        public bool Exists(string key) { Entry e; return TryLive(key, out e); }

        public bool Set(string key, byte[] val, long expireTicks, bool nx, bool xx)
        {
            Entry old;
            bool had = TryLive(key, out old);
            if (nx && had) return false;
            if (xx && !had) return false;
            var msg = Build(key, val, expireTicks);
            using (var tx = new MessageQueueTransaction())
            {
                tx.Begin();
                try
                {
                    _q.Send(msg, tx);
                    if (had)
                    {
                        try { using (_q.ReceiveByLookupId(MessageLookupAction.Current, old.LookupId, tx)) { } }
                        catch (Exception e) when (IsNotFound(e)) { }
                    }
                    tx.Commit();
                }
                catch { tx.Abort(); throw; }
            }
            _idx[key] = new Entry { LookupId = ResolveLookupId(msg.Id), ExpireTicks = expireTicks };
            msg.Dispose();
            return true;
        }

        // The processor is the only writer, so the message we just committed is the last one in the queue.
        long ResolveLookupId(string messageId)
        {
            using (var last = _meta.PeekByLookupId(MessageLookupAction.Last, 0))
            {
                if (last.Id == messageId) return last.LookupId;
            }
            using (var byId = _meta.PeekById(messageId)) return byId.LookupId;   // slower fallback (scan)
        }

        public bool Del(string key)
        {
            Entry e;
            if (!TryLive(key, out e)) return false;
            RemoveByLookup(e.LookupId);
            _idx.Remove(key);
            return true;
        }

        /// <summary>false = key missing. ticks = 0 when the key has no expiry.</summary>
        public bool TryGetExpire(string key, out long ticks)
        {
            Entry e;
            ticks = 0;
            if (!TryLive(key, out e)) return false;
            ticks = e.ExpireTicks;
            return true;
        }

        /// <summary>MSMQ messages are immutable, so changing a TTL re-sends the message (value is copied).</summary>
        public bool SetExpire(string key, long expireTicks)
        {
            byte[] v = Get(key);
            if (v == null) return false;
            return Set(key, v, expireTicks, false, false);
        }

        public List<string> Keys(string pattern)
        {
            var res = new List<string>();
            foreach (var k in new List<string>(_idx.Keys))
            {
                Entry e;
                if (TryLive(k, out e) && Glob.IsMatch(pattern, k)) res.Add(k);
            }
            return res;
        }

        public void Flush() { _q.Purge(); _idx.Clear(); }

        public int Sweep()
        {
            int n = 0;
            foreach (var k in new List<string>(_idx.Keys)) { Entry e; if (!TryLive(k, out e)) n++; }
            return n;
        }

        public void Dispose() { _q.Dispose(); _meta.Dispose(); }
    }
}
