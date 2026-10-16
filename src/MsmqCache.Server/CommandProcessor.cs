// File: src\MsmqCache.Server\CommandProcessor.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: 
// License: MIT

using System;
using System.Collections.Generic;
using System.Messaging;
using System.Text;
using MsmqCache.Core;

namespace MsmqCache.Server
{
    /// <summary>Executes one command (already dequeued from MSMQ) and returns the RESP reply bytes.</summary>
    sealed class CommandProcessor
    {
        readonly CacheStore _s;
        readonly WorkQueues _w;
        readonly ServerConfig _c;
        readonly DateTime _start = DateTime.UtcNow;
        public long Processed;

        public CommandProcessor(CacheStore s, WorkQueues w, ServerConfig c) { _s = s; _w = w; _c = c; }

        static string Str(byte[] b) { return Encoding.UTF8.GetString(b); }
        static byte[] B(string s) { return Encoding.UTF8.GetBytes(s); }
        static long NowTicks() { return DateTime.UtcNow.Ticks; }

        string Key(byte[] b)
        {
            if (b.Length == 0 || b.Length > 1024) throw new ArgumentException("key length must be 1..1024 bytes");
            return Str(b);
        }

        static long Num(byte[] b)
        {
            long v;
            if (!long.TryParse(Encoding.ASCII.GetString(b), out v)) throw new ArgumentException("value is not an integer or out of range");
            return v;
        }

        static byte[] WrongArgs(string cmd) { return Resp.Error("ERR wrong number of arguments for '" + cmd.ToLowerInvariant() + "'"); }

        public byte[] Execute(List<byte[]> a)
        {
            if (a == null || a.Count == 0) return Resp.Error("ERR empty command");
            string cmd = Encoding.ASCII.GetString(a[0]).ToUpperInvariant();
            Processed++;
            try { return Dispatch(cmd, a); }
            catch (ArgumentException e) { return Resp.Error("ERR " + e.Message); }
            catch (MessageQueueException e)
            {
                Log.Error(cmd + " failed: MSMQ " + e.MessageQueueErrorCode + " (0x" + ((int)e.MessageQueueErrorCode).ToString("X8") + "): " + e.Message);
                return Resp.Error("ERR MSMQ " + e.MessageQueueErrorCode + ": " + e.Message);
            }
            catch (Exception e)
            {
                Log.Error(cmd + " failed: " + e);
                return Resp.Error("ERR internal " + e.GetType().Name + ": " + e.Message);
            }
        }

        byte[] Dispatch(string cmd, List<byte[]> a)
        {
            int n = a.Count;
            switch (cmd)
            {
                case "PING": return n == 1 ? Resp.Simple("PONG") : Resp.Bulk(a[1]);
                case "ECHO": return n == 2 ? Resp.Bulk(a[1]) : WrongArgs(cmd);

                case "SET":
                {
                    if (n < 3) return WrongArgs(cmd);
                    string k = Key(a[1]);
                    if (a[2].Length > _c.MaxValueBytes) return Resp.Error("ERR value exceeds max-value-bytes (" + _c.MaxValueBytes + ")");
                    long exp = 0; bool nx = false, xx = false, keep = false;
                    for (int i = 3; i < n; i++)
                    {
                        string o = Encoding.ASCII.GetString(a[i]).ToUpperInvariant();
                        if ((o == "EX" || o == "PX") && i + 1 < n)
                        {
                            long v = Num(a[++i]);
                            if (v <= 0) return Resp.Error("ERR invalid expire time in 'set'");
                            exp = NowTicks() + (o == "EX" ? v * TimeSpan.TicksPerSecond : v * TimeSpan.TicksPerMillisecond);
                        }
                        else if (o == "NX") nx = true;
                        else if (o == "XX") xx = true;
                        else if (o == "KEEPTTL") keep = true;
                        else return Resp.Error("ERR syntax error");
                    }
                    if (nx && xx) return Resp.Error("ERR syntax error");
                    if (keep && exp == 0) { long cur; if (_s.TryGetExpire(k, out cur)) exp = cur; }
                    return _s.Set(k, a[2], exp, nx, xx) ? Resp.Ok : Resp.Nil;
                }
                case "GET": return n == 2 ? Resp.Bulk(_s.Get(Key(a[1]))) : WrongArgs(cmd);
                case "MGET":
                {
                    if (n < 2) return WrongArgs(cmd);
                    var items = new List<byte[]>();
                    for (int i = 1; i < n; i++) items.Add(_s.Get(Key(a[i])));
                    return Resp.Array(items);
                }
                case "MSET":
                {
                    if (n < 3 || n % 2 == 0) return WrongArgs(cmd);
                    for (int i = 1; i < n; i += 2) if (a[i + 1].Length > _c.MaxValueBytes) return Resp.Error("ERR value exceeds max-value-bytes");
                    for (int i = 1; i < n; i += 2) _s.Set(Key(a[i]), a[i + 1], 0, false, false);
                    return Resp.Ok;
                }
                case "DEL":
                {
                    if (n < 2) return WrongArgs(cmd);
                    long c = 0;
                    for (int i = 1; i < n; i++) if (_s.Del(Key(a[i]))) c++;
                    return Resp.Integer(c);
                }
                case "EXISTS":
                {
                    if (n < 2) return WrongArgs(cmd);
                    long c = 0;
                    for (int i = 1; i < n; i++) if (_s.Exists(Key(a[i]))) c++;
                    return Resp.Integer(c);
                }
                case "EXPIRE":
                case "PEXPIRE":
                {
                    if (n != 3) return WrongArgs(cmd);
                    long v = Num(a[2]);
                    long ticks = cmd == "EXPIRE" ? v * TimeSpan.TicksPerSecond : v * TimeSpan.TicksPerMillisecond;
                    if (v <= 0) return Resp.Integer(_s.Del(Key(a[1])) ? 1 : 0);
                    return Resp.Integer(_s.SetExpire(Key(a[1]), NowTicks() + ticks) ? 1 : 0);
                }
                case "PERSIST":
                {
                    if (n != 2) return WrongArgs(cmd);
                    long t;
                    if (!_s.TryGetExpire(Key(a[1]), out t) || t == 0) return Resp.Integer(0);
                    return Resp.Integer(_s.SetExpire(Key(a[1]), 0) ? 1 : 0);
                }
                case "TTL":
                case "PTTL":
                {
                    if (n != 2) return WrongArgs(cmd);
                    long t;
                    if (!_s.TryGetExpire(Key(a[1]), out t)) return Resp.Integer(-2);
                    if (t == 0) return Resp.Integer(-1);
                    long ms = Math.Max(0, (t - NowTicks()) / TimeSpan.TicksPerMillisecond);
                    return Resp.Integer(cmd == "PTTL" ? ms : (ms + 999) / 1000);
                }
                case "INCR": return n == 2 ? IncrBy(a[1], 1) : WrongArgs(cmd);
                case "DECR": return n == 2 ? IncrBy(a[1], -1) : WrongArgs(cmd);
                case "INCRBY": return n == 3 ? IncrBy(a[1], Num(a[2])) : WrongArgs(cmd);
                case "DECRBY": return n == 3 ? IncrBy(a[1], -Num(a[2])) : WrongArgs(cmd);
                case "KEYS":
                {
                    if (n != 2) return WrongArgs(cmd);
                    var l = new List<byte[]>();
                    foreach (var k in _s.Keys(Str(a[1]))) l.Add(B(k));
                    return Resp.Array(l);
                }
                case "DBSIZE": return Resp.Integer(_s.Count());
                case "FLUSHDB":
                case "FLUSHALL": _s.Flush(); return Resp.Ok;

                case "QPUSH":
                {
                    if (n < 3) return WrongArgs(cmd);
                    if (a[2].Length > _c.MaxValueBytes) return Resp.Error("ERR value exceeds max-value-bytes");
                    int prio = 3, ttl = 0;
                    for (int i = 3; i < n; i++)
                    {
                        string o = Encoding.ASCII.GetString(a[i]).ToUpperInvariant();
                        if (o == "PRIORITY" && i + 1 < n) { long p = Num(a[++i]); if (p < 0 || p > 7) return Resp.Error("ERR PRIORITY must be 0..7"); prio = (int)p; }
                        else if (o == "TTL" && i + 1 < n) { long t = Num(a[++i]); if (t < 1 || t > int.MaxValue) return Resp.Error("ERR invalid TTL"); ttl = (int)t; }
                        else return Resp.Error("ERR syntax error");
                    }
                    _w.Push(Str(a[1]), a[2], prio, ttl);
                    return Resp.Ok;
                }
                case "QPOP": return n == 2 ? Resp.Bulk(_w.Pop(Str(a[1]), true)) : WrongArgs(cmd);
                case "QPEEK": return n == 2 ? Resp.Bulk(_w.Pop(Str(a[1]), false)) : WrongArgs(cmd);
                case "QLEN": return n == 2 ? Resp.Integer(_w.Len(Str(a[1]))) : WrongArgs(cmd);
                case "QPURGE": return n == 2 ? Resp.Integer(_w.Purge(Str(a[1])) ? 1 : 0) : WrongArgs(cmd);
                case "QDEL": return n == 2 ? Resp.Integer(_w.Delete(Str(a[1])) ? 1 : 0) : WrongArgs(cmd);
                case "QLIST":
                {
                    var l = new List<byte[]>();
                    foreach (var q in _w.List()) l.Add(B(q));
                    return Resp.Array(l);
                }
                case "INFO": return Resp.Bulk(Info());
            }
            return Resp.Error("ERR unknown command '" + cmd.ToLowerInvariant() + "'");
        }

        byte[] IncrBy(byte[] keyBytes, long delta)
        {
            string k = Key(keyBytes);
            byte[] cur = _s.Get(k);
            long v = cur == null ? 0 : Num(cur);
            try { v = checked(v + delta); } catch (OverflowException) { return Resp.Error("ERR increment or decrement would overflow"); }
            long exp; _s.TryGetExpire(k, out exp);
            _s.Set(k, B(v.ToString()), exp, false, false);
            return Resp.Integer(v);
        }

        string Info()
        {
            var sb = new StringBuilder();
            sb.Append("# Server\r\n");
            sb.Append("mcache_version:1.0.0\r\n");
            sb.Append("uptime_seconds:" + (long)(DateTime.UtcNow - _start).TotalSeconds + "\r\n");
            sb.Append("tcp_enabled:" + (_c.TcpEnabled ? 1 : 0) + "\r\n");
            sb.Append("tcp_port:" + _c.Port + "\r\n");
            sb.Append("# Msmq\r\n");
            sb.Append("ingress_queue:" + _c.IngressPath + "\r\n");
            sb.Append("data_queue:" + _c.DataPath + "\r\n");
            sb.Append("recoverable:" + (_c.Recoverable ? 1 : 0) + "\r\n");
            sb.Append("# Stats\r\n");
            sb.Append("commands_processed:" + Processed + "\r\n");
            sb.Append("keys:" + _s.Count() + "\r\n");
            sb.Append("work_queues:" + _w.List().Count + "\r\n");
            return sb.ToString();
        }
    }
}
