// File: src\MsmqCache.Core\Resp.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: MsmqCache.Core - RESP-style wire/queue protocol (the SAME bytes travel over TCP and
//              inside MSMQ message bodies).
// License: MIT

// MsmqCache.Core - RESP-style wire/queue protocol (the SAME bytes travel over TCP and inside MSMQ message bodies).

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace MsmqCache.Core
{
    public sealed class ProtocolException : Exception
    {
        public ProtocolException(string message) : base(message) { }
    }

    /// <summary>A decoded server reply.</summary>
    public sealed class RespValue
    {
        public char Type;                 // + simple, - error, : integer, $ bulk, * array
        public byte[] Bytes;              // + - $ ; null for nil
        public long Integer;              // :
        public List<RespValue> Items;     // *
        public bool IsNil => (Type == '$' && Bytes == null) || (Type == '*' && Items == null);
        public bool IsError => Type == '-';

        public string AsString() => Bytes == null ? null : Encoding.UTF8.GetString(Bytes);

        /// <summary>Human readable form similar to redis-cli.</summary>
        public string Format(int indent = 0)
        {
            string pad = new string(' ', indent);
            switch (Type)
            {
                case '+': return AsString();
                case '-': return "(error) " + AsString();
                case ':': return "(integer) " + Integer;
                case '$': return Bytes == null ? "(nil)" : "\"" + AsString() + "\"";
                case '*':
                    if (Items == null) return "(nil)";
                    if (Items.Count == 0) return "(empty array)";
                    var sb = new StringBuilder();
                    for (int i = 0; i < Items.Count; i++)
                    {
                        if (i > 0) sb.Append('\n').Append(pad);
                        sb.Append(i + 1).Append(") ").Append(Items[i].Format(indent + 3));
                    }
                    return sb.ToString();
            }
            return "?";
        }
    }

    public static class Resp
    {
        static readonly byte[] Crlf = { 13, 10 };

        public static byte[] EncodeCommand(IList<byte[]> args)
        {
            using (var ms = new MemoryStream())
            {
                Write(ms, "*" + args.Count + "\r\n");
                foreach (var a in args)
                {
                    Write(ms, "$" + a.Length + "\r\n");
                    ms.Write(a, 0, a.Length);
                    ms.Write(Crlf, 0, 2);
                }
                return ms.ToArray();
            }
        }

        public static byte[] EncodeCommand(params string[] args)
        {
            var l = new List<byte[]>(args.Length);
            foreach (var a in args) l.Add(Encoding.UTF8.GetBytes(a));
            return EncodeCommand(l);
        }

        public static byte[] Simple(string s) => Encoding.UTF8.GetBytes("+" + Clean(s) + "\r\n");
        public static byte[] Error(string s) => Encoding.UTF8.GetBytes("-" + Clean(s) + "\r\n");
        public static byte[] Integer(long n) => Encoding.ASCII.GetBytes(":" + n + "\r\n");
        public static readonly byte[] Ok = Simple("OK");
        public static readonly byte[] Nil = Encoding.ASCII.GetBytes("$-1\r\n");

        public static byte[] Bulk(byte[] data)
        {
            if (data == null) return Nil;
            using (var ms = new MemoryStream())
            {
                Write(ms, "$" + data.Length + "\r\n");
                ms.Write(data, 0, data.Length);
                ms.Write(Crlf, 0, 2);
                return ms.ToArray();
            }
        }

        public static byte[] Bulk(string s) => Bulk(s == null ? null : Encoding.UTF8.GetBytes(s));

        public static byte[] Array(IList<byte[]> items)
        {
            using (var ms = new MemoryStream())
            {
                Write(ms, "*" + items.Count + "\r\n");
                foreach (var i in items) { var b = Bulk(i); ms.Write(b, 0, b.Length); }
                return ms.ToArray();
            }
        }

        static string Clean(string s) => (s ?? "").Replace("\r", " ").Replace("\n", " ");
        static void Write(Stream s, string text) { var b = Encoding.UTF8.GetBytes(text); s.Write(b, 0, b.Length); }
    }

    /// <summary>Incremental RESP reader. Accepts RESP arrays and plain "inline" commands (telnet friendly).</summary>
    public sealed class RespReader
    {
        const int MaxLine = 64 * 1024;
        const int MaxArgs = 1024 * 1024;
        readonly Stream _s;
        readonly byte[] _buf = new byte[8192];
        readonly int _maxBulk;
        int _pos, _len;

        public RespReader(Stream s, int maxBulkBytes = 4 * 1000 * 1000) { _s = s; _maxBulk = maxBulkBytes; }

        async Task<int> ReadByteAsync(CancellationToken ct)
        {
            if (_pos >= _len)
            {
                _pos = 0;
                _len = await _s.ReadAsync(_buf, 0, _buf.Length, ct).ConfigureAwait(false);
                if (_len == 0) return -1;
            }
            return _buf[_pos++];
        }

        async Task<string> ReadLineAsync(CancellationToken ct)
        {
            var sb = new StringBuilder();
            int b;
            while ((b = await ReadByteAsync(ct).ConfigureAwait(false)) >= 0)
            {
                if (b == '\n') { if (sb.Length > 0 && sb[sb.Length - 1] == '\r') sb.Length--; return sb.ToString(); }
                sb.Append((char)b);
                if (sb.Length > MaxLine) throw new ProtocolException("line too long");
            }
            return sb.Length == 0 ? null : sb.ToString();
        }

        async Task<byte[]> ReadExactAsync(int n, CancellationToken ct)
        {
            var data = new byte[n];
            int got = 0;
            while (got < n)
            {
                if (_pos >= _len)
                {
                    _pos = 0;
                    _len = await _s.ReadAsync(_buf, 0, _buf.Length, ct).ConfigureAwait(false);
                    if (_len == 0) throw new EndOfStreamException("connection closed mid-frame");
                }
                int take = Math.Min(n - got, _len - _pos);
                Buffer.BlockCopy(_buf, _pos, data, got, take);
                _pos += take; got += take;
            }
            return data;
        }

        static int ParseInt(string s, string what)
        {
            int v;
            if (!int.TryParse(s, out v)) throw new ProtocolException("invalid " + what + ": '" + s + "'");
            return v;
        }

        /// <summary>Reads one command. Returns null on clean EOF, an empty list for a blank line.</summary>
        public async Task<List<byte[]>> ReadCommandAsync(CancellationToken ct)
        {
            string line = await ReadLineAsync(ct).ConfigureAwait(false);
            if (line == null) return null;
            if (line.Length == 0) return new List<byte[]>();
            if (line[0] != '*')
            {
                var l = new List<byte[]>();
                foreach (var t in ArgSplitter.Split(line)) l.Add(Encoding.UTF8.GetBytes(t));
                return l;
            }
            int n = ParseInt(line.Substring(1), "array length");
            if (n < 0 || n > MaxArgs) throw new ProtocolException("invalid array length " + n);
            var args = new List<byte[]>(n);
            for (int i = 0; i < n; i++)
            {
                string h = await ReadLineAsync(ct).ConfigureAwait(false);
                if (h == null || h.Length == 0 || h[0] != '$') throw new ProtocolException("expected bulk string header, got '" + h + "'");
                int len = ParseInt(h.Substring(1), "bulk length");
                if (len < 0 || len > _maxBulk) throw new ProtocolException("bulk length " + len + " exceeds limit " + _maxBulk);
                args.Add(await ReadExactAsync(len, ct).ConfigureAwait(false));
                await ReadExactAsync(2, ct).ConfigureAwait(false);
            }
            return args;
        }

        /// <summary>Reads one reply value (client side).</summary>
        public async Task<RespValue> ReadValueAsync(CancellationToken ct)
        {
            string line = await ReadLineAsync(ct).ConfigureAwait(false);
            if (line == null) throw new EndOfStreamException("connection closed");
            if (line.Length == 0) throw new ProtocolException("empty reply line");
            var v = new RespValue { Type = line[0] };
            string rest = line.Substring(1);
            switch (v.Type)
            {
                case '+': case '-': v.Bytes = Encoding.UTF8.GetBytes(rest); return v;
                case ':': v.Integer = long.Parse(rest); return v;
                case '$':
                {
                    int len = ParseInt(rest, "bulk length");
                    if (len >= 0)
                    {
                        if (len > _maxBulk) throw new ProtocolException("bulk reply too large");
                        v.Bytes = await ReadExactAsync(len, ct).ConfigureAwait(false);
                        await ReadExactAsync(2, ct).ConfigureAwait(false);
                    }
                    return v;
                }
                case '*':
                {
                    int n = ParseInt(rest, "array length");
                    if (n >= 0)
                    {
                        v.Items = new List<RespValue>(n);
                        for (int i = 0; i < n; i++) v.Items.Add(await ReadValueAsync(ct).ConfigureAwait(false));
                    }
                    return v;
                }
            }
            throw new ProtocolException("unknown reply type '" + v.Type + "'");
        }

        public static RespValue ParseReply(byte[] data)
        {
            return new RespReader(new MemoryStream(data)).ReadValueAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        public static List<byte[]> ParseCommand(byte[] data)
        {
            return new RespReader(new MemoryStream(data)).ReadCommandAsync(CancellationToken.None).GetAwaiter().GetResult();
        }
    }

    /// <summary>Shell-like splitting: whitespace separated, "double" and 'single' quotes, backslash escapes inside double quotes.</summary>
    public static class ArgSplitter
    {
        public static List<string> Split(string line)
        {
            var res = new List<string>();
            var sb = new StringBuilder();
            bool have = false;
            char q = '\0';
            for (int i = 0; i < line.Length; i++)
            {
                char c = line[i];
                if (q == '\0')
                {
                    if (char.IsWhiteSpace(c)) { if (have) { res.Add(sb.ToString()); sb.Clear(); have = false; } }
                    else if (c == '"' || c == '\'') { q = c; have = true; }
                    else { sb.Append(c); have = true; }
                }
                else if (q == '"' && c == '\\' && i + 1 < line.Length)
                {
                    char n = line[++i];
                    sb.Append(n == 'n' ? '\n' : n == 'r' ? '\r' : n == 't' ? '\t' : n);
                }
                else if (c == q) q = '\0';
                else sb.Append(c);
            }
            if (q != '\0') throw new ProtocolException("unbalanced quotes");
            if (have) res.Add(sb.ToString());
            return res;
        }
    }

    public static class Glob
    {
        /// <summary>Redis-style glob: * ? [abc] [a-z] and \ escape.</summary>
        public static bool IsMatch(string pattern, string text)
        {
            return Match(pattern, 0, text, 0);
        }

        static bool Match(string p, int pi, string t, int ti)
        {
            while (pi < p.Length)
            {
                char c = p[pi];
                if (c == '*')
                {
                    while (pi < p.Length && p[pi] == '*') pi++;
                    if (pi == p.Length) return true;
                    for (int k = ti; k <= t.Length; k++) if (Match(p, pi, t, k)) return true;
                    return false;
                }
                if (ti >= t.Length) return false;
                if (c == '?') { pi++; ti++; }
                else if (c == '[')
                {
                    int end = p.IndexOf(']', pi + 1);
                    if (end < 0) { if (t[ti] != '[') return false; pi++; ti++; continue; }
                    bool neg = p[pi + 1] == '^', ok = false;
                    for (int j = pi + 1 + (neg ? 1 : 0); j < end; j++)
                    {
                        if (j + 2 < end && p[j + 1] == '-') { if (t[ti] >= p[j] && t[ti] <= p[j + 2]) ok = true; j += 2; }
                        else if (p[j] == t[ti]) ok = true;
                    }
                    if (ok == neg) return false;
                    pi = end + 1; ti++;
                }
                else
                {
                    if (c == '\\' && pi + 1 < p.Length) { pi++; c = p[pi]; }
                    if (c != t[ti]) return false;
                    pi++; ti++;
                }
            }
            return ti == t.Length;
        }
    }
}
