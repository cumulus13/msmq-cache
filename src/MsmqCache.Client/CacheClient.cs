// File: src\MsmqCache.Client\CacheClient.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: 
// License: MIT

using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MsmqCache.Core;

namespace MsmqCache.Client
{
    public sealed class CacheException : Exception
    {
        public CacheException(string message) : base(message) { }
    }

    /// <summary>Thread-safe (calls are serialized) TCP client. Server side every call is turned into an MSMQ message.</summary>
    public sealed class CacheClient : IDisposable
    {
        readonly TcpClient _tcp;
        readonly NetworkStream _ns;
        readonly RespReader _reader;
        readonly SemaphoreSlim _gate = new SemaphoreSlim(1, 1);

        CacheClient(TcpClient t, int maxBulk)
        {
            _tcp = t; _ns = t.GetStream(); _reader = new RespReader(_ns, maxBulk);
        }

        public static async Task<CacheClient> ConnectAsync(string host, int port, string password = null, CancellationToken ct = default(CancellationToken))
        {
            var t = new TcpClient { NoDelay = true };
            await t.ConnectAsync(host, port).ConfigureAwait(false);
            var c = new CacheClient(t, 8 * 1000 * 1000);
            if (!string.IsNullOrEmpty(password))
            {
                var r = await c.ExecuteAsync(new[] { "AUTH", password }, ct).ConfigureAwait(false);
                if (r.IsError) { c.Dispose(); throw new CacheException(r.AsString()); }
            }
            return c;
        }

        /// <summary>Raw call; returns the decoded reply, including error replies.</summary>
        public async Task<RespValue> ExecuteAsync(IList<byte[]> args, CancellationToken ct = default(CancellationToken))
        {
            var bytes = Resp.EncodeCommand(args);
            await _gate.WaitAsync(ct).ConfigureAwait(false);
            try
            {
                await _ns.WriteAsync(bytes, 0, bytes.Length, ct).ConfigureAwait(false);
                return await _reader.ReadValueAsync(ct).ConfigureAwait(false);
            }
            finally { _gate.Release(); }
        }

        public Task<RespValue> ExecuteAsync(string[] args, CancellationToken ct = default(CancellationToken))
        {
            var l = new List<byte[]>(args.Length);
            foreach (var a in args) l.Add(Encoding.UTF8.GetBytes(a));
            return ExecuteAsync(l, ct);
        }

        async Task<RespValue> Ok(params string[] args)
        {
            var r = await ExecuteAsync(args).ConfigureAwait(false);
            if (r.IsError) throw new CacheException(r.AsString());
            return r;
        }

        public async Task<bool> SetAsync(string key, string value, TimeSpan? ttl = null)
        {
            var r = ttl.HasValue
                ? await Ok("SET", key, value, "PX", ((long)ttl.Value.TotalMilliseconds).ToString()).ConfigureAwait(false)
                : await Ok("SET", key, value).ConfigureAwait(false);
            return !r.IsNil;
        }

        public async Task<string> GetAsync(string key) { return (await Ok("GET", key).ConfigureAwait(false)).AsString(); }
        public async Task<long> DelAsync(string key) { return (await Ok("DEL", key).ConfigureAwait(false)).Integer; }
        public async Task<long> IncrAsync(string key) { return (await Ok("INCR", key).ConfigureAwait(false)).Integer; }
        public async Task<string> PingAsync() { return (await Ok("PING").ConfigureAwait(false)).AsString(); }
        public Task QPushAsync(string queue, string value) { return Ok("QPUSH", queue, value); }
        public async Task<string> QPopAsync(string queue) { return (await Ok("QPOP", queue).ConfigureAwait(false)).AsString(); }

        public void Dispose() { _ns.Dispose(); _tcp.Close(); _gate.Dispose(); }
    }
}
