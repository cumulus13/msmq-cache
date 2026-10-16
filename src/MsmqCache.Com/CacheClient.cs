// COM automation object: ProgID "MsmqCache.Client". Talks to the server through MSMQ (no TCP involved):
// every call becomes a message in the server's ingress queue; the reply is collected from a local reply queue.
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;
using MsmqCache.Core;
using MsmqCache.Core.Msmq;

namespace MsmqCache.Com
{
    [ComVisible(true)]
    [Guid("6C1F6A4E-3B7D-4E0B-9A52-0D5E7C1B2F11")]
    [InterfaceType(ComInterfaceType.InterfaceIsDual)]
    public interface ICacheClient
    {
        /// <summary>host: "." or machine name running mcache-server. prefix: queue-prefix from mcache.conf (default "mcache").</summary>
        void Connect(string host, string prefix);
        int TimeoutMs { get; set; }
        /// <summary>Runs a command line, e.g. Execute("SET greeting \"hello world\" EX 60"). Returns Empty for nil.</summary>
        object Execute(string commandLine);
        /// <summary>Fire and forget (no reply is requested from the server).</summary>
        void Post(string commandLine);
        void Set(string key, string value, int ttlSeconds);
        object Get(string key);
        int Del(string key);
        int Exists(string key);
        object Incr(string key);
        object Ttl(string key);
        void Close();
    }

    [ComVisible(true)]
    [Guid("9B2E41D8-5A6C-4F37-8E1D-72A0C3B94E25")]
    [ClassInterface(ClassInterfaceType.None)]
    [ProgId("MsmqCache.Client")]
    public sealed class CacheClient : ICacheClient, IDisposable
    {
        MsmqRequester _r;
        string _host = ".", _prefix = "mcache";

        public int TimeoutMs { get; set; } = 5000;

        public void Connect(string host, string prefix)
        {
            Close();
            _host = string.IsNullOrEmpty(host) ? "." : host;
            _prefix = string.IsNullOrEmpty(prefix) ? "mcache" : prefix;
            _r = new MsmqRequester(_host, _prefix);
        }

        MsmqRequester R() { if (_r == null) Connect(_host, _prefix); return _r; }

        static List<byte[]> Args(params string[] a)
        {
            var l = new List<byte[]>();
            foreach (var s in a) l.Add(Encoding.UTF8.GetBytes(s));
            return l;
        }

        object Run(List<byte[]> args)
        {
            byte[] reply;
            try { reply = R().Request(args, TimeoutMs); }
            catch (TimeoutException e) { throw new COMException(e.Message, unchecked((int)0x800705B4)); } // ERROR_TIMEOUT
            return Convert(RespReader.ParseReply(reply));
        }

        static object Convert(RespValue v)
        {
            switch (v.Type)
            {
                case '-': throw new COMException(v.AsString());
                case '+': return v.AsString();
                case ':': return v.Integer >= int.MinValue && v.Integer <= int.MaxValue ? (object)(int)v.Integer : v.Integer.ToString();
                case '$': return v.Bytes == null ? null : v.AsString();
                case '*':
                    if (v.Items == null) return null;
                    var arr = new object[v.Items.Count];
                    for (int i = 0; i < arr.Length; i++) arr[i] = Convert(v.Items[i]);
                    return arr;
            }
            return null;
        }

        public object Execute(string commandLine)
        {
            var parts = ArgSplitter.Split(commandLine);
            if (parts.Count == 0) throw new COMException("empty command");
            return Run(Args(parts.ToArray()));
        }

        public void Post(string commandLine)
        {
            var parts = ArgSplitter.Split(commandLine);
            if (parts.Count == 0) throw new COMException("empty command");
            R().Post(Args(parts.ToArray()));
        }

        public void Set(string key, string value, int ttlSeconds)
        {
            Run(ttlSeconds > 0 ? Args("SET", key, value, "EX", ttlSeconds.ToString()) : Args("SET", key, value));
        }

        public object Get(string key) { return Run(Args("GET", key)); }
        public int Del(string key) { return (int)Run(Args("DEL", key)); }
        public int Exists(string key) { return (int)Run(Args("EXISTS", key)); }
        public object Incr(string key) { return Run(Args("INCR", key)); }
        public object Ttl(string key) { return Run(Args("TTL", key)); }

        public void Close() { if (_r != null) { _r.Dispose(); _r = null; } }
        public void Dispose() { Close(); }
    }
}
