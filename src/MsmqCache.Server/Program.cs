// File: src\MsmqCache.Server\Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: mcache-server - MSMQ-backed cache server. Console when run interactively, Windows Service otherwise.
// License: MIT

// mcache-server - MSMQ-backed cache server. Console when run interactively, Windows Service otherwise.

using System;
using System.IO;
using System.ServiceProcess;
using System.Threading;
using MsmqCache.Core;

namespace MsmqCache.Server
{
    sealed class ServerHost : IDisposable
    {
        readonly ServerConfig _c;
        CacheStore _store; WorkQueues _work; Engine _engine; TcpGateway _tcp;

        public ServerHost(ServerConfig c) { _c = c; }

        public void Start()
        {
            Log.Init(_c.LogLevel, _c.LogFile);
            Log.Info("mcache-server 1.0.0 starting (prefix '" + _c.QueuePrefix + "', recoverable=" + _c.Recoverable + ")");
            _store = new CacheStore(_c);
            _work = new WorkQueues(_c);
            var proc = new CommandProcessor(_store, _work, _c);
            _engine = new Engine(_c, proc, _store);
            _engine.Start();                       // creates the ingress queue first
            if (_c.TcpEnabled) { _tcp = new TcpGateway(_c); _tcp.Start(); }
            else Log.Info("TCP gateway disabled; only MSMQ/COM clients can reach the ingress queue");
        }

        public void Dispose()
        {
            Log.Info("stopping");
            if (_tcp != null) _tcp.Dispose();
            if (_engine != null) _engine.Dispose();
            if (_work != null) _work.Dispose();
            if (_store != null) _store.Dispose();
        }
    }

    sealed class McacheService : ServiceBase
    {
        readonly ServerConfig _c; ServerHost _h;
        public McacheService(ServerConfig c) { _c = c; ServiceName = "MsmqCache"; }
        protected override void OnStart(string[] args) { _h = new ServerHost(_c); _h.Start(); }
        protected override void OnStop() { if (_h != null) _h.Dispose(); }
    }

    static class Program
    {
        static int Main(string[] args)
        {
            string cfgPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "mcache.conf");
            bool check = false;
            for (int i = 0; i < args.Length; i++)
            {
                switch (args[i])
                {
                    case "--config": if (i + 1 >= args.Length) { Console.Error.WriteLine("--config needs a path"); return 2; } cfgPath = args[++i]; break;
                    case "--check-config": check = true; break;
                    case "--version": Console.WriteLine("mcache-server 1.0.0"); return 0;
                    default:
                        Console.WriteLine("usage: mcache-server [--config <file>] [--check-config] [--version]");
                        return args[i] == "--help" ? 0 : 2;
                }
            }

            ServerConfig cfg;
            try { cfg = ServerConfig.Load(cfgPath); }
            catch (ConfigException e) { Console.Error.WriteLine("config error: " + e.Message); return 1; }
            if (check) { Console.WriteLine("config OK: " + cfgPath); return 0; }

            try
            {
                if (!Environment.UserInteractive) { ServiceBase.Run(new McacheService(cfg)); return 0; }
                using (var host = new ServerHost(cfg))
                {
                    var quit = new ManualResetEvent(false);
                    Console.CancelKeyPress += (s, e) => { e.Cancel = true; quit.Set(); };
                    host.Start();
                    Console.WriteLine("running; press Ctrl+C to stop");
                    quit.WaitOne();
                }
                return 0;
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("FATAL: " + e);
                return 3;
            }
        }
    }
}
