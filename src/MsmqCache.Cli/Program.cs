// File: src\MsmqCache.Cli\Program.cs
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-17
// Description: command line client for msmq-cache
// License: MIT

// mcache-cli - command line client (any OS). Usage: mcache-cli [-h host] [-p port] [-a password] [command args...]

using System;
using System.Threading.Tasks;
using MsmqCache.Client;
using MsmqCache.Core;

static class Program
{
    static async Task<int> Main(string[] args)
    {
        string host = "127.0.0.1", pass = null;
        int port = 6390, i = 0;
        for (; i < args.Length && args[i].StartsWith("-"); i++)
        {
            if (args[i] == "--help") { Console.WriteLine("usage: mcache-cli [-h host] [-p port] [-a password] [command args...]"); return 0; }
            if (i + 1 >= args.Length) { Console.Error.WriteLine("option " + args[i] + " needs a value"); return 2; }
            switch (args[i])
            {
                case "-h": host = args[++i]; break;
                case "-p": if (!int.TryParse(args[++i], out port)) { Console.Error.WriteLine("invalid port"); return 2; } break;
                case "-a": pass = args[++i]; break;
                default: Console.Error.WriteLine("unknown option " + args[i]); return 2;
            }
        }

        CacheClient c;
        try { c = await CacheClient.ConnectAsync(host, port, pass); }
        catch (Exception e) { Console.Error.WriteLine("cannot connect to " + host + ":" + port + ": " + e.Message); return 1; }

        using (c)
        {
            if (i < args.Length)
            {
                var r = await c.ExecuteAsync(args[i..]);
                Console.WriteLine(r.Format());
                return r.IsError ? 1 : 0;
            }
            while (true)
            {
                Console.Write(host + ":" + port + "> ");
                string line = Console.ReadLine();
                if (line == null) break;
                line = line.Trim();
                if (line.Length == 0) continue;
                if (line.Equals("quit", StringComparison.OrdinalIgnoreCase) || line.Equals("exit", StringComparison.OrdinalIgnoreCase)) break;
                try
                {
                    var parts = ArgSplitter.Split(line);
                    var r = await c.ExecuteAsync(parts.ToArray());
                    Console.WriteLine(r.Format());
                }
                catch (Exception e) { Console.WriteLine("(error) " + e.Message); if (e is System.IO.IOException || e is System.IO.EndOfStreamException) return 1; }
            }
        }
        return 0;
    }
}
