// MsmqCache.Core - redis-style configuration file ("directive value" per line, '#' comments).
using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;

namespace MsmqCache.Core
{
    public sealed class ConfigException : Exception
    {
        public ConfigException(string message) : base(message) { }
    }

    public sealed class ServerConfig
    {
        // network (TCP ingress)
        public bool TcpEnabled = true;
        public string Bind = "127.0.0.1";
        public int Port = 6390;
        public string RequirePass = "";
        public int MaxClients = 1000;
        public int CommandTimeoutMs = 5000;

        // MSMQ
        public string QueuePrefix = "mcache";          // queues: private$\<prefix>.ingress / .data / .reply.tcp / .q.<name>
        public bool Recoverable = true;                // MSMQ disk-backed (survives reboot) for requests and work-queue messages
        public int RequestTtlSeconds = 60;             // MSMQ TimeToBeReceived on commands that nobody consumed
        public int ReplyTtlSeconds = 30;               // MSMQ TimeToBeReceived on replies nobody collected
        public long MaxQueueKb = 0;                    // 0 = MSMQ default quota
        public int MaxValueBytes = 4 * 1000 * 1000;    // MSMQ message limit is 4 MB including properties
        public int SweepIntervalSeconds = 30;
        public List<string> IngressSendAccounts = new List<string>(); // accounts granted WriteMessage on the ingress queue (COM / remote MSMQ senders)

        // logging
        public string LogLevel = "info";               // debug | info | warn | error
        public string LogFile = "";

        public string IngressPath => @".\private$\" + QueuePrefix + ".ingress";
        public string DataPath => @".\private$\" + QueuePrefix + ".data";
        public string TcpReplyPath => @".\private$\" + QueuePrefix + ".reply.tcp";
        public string WorkQueuePrefix => @".\private$\" + QueuePrefix + ".q.";

        public static ServerConfig Load(string path)
        {
            if (!File.Exists(path)) throw new ConfigException("config file not found: " + path);
            return Parse(File.ReadAllLines(path), path);
        }

        public static ServerConfig Parse(IEnumerable<string> lines, string source = "<memory>")
        {
            var c = new ServerConfig();
            int n = 0;
            foreach (var raw in lines)
            {
                n++;
                string line = raw.Trim();
                if (line.Length == 0 || line[0] == '#') continue;
                List<string> t;
                try { t = ArgSplitter.Split(line); }
                catch (ProtocolException e) { throw new ConfigException(source + ":" + n + ": " + e.Message); }
                string key = t[0].ToLowerInvariant();
                try { c.Apply(key, t.GetRange(1, t.Count - 1)); }
                catch (ConfigException e) { throw new ConfigException(source + ":" + n + ": " + e.Message); }
            }
            c.Validate();
            return c;
        }

        void Apply(string key, List<string> v)
        {
            switch (key)
            {
                case "tcp-enabled": TcpEnabled = Bool(key, v); break;
                case "bind": Bind = One(key, v); break;
                case "port": Port = Int(key, v, 1, 65535); break;
                case "requirepass": RequirePass = v.Count == 0 ? "" : One(key, v); break;
                case "maxclients": MaxClients = Int(key, v, 1, 100000); break;
                case "command-timeout-ms": CommandTimeoutMs = Int(key, v, 100, 600000); break;
                case "queue-prefix": QueuePrefix = One(key, v); break;
                case "recoverable": Recoverable = Bool(key, v); break;
                case "request-ttl": RequestTtlSeconds = Int(key, v, 1, int.MaxValue); break;
                case "reply-ttl": ReplyTtlSeconds = Int(key, v, 1, int.MaxValue); break;
                case "max-queue-kb": MaxQueueKb = Int(key, v, 0, int.MaxValue); break;
                case "max-value-bytes": MaxValueBytes = Int(key, v, 1, 4 * 1000 * 1000); break;
                case "sweep-interval": SweepIntervalSeconds = Int(key, v, 1, 86400); break;
                case "ingress-send-account": IngressSendAccounts.Add(One(key, v)); break;
                case "loglevel": LogLevel = One(key, v).ToLowerInvariant(); break;
                case "logfile": LogFile = v.Count == 0 ? "" : One(key, v); break;
                default: throw new ConfigException("unknown directive '" + key + "'");
            }
        }

        void Validate()
        {
            foreach (char ch in QueuePrefix)
                if (!(char.IsLetterOrDigit(ch) || ch == '_' || ch == '-'))
                    throw new ConfigException("queue-prefix may only contain letters, digits, '_' and '-': '" + QueuePrefix + "'");
            if (QueuePrefix.Length == 0 || QueuePrefix.Length > 40) throw new ConfigException("queue-prefix must be 1..40 characters");
            if (LogLevel != "debug" && LogLevel != "info" && LogLevel != "warn" && LogLevel != "error")
                throw new ConfigException("loglevel must be debug|info|warn|error");
        }

        static string One(string k, List<string> v)
        {
            if (v.Count != 1) throw new ConfigException("'" + k + "' expects exactly 1 value");
            return v[0];
        }

        static bool Bool(string k, List<string> v)
        {
            string s = One(k, v).ToLowerInvariant();
            if (s == "yes" || s == "true" || s == "1") return true;
            if (s == "no" || s == "false" || s == "0") return false;
            throw new ConfigException("'" + k + "' expects yes|no");
        }

        static int Int(string k, List<string> v, int min, int max)
        {
            int r;
            if (!int.TryParse(One(k, v), NumberStyles.Integer, CultureInfo.InvariantCulture, out r) || r < min || r > max)
                throw new ConfigException("'" + k + "' expects an integer in [" + min + ", " + max + "]");
            return r;
        }
    }
}
