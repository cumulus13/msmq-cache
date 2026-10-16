using System;
using System.IO;

namespace MsmqCache.Server
{
    static class Log
    {
        static readonly object L = new object();
        static int _level = 1; // 0 debug 1 info 2 warn 3 error
        static string _file = "";

        public static void Init(string level, string file)
        {
            _level = level == "debug" ? 0 : level == "warn" ? 2 : level == "error" ? 3 : 1;
            _file = file ?? "";
        }
        public static void Debug(string m) { Write(0, "DEBUG", m); }
        public static void Info(string m) { Write(1, "INFO ", m); }
        public static void Warn(string m) { Write(2, "WARN ", m); }
        public static void Error(string m) { Write(3, "ERROR", m); }

        static void Write(int lvl, string tag, string m)
        {
            if (lvl < _level) return;
            string line = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + " " + tag + " " + m;
            lock (L)
            {
                Console.WriteLine(line);
                if (_file.Length > 0)
                {
                    try { File.AppendAllText(_file, line + Environment.NewLine); } catch { /* never let logging kill the server */ }
                }
            }
        }
    }
}
