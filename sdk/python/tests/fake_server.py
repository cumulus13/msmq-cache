"""Tiny in-process RESP server that mimics mcache-server's wire behaviour (NOT its MSMQ storage)."""
import fnmatch
import socketserver
import threading
import time


def _read_command(f):
    line = f.readline()
    if not line:
        return None
    line = line.rstrip(b"\r\n")
    if not line.startswith(b"*"):
        return line.split()
    n = int(line[1:])
    args = []
    for _ in range(n):
        h = f.readline().rstrip(b"\r\n")
        assert h.startswith(b"$"), h
        ln = int(h[1:])
        data = f.read(ln + 2)
        assert data.endswith(b"\r\n")
        args.append(data[:-2])
    return args


def _bulk(b):
    return b"$-1\r\n" if b is None else b"$%d\r\n%s\r\n" % (len(b), b)


class Handler(socketserver.StreamRequestHandler):
    def handle(self):
        srv = self.server
        authed = srv.password is None
        while True:
            try:
                args = _read_command(self.rfile)
            except (AssertionError, ValueError):
                self.wfile.write(b"-ERR protocol error\r\n")
                return
            if args is None:
                return
            if not args:
                continue
            cmd = args[0].upper().decode()
            srv.log.append(args)
            if cmd == "AUTH":
                if args[-1].decode() == srv.password:
                    authed = True
                    self.wfile.write(b"+OK\r\n")
                else:
                    self.wfile.write(b"-WRONGPASS invalid password\r\n")
                continue
            if not authed:
                self.wfile.write(b"-NOAUTH Authentication required.\r\n")
                continue
            if cmd == "QUIT":
                self.wfile.write(b"+OK\r\n")
                return
            if cmd == "DROP":          # test hook: close without replying
                return
            if cmd == "SLOW":          # test hook: reply after a delay
                time.sleep(float(args[1]))
                self.wfile.write(b"+DONE\r\n")
                continue
            self.wfile.write(srv.execute(cmd, args))


class FakeServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, password=None):
        super().__init__(("127.0.0.1", 0), Handler)
        self.password = password
        self.data = {}
        self.queues = {}
        self.log = []
        self.lock = threading.Lock()
        self.thread = threading.Thread(target=self.serve_forever, daemon=True)

    @property
    def port(self):
        return self.server_address[1]

    def start(self):
        self.thread.start()
        return self

    def stop(self):
        self.shutdown()
        self.server_close()

    def execute(self, cmd, a):
        with self.lock:
            d = self.data
            if cmd == "PING":
                return b"+PONG\r\n" if len(a) == 1 else _bulk(a[1])
            if cmd == "ECHO":
                return _bulk(a[1])
            if cmd == "SET":
                opts = [x.upper() for x in a[3:]]
                if b"NX" in opts and a[1] in d:
                    return b"$-1\r\n"
                if b"XX" in opts and a[1] not in d:
                    return b"$-1\r\n"
                d[a[1]] = a[2]
                return b"+OK\r\n"
            if cmd == "GET":
                return _bulk(d.get(a[1]))
            if cmd == "MGET":
                items = [_bulk(d.get(k)) for k in a[1:]]
                return b"*%d\r\n%s" % (len(items), b"".join(items))
            if cmd == "MSET":
                for i in range(1, len(a), 2):
                    d[a[i]] = a[i + 1]
                return b"+OK\r\n"
            if cmd == "DEL":
                n = sum(1 for k in a[1:] if d.pop(k, None) is not None)
                return b":%d\r\n" % n
            if cmd == "EXISTS":
                return b":%d\r\n" % sum(1 for k in a[1:] if k in d)
            if cmd in ("INCR", "INCRBY"):
                by = 1 if cmd == "INCR" else int(a[2])
                try:
                    v = int(d.get(a[1], b"0")) + by
                except ValueError:
                    return b"-ERR value is not an integer or out of range\r\n"
                d[a[1]] = b"%d" % v
                return b":%d\r\n" % v
            if cmd == "KEYS":
                ks = [k for k in d if fnmatch.fnmatchcase(k.decode(), a[1].decode())]
                return b"*%d\r\n%s" % (len(ks), b"".join(_bulk(k) for k in ks))
            if cmd == "DBSIZE":
                return b":%d\r\n" % len(d)
            if cmd == "QPUSH":
                self.queues.setdefault(a[1], []).append(a[2])
                return b"+OK\r\n"
            if cmd == "QPOP":
                q = self.queues.get(a[1])
                return _bulk(q.pop(0) if q else None)
            if cmd == "EXPIRE":
                return b":%d\r\n" % (1 if a[1] in d else 0)
            if cmd == "INFO":
                return _bulk(b"# Server\r\nmcache_version:1.0.0\r\n# Stats\r\nkeys:%d\r\n" % len(d))
            return b"-ERR unknown command '%s'\r\n" % cmd.lower().encode()
