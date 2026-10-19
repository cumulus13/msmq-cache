package com.cumulus13.msmqcache;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Zero-dependency blocking client for mcache-server (MSMQ-backed cache / queue server). Thread-safe: calls are
 * serialised over one connection. A command that fails with a connection error or timeout is never retried
 * (the server is at-most-once); the connection is closed and re-opened by the next call.
 *
 * Reply mapping for {@link #execute}: String (status), Long, byte[] (bulk), null (nil), List&lt;Object&gt;;
 * server errors are thrown as {@link ResponseException}.
 */
public final class MsmqCacheClient implements AutoCloseable {

    /** Connection settings. */
    public static final class Options {
        public String host = "127.0.0.1";
        public int port = 6390;
        public String password;
        public int timeoutMs = 10_000;
        public int connectTimeoutMs = 5_000;
        public int maxValueBytes = 4_000_000;
        public Options host(String v) { host = v; return this; }
        public Options port(int v) { port = v; return this; }
        public Options password(String v) { password = v; return this; }
        public Options timeoutMs(int v) { timeoutMs = v; return this; }
    }

    /** SET options: ex = seconds, px = milliseconds. */
    public static final class SetOptions {
        long ex, px; boolean nx, xx, keepTtl;
        public SetOptions ex(long s) { ex = s; return this; }
        public SetOptions px(long ms) { px = ms; return this; }
        public SetOptions nx() { nx = true; return this; }
        public SetOptions xx() { xx = true; return this; }
        public SetOptions keepTtl() { keepTtl = true; return this; }
    }

    /** QPUSH options: priority 0..7 (7 highest, -1 = default), ttlSeconds 0 = none. */
    public static final class QPushOptions {
        int priority = -1; long ttl;
        public QPushOptions priority(int p) { priority = p; return this; }
        public QPushOptions ttl(long seconds) { ttl = seconds; return this; }
    }

    private static final class ErrorReply { final String message; ErrorReply(String m) { message = m; } }

    private final Options o;
    private Socket sock;
    private InputStream in;
    private OutputStream out;

    public MsmqCacheClient(Options o) { this.o = o; }
    public MsmqCacheClient(String host, int port) { this(new Options().host(host).port(port)); }

    // ------------------------------------------------------------------ connection / wire
    private void ensure() {
        if (sock != null) return;
        Socket s = new Socket();
        try {
            s.connect(new InetSocketAddress(o.host, o.port), o.connectTimeoutMs);
            s.setTcpNoDelay(true);
            s.setSoTimeout(o.timeoutMs);
        } catch (SocketTimeoutException e) {
            closeQuietly(s);
            throw new CacheTimeoutException("connect to " + o.host + ":" + o.port + " timed out", e);
        } catch (IOException e) {
            closeQuietly(s);
            throw new CacheConnectionException("cannot connect to " + o.host + ":" + o.port + ": " + e.getMessage(), e);
        }
        try {
            sock = s; in = new BufferedInputStream(s.getInputStream()); out = s.getOutputStream();
        } catch (IOException e) {
            close();
            throw new CacheConnectionException("connection error: " + e.getMessage(), e);
        }
        if (o.password != null && !o.password.isEmpty()) {
            Object r;
            try { r = roundtrip(single(args("AUTH", o.password))).get(0); } catch (RuntimeException e) { close(); throw e; }
            if (r instanceof ErrorReply) { close(); throw serverError(((ErrorReply) r).message); }
        }
    }

    private static List<byte[][]> single(byte[][] cmd) {
        List<byte[][]> l = new ArrayList<>(1);
        l.add(cmd);
        return l;
    }

    private static void closeQuietly(Socket s) { try { s.close(); } catch (IOException ignored) { } }

    @Override public synchronized void close() {
        if (sock != null) { closeQuietly(sock); sock = null; in = null; out = null; }
    }

    private List<Object> roundtrip(List<byte[][]> cmds) {
        try {
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            for (byte[][] c : cmds) {
                buf.write(("*" + c.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
                for (byte[] a : c) {
                    buf.write(("$" + a.length + "\r\n").getBytes(StandardCharsets.US_ASCII));
                    buf.write(a); buf.write('\r'); buf.write('\n');
                }
            }
            out.write(buf.toByteArray());
            out.flush();
            List<Object> replies = new ArrayList<>(cmds.size());
            for (int i = 0; i < cmds.size(); i++) replies.add(readReply());
            return replies;
        } catch (SocketTimeoutException e) {
            close();
            throw new CacheTimeoutException("no reply within " + o.timeoutMs + " ms; connection closed, the command may still be executed", e);
        } catch (IOException e) {
            close();
            throw new CacheConnectionException("connection lost: " + e.getMessage(), e);
        } catch (ProtocolException e) {
            close();
            throw e;
        }
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        int c;
        while ((c = in.read()) != -1) {
            if (c == '\n') {
                byte[] raw = b.toByteArray();
                if (raw.length == 0 || raw[raw.length - 1] != '\r') throw new ProtocolException("line not terminated by CRLF");
                return new String(raw, 0, raw.length - 1, StandardCharsets.UTF_8);
            }
            b.write(c);
            if (b.size() > 64 * 1024) throw new ProtocolException("line too long");
        }
        throw new EOFException("connection closed by server");
    }

    private static long num(String s, String what) {
        try { return Long.parseLong(s); } catch (NumberFormatException e) { throw new ProtocolException("invalid " + what + ": " + s); }
    }

    private Object readReply() throws IOException {
        String line = readLine();
        if (line.isEmpty()) throw new ProtocolException("empty reply line");
        String rest = line.substring(1);
        switch (line.charAt(0)) {
            case '+': return rest;
            case '-': return new ErrorReply(rest);
            case ':': return num(rest, "integer");
            case '$': {
                long n = num(rest, "bulk length");
                if (n < 0) return null;
                byte[] data = in.readNBytes((int) n);
                byte[] crlf = in.readNBytes(2);
                if (data.length != n || crlf.length != 2) throw new EOFException("connection closed mid-reply");
                return data;
            }
            case '*': {
                long n = num(rest, "array length");
                if (n < 0) return null;
                List<Object> items = new ArrayList<>();
                for (long i = 0; i < n; i++) items.add(readReply());
                return items;
            }
            default: throw new ProtocolException("unknown reply type '" + line.charAt(0) + "'");
        }
    }

    private static MsmqCacheException serverError(String m) {
        return (m.startsWith("NOAUTH") || m.startsWith("WRONGPASS")) ? new AuthenticationException(m) : new ResponseException(m);
    }

    private synchronized List<Object> exec(List<byte[][]> cmds) {
        ensure();
        return roundtrip(cmds);
    }

    private static byte[] bytes(Object v) {
        if (v instanceof byte[]) return (byte[]) v;
        if (v instanceof String) return ((String) v).getBytes(StandardCharsets.UTF_8);
        if (v instanceof Long || v instanceof Integer || v instanceof Short) return v.toString().getBytes(StandardCharsets.US_ASCII);
        throw new IllegalArgumentException("unsupported argument type " + (v == null ? "null" : v.getClass().getName()));
    }

    private static byte[][] args(Object... a) {
        byte[][] r = new byte[a.length][];
        for (int i = 0; i < a.length; i++) r[i] = bytes(a[i]);
        return r;
    }

    private byte[] key(Object k) {
        byte[] b = bytes(k);
        if (b.length < 1 || b.length > 1024) throw new IllegalArgumentException("key length must be 1..1024 bytes");
        return b;
    }

    private byte[] val(Object v) {
        byte[] b = bytes(v);
        if (b.length > o.maxValueBytes) throw new IllegalArgumentException("value is " + b.length + " bytes; limit is " + o.maxValueBytes);
        return b;
    }

    private Object one(byte[][] cmd) {
        Object r = exec(single(cmd)).get(0);
        if (r instanceof ErrorReply) throw serverError(((ErrorReply) r).message);
        return r;
    }

    private static long asLong(Object r) {
        if (r instanceof Long) return (Long) r;
        throw new ProtocolException("expected integer reply, got " + r);
    }

    private static byte[] asBytes(Object r) {
        if (r == null) return null;
        if (r instanceof byte[]) return (byte[]) r;
        throw new ProtocolException("expected bulk reply, got " + r);
    }

    @SuppressWarnings("unchecked")
    private static List<byte[]> asList(Object r) {
        if (!(r instanceof List)) throw new ProtocolException("expected array reply, got " + r);
        List<byte[]> out = new ArrayList<>();
        for (Object o : (List<Object>) r) out.add(asBytes(o));
        return out;
    }

    // ------------------------------------------------------------------ commands
    /** Run any command, e.g. {@code execute("INFO")}. */
    public Object execute(Object... args) { return one(args(args)); }

    public boolean ping() { return "PONG".equals(one(args("PING"))); }

    private List<Object> setArgs(Object key, Object value, SetOptions o) {
        if (o.nx && o.xx) throw new IllegalArgumentException("nx and xx are mutually exclusive");
        if (o.ex != 0 && o.px != 0) throw new IllegalArgumentException("use either ex or px");
        if (o.ex < 0 || o.px < 0) throw new IllegalArgumentException("ex/px must be positive");
        List<Object> a = new ArrayList<>(Arrays.asList("SET", key(key), val(value)));
        if (o.ex > 0) { a.add("EX"); a.add(o.ex); }
        if (o.px > 0) { a.add("PX"); a.add(o.px); }
        if (o.nx) a.add("NX");
        if (o.xx) a.add("XX");
        if (o.keepTtl) a.add("KEEPTTL");
        return a;
    }

    /** true = stored, false = NX/XX prevented it. */
    public boolean set(Object key, Object value, SetOptions o) { return one(args(setArgs(key, value, o).toArray())) != null; }
    public boolean set(Object key, Object value) { return set(key, value, new SetOptions()); }

    /** null for a missing key; an empty value is an empty array. */
    public byte[] get(Object key) { return asBytes(one(args("GET", key(key)))); }
    public String getString(Object key) { byte[] b = get(key); return b == null ? null : new String(b, StandardCharsets.UTF_8); }

    public List<byte[]> mget(Object... keys) {
        Object[] a = new Object[keys.length + 1];
        a[0] = "MGET";
        for (int i = 0; i < keys.length; i++) a[i + 1] = key(keys[i]);
        return asList(one(args(a)));
    }

    /** Not atomic on the server (one MSMQ message per key). */
    public void mset(Map<String, ?> entries) {
        List<Object> a = new ArrayList<>(); a.add("MSET");
        for (Map.Entry<String, ?> e : entries.entrySet()) { a.add(key(e.getKey())); a.add(val(e.getValue())); }
        if (a.size() == 1) throw new IllegalArgumentException("entries must not be empty");
        one(args(a.toArray()));
    }

    private long keysCmd(String cmd, Object[] keys) {
        Object[] a = new Object[keys.length + 1];
        a[0] = cmd;
        for (int i = 0; i < keys.length; i++) a[i + 1] = key(keys[i]);
        return asLong(one(args(a)));
    }
    public long del(Object... keys) { return keysCmd("DEL", keys); }
    public long exists(Object... keys) { return keysCmd("EXISTS", keys); }

    /** Re-sends the value on the server (MSMQ messages are immutable). */
    public boolean expire(Object key, long seconds) {
        if (seconds <= 0) throw new IllegalArgumentException("seconds must be positive");
        return asLong(one(args("EXPIRE", key(key), seconds))) == 1;
    }
    /** Seconds left; -1 no expiry; -2 no such key. */
    public long ttl(Object key) { return asLong(one(args("TTL", key(key)))); }
    public long incr(Object key) { return asLong(one(args("INCR", key(key)))); }
    public long decr(Object key) { return asLong(one(args("DECR", key(key)))); }
    public long incrBy(Object key, long n) { return asLong(one(args("INCRBY", key(key), n))); }
    /** O(n) on the server. */
    public List<byte[]> keys(String pattern) { return asList(one(args("KEYS", pattern))); }
    public long dbsize() { return asLong(one(args("DBSIZE"))); }
    public void flushdb() { one(args("FLUSHDB")); }

    private Object[] qpushArgs(String queue, Object value, QPushOptions o) {
        if (o.priority < -1 || o.priority > 7) throw new IllegalArgumentException("priority must be 0..7");
        if (o.ttl < 0) throw new IllegalArgumentException("ttl must be positive");
        List<Object> a = new ArrayList<>(Arrays.asList("QPUSH", queue, val(value)));
        if (o.priority >= 0) { a.add("PRIORITY"); a.add((long) o.priority); }
        if (o.ttl > 0) { a.add("TTL"); a.add(o.ttl); }
        return a.toArray();
    }
    /** Native MSMQ queue (auto-created). */
    public void qpush(String queue, Object value, QPushOptions o) { one(args(qpushArgs(queue, value, o))); }
    public void qpush(String queue, Object value) { qpush(queue, value, new QPushOptions()); }
    /** Non-blocking: null when empty. */
    public byte[] qpop(String queue) { return asBytes(one(args("QPOP", queue))); }
    public byte[] qpeek(String queue) { return asBytes(one(args("QPEEK", queue))); }
    public long qlen(String queue) { return asLong(one(args("QLEN", queue))); }
    public List<byte[]> qlist() { return asList(one(args("QLIST"))); }

    public Map<String, Map<String, String>> info() {
        String text = new String(asBytes(one(args("INFO"))), StandardCharsets.UTF_8);
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        String section = "default";
        for (String raw : text.split("\n")) {
            String l = raw.trim();
            if (l.isEmpty()) continue;
            if (l.startsWith("#")) { section = l.replaceFirst("^#+\\s*", "").toLowerCase(); continue; }
            int i = l.indexOf(':');
            out.computeIfAbsent(section, k -> new LinkedHashMap<>()).put(l.substring(0, i), l.substring(i + 1));
        }
        return out;
    }

    // ------------------------------------------------------------------ pipeline
    public Pipeline pipeline() { return new Pipeline(); }

    /** Queues commands and sends them in one write. The server still runs them one after another. */
    public final class Pipeline {
        private final List<byte[][]> cmds = new ArrayList<>();
        public Pipeline cmd(Object... args) { cmds.add(MsmqCacheClient.args(args)); return this; }
        public Pipeline set(Object key, Object value, SetOptions o) { cmds.add(MsmqCacheClient.args(setArgs(key, value, o).toArray())); return this; }
        public Pipeline get(Object key) { return cmd("GET", key(key)); }
        public Pipeline incr(Object key) { return cmd("INCR", key(key)); }

        /** Replies in order; server errors appear as {@link ResponseException} instances in the list. */
        public List<Object> execLenient() {
            if (cmds.isEmpty()) return new ArrayList<>();
            List<Object> raw = MsmqCacheClient.this.exec(new ArrayList<>(cmds));
            cmds.clear();
            List<Object> out = new ArrayList<>(raw.size());
            for (Object r : raw) out.add(r instanceof ErrorReply ? serverError(((ErrorReply) r).message) : r);
            return out;
        }

        /** Like {@link #execLenient()} but throws the first server error. */
        public List<Object> exec() {
            List<Object> out = execLenient();
            for (Object r : out) if (r instanceof ResponseException) throw (ResponseException) r;
            return out;
        }
    }
}
