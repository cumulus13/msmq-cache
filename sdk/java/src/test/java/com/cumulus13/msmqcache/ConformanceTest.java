package com.cumulus13.msmqcache;

import com.cumulus13.msmqcache.MsmqCacheClient.Options;
import com.cumulus13.msmqcache.MsmqCacheClient.QPushOptions;
import com.cumulus13.msmqcache.MsmqCacheClient.SetOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Dependency-free runner: java -cp out com.cumulus13.msmqcache.ConformanceTest (needs sdk/conformance/run.sh env). */
public final class ConformanceTest {
    interface T { void run() throws Exception; }

    static int port(String env) {
        String v = System.getenv(env);
        if (v == null) throw new IllegalStateException(env + " not set: run through sdk/conformance/run.sh");
        return Integer.parseInt(v);
    }
    static MsmqCacheClient client() { return new MsmqCacheClient(new Options().port(port("MCACHE_TEST_PORT")).timeoutMs(2000)); }
    static String s(byte[] b) { return b == null ? null : new String(b, StandardCharsets.UTF_8); }
    static void eq(Object want, Object got) { if (!java.util.Objects.equals(want, got)) throw new AssertionError("want " + want + " got " + got); }
    static void yes(boolean c, String m) { if (!c) throw new AssertionError(m); }
    static void throwsA(Class<? extends Throwable> k, Runnable r) {
        try { r.run(); } catch (Throwable t) { if (k.isInstance(t)) return; throw new AssertionError("want " + k.getSimpleName() + " got " + t); }
        throw new AssertionError("expected " + k.getSimpleName());
    }
    static List<String> last(MsmqCacheClient c) {
        List<String> out = new ArrayList<>();
        for (Object o : (List<?>) c.execute("LASTCMD")) out.add(s((byte[]) o));
        return out;
    }

    public static void main(String[] a) {
        Map<String, T> tests = new LinkedHashMap<>();
        tests.put("ping", () -> { try (MsmqCacheClient c = client()) { yes(c.ping(), "ping"); } });
        tests.put("set_get_delete", () -> { try (MsmqCacheClient c = client()) {
            yes(c.set("j:k", "v"), "set"); eq("v", c.getString("j:k")); eq(1L, c.del("j:k", "j:missing")); eq(null, c.get("j:k")); } });
        tests.put("binary_safe", () -> { try (MsmqCacheClient c = client()) {
            byte[] unit = {0, 13, 10, '$', '5', 13, 10, '*', '1', 13, 10, (byte) 255};
            byte[] blob = new byte[unit.length * 100];
            for (int i = 0; i < 100; i++) System.arraycopy(unit, 0, blob, i * unit.length, unit.length);
            c.set("j:blob", blob);
            yes(Arrays.equals(blob, c.get("j:blob")), "blob mismatch"); } });
        tests.put("nil_vs_empty", () -> { try (MsmqCacheClient c = client()) {
            c.set("j:empty", ""); byte[] v = c.get("j:empty"); yes(v != null && v.length == 0, "empty must be a non-null empty array");
            eq(null, c.get("j:never")); } });
        tests.put("set_options", () -> { try (MsmqCacheClient c = client()) {
            c.del("j:opt");
            yes(c.set("j:opt", 1L, new SetOptions().ex(30).nx()), "set nx");
            eq(Arrays.asList("SET", "j:opt", "1", "EX", "30", "NX"), last(c));
            yes(!c.set("j:opt", 2L, new SetOptions().nx()), "nx on existing");
            yes(!c.set("j:nokey", 2L, new SetOptions().xx()), "xx on missing");
            c.set("j:opt", "x", new SetOptions().px(1500));
            eq(Arrays.asList("PX", "1500"), last(c).subList(3, 5));
            throwsA(IllegalArgumentException.class, () -> c.set("k", "v", new SetOptions().nx().xx()));
            throwsA(IllegalArgumentException.class, () -> c.set("", "v"));
            throwsA(IllegalArgumentException.class, () -> c.set("k", "v", new SetOptions().ex(-1))); } });
        tests.put("numbers_and_multi", () -> { try (MsmqCacheClient c = client()) {
            c.del("j:cnt"); eq(1L, c.incr("j:cnt")); eq(10L, c.incrBy("j:cnt", 9));
            Map<String, Object> m = new LinkedHashMap<>(); m.put("jm:1", "a"); m.put("jm:2", "b"); c.mset(m);
            List<byte[]> got = c.mget("jm:1", "jm:none", "jm:2");
            eq("a", s(got.get(0))); eq(null, got.get(1)); eq("b", s(got.get(2)));
            List<String> keys = new ArrayList<>(); for (byte[] k : c.keys("jm:*")) keys.add(s(k)); java.util.Collections.sort(keys);
            eq(Arrays.asList("jm:1", "jm:2"), keys); } });
        tests.put("server_error", () -> { try (MsmqCacheClient c = client()) {
            c.set("j:str", "abc");
            try { c.incr("j:str"); throw new AssertionError("expected ResponseException"); }
            catch (ResponseException e) { yes(e.getMessage().contains("not an integer"), e.getMessage()); }
            throwsA(ResponseException.class, () -> c.execute("BOGUS"));
            yes(c.ping(), "connection must stay usable"); } });
        tests.put("auth", () -> {
            int p = port("MCACHE_TEST_AUTH_PORT"); String pw = System.getenv("MCACHE_TEST_PASSWORD");
            try (MsmqCacheClient ok = new MsmqCacheClient(new Options().port(p).password(pw))) { yes(ok.ping(), "auth ok"); }
            try (MsmqCacheClient bad = new MsmqCacheClient(new Options().port(p).password("wrong"))) { throwsA(AuthenticationException.class, bad::ping); }
            try (MsmqCacheClient none = new MsmqCacheClient(new Options().port(p))) { throwsA(AuthenticationException.class, none::ping); } });
        tests.put("timeout_reconnect", () -> { try (MsmqCacheClient c = new MsmqCacheClient(new Options().port(port("MCACHE_TEST_PORT")).timeoutMs(300))) {
            throwsA(CacheTimeoutException.class, () -> c.execute("SLOW", 1)); yes(c.ping(), "fresh connection"); } });
        tests.put("dropped_connection", () -> { try (MsmqCacheClient c = client()) {
            try { c.execute("DROP"); throw new AssertionError("expected exception"); }
            catch (CacheTimeoutException e) { throw new AssertionError("must not be a timeout"); }
            catch (CacheConnectionException e) { /* expected */ }
            yes(c.ping(), "reconnect"); } });
        tests.put("pipeline", () -> { try (MsmqCacheClient c = client()) {
            c.del("jp:1", "jp:2", "jp:cnt");
            List<Object> r = c.pipeline().set("jp:1", "a", new SetOptions()).set("jp:2", "b", new SetOptions()).get("jp:1").incr("jp:cnt").exec();
            eq("a", s((byte[]) r.get(2))); eq(1L, r.get(3));
            eq(1, c.pipeline().get("jp:1").exec().size());
            List<Object> mixed = c.pipeline().set("jp:s", "abc", new SetOptions()).cmd("INCR", "jp:s").get("jp:s").execLenient();
            yes(mixed.get(1) instanceof ResponseException, "lenient keeps errors"); eq("abc", s((byte[]) mixed.get(2)));
            throwsA(ResponseException.class, () -> c.pipeline().cmd("INCR", "jp:s").exec()); } });
        tests.put("queues", () -> { try (MsmqCacheClient c = client()) {
            c.qpush("j:jobs", "a", new QPushOptions().priority(5).ttl(60));
            eq(Arrays.asList("QPUSH", "j:jobs", "a", "PRIORITY", "5", "TTL", "60"), last(c));
            eq("a", s(c.qpop("j:jobs"))); eq(null, c.qpop("j:jobs"));
            throwsA(IllegalArgumentException.class, () -> c.qpush("q", "v", new QPushOptions().priority(8))); } });
        tests.put("info", () -> { try (MsmqCacheClient c = client()) { eq("1.0.0", c.info().get("server").get("mcache_version")); } });

        int failed = 0;
        for (Map.Entry<String, T> t : tests.entrySet()) {
            try { t.getValue().run(); System.out.println("ok   " + t.getKey()); }
            catch (Throwable e) { failed++; System.out.println("FAIL " + t.getKey() + ": " + e); e.printStackTrace(System.out); }
        }
        System.out.println(tests.size() - failed + "/" + tests.size() + " passed");
        System.exit(failed == 0 ? 0 : 1);
    }
}
