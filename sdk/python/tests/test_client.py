import asyncio
import os
import sys
import unittest
from datetime import timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from fake_server import FakeServer  # noqa: E402
from msmqcache import (AsyncClient, AuthenticationError, CacheConnectionError, CacheTimeoutError,  # noqa: E402
                       Client, ResponseError)


class SyncTests(unittest.TestCase):
    def setUp(self):
        self.srv = FakeServer().start()
        self.c = Client(port=self.srv.port, timeout=2.0)

    def tearDown(self):
        self.c.close()
        self.srv.stop()

    def test_ping_set_get_delete(self):
        self.assertTrue(self.c.ping())
        self.assertTrue(self.c.set("k", "v"))
        self.assertEqual(self.c.get("k"), b"v")
        self.assertEqual(self.c.delete("k", "missing"), 1)
        self.assertIsNone(self.c.get("k"))

    def test_binary_safe_value_with_crlf_and_nul(self):
        blob = b"\x00\r\n$5\r\n*1\r\n\xff" * 100
        self.c.set("blob", blob)
        self.assertEqual(self.c.get("blob"), blob)

    def test_set_options_are_sent_and_nx_xx_return_bool(self):
        self.assertTrue(self.c.set("a", 1, ex=30, nx=True))
        self.assertFalse(self.c.set("a", 2, nx=True))
        self.assertFalse(self.c.set("zz", 2, xx=True))
        self.c.set("t", "x", px=timedelta(seconds=1.5))
        sent = [x for x in self.srv.log if x[0] == b"SET"]
        self.assertEqual(sent[0], [b"SET", b"a", b"1", b"EX", b"30", b"NX"])
        self.assertEqual(sent[3][3:], [b"PX", b"1500"])

    def test_numbers_incr_mget_mset_keys(self):
        self.assertEqual(self.c.incr("n"), 1)
        self.assertEqual(self.c.incrby("n", 9), 10)
        self.c.mset({"x1": "a", "x2": "b"})
        self.assertEqual(self.c.mget(["x1", "nope", "x2"]), [b"a", None, b"b"])
        self.assertEqual(sorted(self.c.keys("x*")), [b"x1", b"x2"])

    def test_decode_responses(self):
        c = Client(port=self.srv.port, decode_responses=True)
        c.set("k", "héllo")
        self.assertEqual(c.get("k"), "héllo")
        self.assertEqual(c.mget(["k", "no"]), ["héllo", None])
        c.close()

    def test_queue_commands_and_info(self):
        self.c.qpush("jobs", "a", priority=5, ttl=60)
        self.assertEqual(self.c.qpop("jobs"), b"a")
        self.assertIsNone(self.c.qpop("jobs"))
        self.assertEqual(self.c.info()["server"]["mcache_version"], "1.0.0")
        self.assertIsInstance(self.c.info()["stats"]["keys"], int)

    def test_server_error_becomes_exception(self):
        self.c.set("s", "abc")
        with self.assertRaises(ResponseError) as cm:
            self.c.incr("s")
        self.assertIn("not an integer", str(cm.exception))
        with self.assertRaises(ResponseError):
            self.c.execute_command("BOGUS")
        self.assertTrue(self.c.ping())          # connection still usable after an error reply

    def test_client_side_validation(self):
        for bad in (lambda: self.c.set("", "v"), lambda: self.c.set("k", None), lambda: self.c.set("k", True),
                    lambda: self.c.set("k", "v", ex=0), lambda: self.c.set("k", "v", nx=True, xx=True),
                    lambda: self.c.set("k", "v", ex=1, px=1), lambda: self.c.qpush("q", "v", priority=8),
                    lambda: self.c.set("k" * 2000, "v")):
            with self.assertRaises((ValueError, TypeError)):
                bad()
        small = Client(port=self.srv.port, max_value_bytes=4)
        with self.assertRaises(ValueError):
            small.set("k", "12345")

    def test_pipeline(self):
        with self.c.pipeline() as p:
            p.set("p1", "a").set("p2", "b")
            p.get("p1")
            p.incr("cnt")
            res = p.execute()
        self.assertEqual(res, [True, True, b"a", 1])
        p = self.c.pipeline()
        p.set("k", "abc")
        p.incr("k")
        p.get("k")
        res = p.execute(raise_on_error=False)
        self.assertIsInstance(res[1], ResponseError)
        self.assertEqual(res[2], b"abc")
        p.incr("k")
        with self.assertRaises(ResponseError):
            p.execute()
        single = self.c.pipeline()
        single.get("p1")
        self.assertEqual(single.execute(), [b"a"])   # one queued command still yields a list

    def test_timeout_closes_connection_and_next_call_reconnects(self):
        c = Client(port=self.srv.port, timeout=0.3)
        with self.assertRaises(CacheTimeoutError):
            c.execute_command("SLOW", 1)
        self.assertTrue(c.ping())               # fresh connection, stream is not desynchronised
        c.close()

    def test_dropped_connection_raises_and_recovers(self):
        with self.assertRaises(CacheConnectionError):
            self.c.execute_command("DROP")
        self.assertTrue(self.c.ping())

    def test_connect_refused(self):
        self.srv.stop()
        with self.assertRaises(CacheConnectionError):
            Client(port=self.srv.port, connect_timeout=1).connect()
        self.srv = FakeServer().start()         # keep tearDown happy


class AuthTests(unittest.TestCase):
    def test_password_flow(self):
        srv = FakeServer(password="s3cret").start()
        try:
            with Client(port=srv.port, password="s3cret") as c:
                self.assertTrue(c.ping())
            with self.assertRaises(AuthenticationError):
                Client(port=srv.port, password="wrong").connect()
            noauth = Client(port=srv.port)
            with self.assertRaises(AuthenticationError):
                noauth.ping()                   # no password -> NOAUTH
            noauth.close()
        finally:
            srv.stop()


class AsyncTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.srv = FakeServer().start()

    async def asyncTearDown(self):
        self.srv.stop()

    async def test_basic_and_concurrent_calls(self):
        async with AsyncClient(port=self.srv.port, timeout=2) as c:
            self.assertTrue(await c.set("k", b"\r\n\x00"))
            self.assertEqual(await c.get("k"), b"\r\n\x00")
            res = await asyncio.gather(*[c.incr("n") for _ in range(50)])
            self.assertEqual(sorted(res), list(range(1, 51)))
            self.assertEqual(await c.mget(["k", "zz"]), [b"\r\n\x00", None])

    async def test_pipeline_and_errors(self):
        async with AsyncClient(port=self.srv.port, decode_responses=True) as c:
            p = c.pipeline()
            p.set("a", "1")
            p.get("a")
            self.assertEqual(await p.execute(), [True, "1"])
            await c.set("s", "x")
            with self.assertRaises(ResponseError):
                await c.incr("s")
            self.assertTrue(await c.ping())

    async def test_timeout_then_reconnect(self):
        c = AsyncClient(port=self.srv.port, timeout=0.3)
        with self.assertRaises(CacheTimeoutError):
            await c.execute_command("SLOW", 1)
        self.assertTrue(await c.ping())
        await c.aclose()

    async def test_auth(self):
        srv = FakeServer(password="pw").start()
        try:
            async with AsyncClient(port=srv.port, password="pw") as c:
                self.assertTrue(await c.ping())
            with self.assertRaises(AuthenticationError):
                await AsyncClient(port=srv.port, password="bad").connect()
        finally:
            srv.stop()


if __name__ == "__main__":
    unittest.main()
