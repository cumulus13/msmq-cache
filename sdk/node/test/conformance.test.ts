import { test } from "node:test";
import * as assert from "node:assert/strict";
import { AuthenticationError, CacheConnectionError, CacheTimeoutError, Client, ResponseError } from "../src";

const port = Number(process.env.MCACHE_TEST_PORT);
const authPort = Number(process.env.MCACHE_TEST_AUTH_PORT);
const password = process.env.MCACHE_TEST_PASSWORD!;
if (!port || !authPort) throw new Error("run through sdk/conformance/run.sh (MCACHE_TEST_PORT is not set)");

async function withClient(fn: (c: Client) => Promise<void>, o = {}) {
  const c = new Client({ port, timeout: 2000, ...o });
  try { await fn(c); } finally { await c.close(); }
}
const s = (b: unknown) => (Buffer.isBuffer(b) ? b.toString() : b);

test("ping", () => withClient(async (c) => { assert.equal(await c.ping(), true); }));

test("set_get_delete", () => withClient(async (c) => {
  assert.equal(await c.set("n:k", "v"), true);
  assert.equal(s(await c.get("n:k")), "v");
  assert.equal(await c.del("n:k", "n:missing"), 1);
  assert.equal(await c.get("n:k"), null);
}));

test("binary_safe", () => withClient(async (c) => {
  const blob = Buffer.concat(Array.from({ length: 100 }, () => Buffer.from([0, 13, 10, 36, 53, 13, 10, 42, 49, 13, 10, 255])));
  await c.set("n:blob", blob);
  const back = (await c.get("n:blob")) as Buffer;
  assert.ok(back.equals(blob));
}));

test("nil_vs_empty", () => withClient(async (c) => {
  await c.set("n:empty", "");
  const v = await c.get("n:empty");
  assert.ok(Buffer.isBuffer(v) && v.length === 0);
  assert.equal(await c.get("n:never"), null);
}));

test("set_options", () => withClient(async (c) => {
  await c.del("n:opt");
  assert.equal(await c.set("n:opt", 1, { ex: 30, nx: true }), true);
  assert.deepEqual(((await c.command("LASTCMD")) as Buffer[]).map(String), ["SET", "n:opt", "1", "EX", "30", "NX"]);
  assert.equal(await c.set("n:opt", 2, { nx: true }), false);
  assert.equal(await c.set("n:nokey", 2, { xx: true }), false);
  await c.set("n:opt", "x", { px: 1500 });
  assert.deepEqual(((await c.command("LASTCMD")) as Buffer[]).map(String).slice(3), ["PX", "1500"]);
  assert.throws(() => c.setArgs("k", "v", { nx: true, xx: true }));
  assert.throws(() => c.setArgs("", "v"));
  assert.throws(() => c.setArgs("k", "v", { ex: 0 }));
}));

test("numbers_and_multi", () => withClient(async (c) => {
  await c.del("n:cnt");
  assert.equal(await c.incr("n:cnt"), 1);
  assert.equal(await c.incrBy("n:cnt", 9), 10);
  await c.mset({ "m:1": "a", "m:2": "b" });
  assert.deepEqual((await c.mget(["m:1", "m:none", "m:2"])).map(s), ["a", null, "b"]);
  assert.deepEqual((await c.keys("m:*")).map(String).sort(), ["m:1", "m:2"]);
}));

test("server_error", () => withClient(async (c) => {
  await c.set("n:str", "abc");
  await assert.rejects(c.incr("n:str"), (e: Error) => e instanceof ResponseError && /not an integer/.test(e.message));
  await assert.rejects(c.command("BOGUS"), ResponseError);
  assert.equal(await c.ping(), true);
}));

test("auth", async () => {
  const ok = new Client({ port: authPort, password });
  assert.equal(await ok.ping(), true);
  await ok.close();
  const bad = new Client({ port: authPort, password: "wrong" });
  await assert.rejects(bad.connect(), AuthenticationError);
  const none = new Client({ port: authPort });
  await assert.rejects(none.ping(), AuthenticationError);
  await none.close();
});

test("timeout_reconnect", () => withClient(async (c) => {
  await assert.rejects(c.command("SLOW", 1), CacheTimeoutError);
  assert.equal(await c.ping(), true);
}, { timeout: 300 }));

test("dropped_connection", () => withClient(async (c) => {
  await assert.rejects(c.command("DROP"), (e: Error) => e instanceof CacheConnectionError && !(e instanceof CacheTimeoutError));
  assert.equal(await c.ping(), true);
}));

test("pipeline", () => withClient(async (c) => {
  await c.del("p:1", "p:2", "p:cnt");
  const res = await c.pipeline().set("p:1", "a").set("p:2", "b").get("p:1").incr("p:cnt").execute();
  assert.deepEqual(res.map(s), [true, true, "a", 1]);
  const single = await c.pipeline().get("p:1").execute();
  assert.equal(single.length, 1);
  assert.equal(s(single[0]), "a");
  const mixed = await c.pipeline().set("p:s", "abc").command("INCR", "p:s").get("p:s").execute(false);
  assert.ok(mixed[1] instanceof ResponseError);
  assert.equal(s(mixed[2]), "abc");
  await assert.rejects(c.pipeline().command("INCR", "p:s").execute(), ResponseError);
}));

test("queues", () => withClient(async (c) => {
  assert.equal(await c.qpush("n:jobs", "a", { priority: 5, ttl: 60 }), true);
  assert.deepEqual(((await c.command("LASTCMD")) as Buffer[]).map(String), ["QPUSH", "n:jobs", "a", "PRIORITY", "5", "TTL", "60"]);
  assert.equal(s(await c.qpop("n:jobs")), "a");
  assert.equal(await c.qpop("n:jobs"), null);
  assert.throws(() => c.qpushArgs("q", "v", { priority: 8 }));
}));

test("decode_responses_and_info", () => withClient(async (c) => {
  await c.set("n:u", "héllo");
  assert.equal(await c.get("n:u"), "héllo");
  assert.equal((await c.info()).server.mcache_version, "1.0.0");
}, { decodeResponses: true }));
