import * as net from "net";
import {
  AuthenticationError, CacheConnectionError, CacheTimeoutError, MsmqCacheError, ProtocolError, ResponseError,
} from "./errors";
import { Arg, Reply, encodeCommand, toBuffer, tryParse } from "./resp";

export interface ClientOptions {
  host?: string;
  port?: number;
  password?: string;
  /** per-command reply timeout, ms (default 10000) */
  timeout?: number;
  connectTimeout?: number;
  /** return strings instead of Buffers for bulk replies */
  decodeResponses?: boolean;
  maxValueBytes?: number;
}

export interface SetOptions { ex?: number; px?: number; nx?: boolean; xx?: boolean; keepTtl?: boolean }
export interface QPushOptions { priority?: number; ttl?: number }

interface Pending { resolve: (r: Reply) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }

const MAX_KEY = 1024;

function posInt(v: number, name: string): number {
  if (!Number.isInteger(v) || v <= 0) throw new RangeError(`${name} must be a positive integer`);
  return v;
}

export class Client {
  readonly opts: Required<Omit<ClientOptions, "password">> & { password?: string };
  private socket: net.Socket | null = null;
  private connecting: Promise<void> | null = null;
  private buf: Buffer = Buffer.alloc(0);
  private pending: Pending[] = [];

  constructor(opts: ClientOptions = {}) {
    this.opts = {
      host: "127.0.0.1", port: 6390, timeout: 10000, connectTimeout: 5000,
      decodeResponses: false, maxValueBytes: 4_000_000, ...opts,
    };
  }

  // ---- connection ------------------------------------------------------------
  async connect(): Promise<this> {
    if (this.socket) return this;
    if (!this.connecting) {
      this.connecting = this.doConnect().finally(() => { this.connecting = null; });
    }
    await this.connecting;
    return this;
  }

  private doConnect(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      const s = net.createConnection({ host: this.opts.host, port: this.opts.port });
      s.setNoDelay(true);
      s.setKeepAlive(true);
      const t = setTimeout(() => { s.destroy(); reject(new CacheTimeoutError(`connect to ${this.opts.host}:${this.opts.port} timed out`)); }, this.opts.connectTimeout);
      s.once("error", (e) => { clearTimeout(t); reject(new CacheConnectionError(`cannot connect to ${this.opts.host}:${this.opts.port}: ${e.message}`)); });
      s.once("connect", () => {
        clearTimeout(t);
        s.removeAllListeners("error");
        this.socket = s;
        this.buf = Buffer.alloc(0);
        s.on("data", (d) => this.onData(d));
        s.on("error", (e) => this.fail(new CacheConnectionError(`connection lost: ${e.message}`)));
        s.on("close", () => this.fail(new CacheConnectionError("connection closed by server")));
        if (!this.opts.password) return resolve();
        this.send([Buffer.from("AUTH"), toBuffer(this.opts.password)]).then((r) => {
          if (r instanceof ResponseError) { this.fail(r); reject(r); } else resolve();
        }, (e) => { reject(e); });
      });
    });
  }

  /** Close the socket; every pending command is rejected with `err`. */
  private fail(err: Error): void {
    const s = this.socket;
    this.socket = null;
    if (s) { s.removeAllListeners("data"); s.removeAllListeners("close"); s.on("error", () => {}); s.destroy(); }
    const p = this.pending;
    this.pending = [];
    this.buf = Buffer.alloc(0);
    for (const x of p) { clearTimeout(x.timer); x.reject(err); }
  }

  async close(): Promise<void> {
    this.fail(new CacheConnectionError("client closed"));
  }

  private onData(d: Buffer): void {
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    try {
      for (;;) {
        const r = tryParse(this.buf);
        if (!r) break;
        this.buf = this.buf.subarray(r.next);
        const p = this.pending.shift();
        if (!p) throw new ProtocolError("unsolicited reply from server");
        clearTimeout(p.timer);
        p.resolve(r.value);
      }
    } catch (e) {
      this.fail(e instanceof MsmqCacheError ? e : new ProtocolError(String(e)));
    }
  }

  /** Writes already-encoded commands in one go and returns one promise per command. */
  private sendMany(cmds: Buffer[][]): Promise<Reply>[] {
    const s = this.socket!;
    const proms = cmds.map((_, i) => new Promise<Reply>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.fail(new CacheTimeoutError(`no reply within ${this.opts.timeout} ms; connection closed, the command may still be executed`));
      }, this.opts.timeout);
      this.pending.push({ resolve, reject, timer });
    }));
    s.write(Buffer.concat(cmds.map(encodeCommand)));
    return proms;
  }

  private send(args: Buffer[]): Promise<Reply> { return this.sendMany([args])[0]; }

  /** Low level: run one command, returns the raw reply (ResponseError instances are returned, not thrown). */
  async raw(args: Buffer[]): Promise<Reply> {
    await this.connect();
    return this.send(args);
  }

  async rawMany(cmds: Buffer[][]): Promise<Reply[]> {
    await this.connect();
    return Promise.all(this.sendMany(cmds));
  }

  private dec<T>(v: T): T | string {
    if (!this.opts.decodeResponses) return v;
    if (Buffer.isBuffer(v)) return v.toString("utf8");
    if (Array.isArray(v)) return v.map((x) => this.dec(x)) as unknown as T;
    return v;
  }

  private async call<T>(args: Buffer[], cb?: (r: any) => T): Promise<T> {
    const r = await this.raw(args);
    if (r instanceof ResponseError) throw r;
    return cb ? cb(r) : (r as unknown as T);
  }

  // ---- argument helpers (shared with Pipeline) ----------------------------------
  key(k: Arg): Buffer {
    const b = toBuffer(k);
    if (b.length < 1 || b.length > MAX_KEY) throw new RangeError(`key length must be 1..${MAX_KEY} bytes`);
    return b;
  }
  val(v: Arg): Buffer {
    const b = toBuffer(v);
    if (b.length > this.opts.maxValueBytes) throw new RangeError(`value is ${b.length} bytes; limit is ${this.opts.maxValueBytes}`);
    return b;
  }
  setArgs(key: Arg, value: Arg, o: SetOptions = {}): Buffer[] {
    if (o.nx && o.xx) throw new TypeError("nx and xx are mutually exclusive");
    if (o.ex !== undefined && o.px !== undefined) throw new TypeError("use either ex or px");
    const a: Buffer[] = [Buffer.from("SET"), this.key(key), this.val(value)];
    if (o.ex !== undefined) a.push(Buffer.from("EX"), toBuffer(posInt(o.ex, "ex")));
    if (o.px !== undefined) a.push(Buffer.from("PX"), toBuffer(posInt(o.px, "px")));
    if (o.nx) a.push(Buffer.from("NX"));
    if (o.xx) a.push(Buffer.from("XX"));
    if (o.keepTtl) a.push(Buffer.from("KEEPTTL"));
    return a;
  }
  qpushArgs(queue: string, value: Arg, o: QPushOptions = {}): Buffer[] {
    const a: Buffer[] = [Buffer.from("QPUSH"), toBuffer(queue), this.val(value)];
    if (o.priority !== undefined) {
      if (!Number.isInteger(o.priority) || o.priority < 0 || o.priority > 7) throw new RangeError("priority must be an integer 0..7");
      a.push(Buffer.from("PRIORITY"), toBuffer(o.priority));
    }
    if (o.ttl !== undefined) a.push(Buffer.from("TTL"), toBuffer(posInt(o.ttl, "ttl")));
    return a;
  }

  // ---- commands ----------------------------------------------------------------
  /** Run any command: `await c.command("INFO")`. Bulk replies honour decodeResponses. */
  command(...args: Arg[]): Promise<unknown> { return this.call(args.map(toBuffer), (r) => this.dec(r)); }
  ping(): Promise<boolean> { return this.call([Buffer.from("PING")], (r) => r === "PONG"); }
  /** true when stored, false when NX/XX prevented it. ex = seconds, px = milliseconds. */
  set(key: Arg, value: Arg, o: SetOptions = {}): Promise<boolean> { return this.call(this.setArgs(key, value, o), (r) => r === "OK"); }
  get(key: Arg): Promise<Buffer | string | null> { return this.call([Buffer.from("GET"), this.key(key)], (r) => this.dec(r)); }
  mget(keys: Arg[]): Promise<(Buffer | string | null)[]> {
    if (!keys.length) throw new RangeError("keys must not be empty");
    return this.call([Buffer.from("MGET"), ...keys.map((k) => this.key(k))], (r) => this.dec(r));
  }
  mset(entries: Record<string, Arg>): Promise<boolean> {
    const a: Buffer[] = [Buffer.from("MSET")];
    for (const [k, v] of Object.entries(entries)) a.push(this.key(k), this.val(v));
    if (a.length === 1) throw new RangeError("entries must not be empty");
    return this.call(a, (r) => r === "OK");
  }
  del(...keys: Arg[]): Promise<number> { return this.call([Buffer.from("DEL"), ...keys.map((k) => this.key(k))]); }
  exists(...keys: Arg[]): Promise<number> { return this.call([Buffer.from("EXISTS"), ...keys.map((k) => this.key(k))]); }
  /** Re-sends the value server-side (MSMQ messages are immutable). */
  expire(key: Arg, seconds: number): Promise<boolean> { return this.call([Buffer.from("EXPIRE"), this.key(key), toBuffer(posInt(seconds, "seconds"))], Boolean); }
  persist(key: Arg): Promise<boolean> { return this.call([Buffer.from("PERSIST"), this.key(key)], Boolean); }
  /** seconds left; -1 no expiry, -2 no such key */
  ttl(key: Arg): Promise<number> { return this.call([Buffer.from("TTL"), this.key(key)]); }
  incr(key: Arg): Promise<number> { return this.call([Buffer.from("INCR"), this.key(key)]); }
  decr(key: Arg): Promise<number> { return this.call([Buffer.from("DECR"), this.key(key)]); }
  incrBy(key: Arg, n: number): Promise<number> { return this.call([Buffer.from("INCRBY"), this.key(key), toBuffer(n)]); }
  keys(pattern = "*"): Promise<(Buffer | string)[]> { return this.call([Buffer.from("KEYS"), toBuffer(pattern)], (r) => this.dec(r)); }
  dbsize(): Promise<number> { return this.call([Buffer.from("DBSIZE")]); }
  flushdb(): Promise<boolean> { return this.call([Buffer.from("FLUSHDB")], (r) => r === "OK"); }
  /** Native MSMQ queue. priority 0..7 (7 highest), ttl seconds. */
  qpush(queue: string, value: Arg, o: QPushOptions = {}): Promise<boolean> { return this.call(this.qpushArgs(queue, value, o), (r) => r === "OK"); }
  /** Non-blocking: null when empty. */
  qpop(queue: string): Promise<Buffer | string | null> { return this.call([Buffer.from("QPOP"), toBuffer(queue)], (r) => this.dec(r)); }
  qpeek(queue: string): Promise<Buffer | string | null> { return this.call([Buffer.from("QPEEK"), toBuffer(queue)], (r) => this.dec(r)); }
  qlen(queue: string): Promise<number> { return this.call([Buffer.from("QLEN"), toBuffer(queue)]); }
  qlist(): Promise<(Buffer | string)[]> { return this.call([Buffer.from("QLIST")], (r) => this.dec(r)); }
  async info(): Promise<Record<string, Record<string, string | number>>> {
    const text = (await this.call<Buffer>([Buffer.from("INFO")])).toString("utf8");
    const out: Record<string, Record<string, string | number>> = {};
    let section = "default";
    for (const line of text.split(/\r?\n/)) {
      const l = line.trim();
      if (!l) continue;
      if (l.startsWith("#")) { section = l.replace(/^#+\s*/, "").toLowerCase(); continue; }
      const i = l.indexOf(":");
      (out[section] ??= {})[l.slice(0, i)] = /^-?\d+$/.test(l.slice(i + 1)) ? parseInt(l.slice(i + 1), 10) : l.slice(i + 1);
    }
    return out;
  }

  pipeline(): Pipeline { return new Pipeline(this); }
}

type Mapper = (r: any) => unknown;

/** Queues commands and sends them in one write. */
export class Pipeline {
  private q: { args: Buffer[]; map?: Mapper }[] = [];
  constructor(private readonly c: Client) {}
  get length(): number { return this.q.length; }
  command(...args: Arg[]): this { this.q.push({ args: args.map(toBuffer) }); return this; }
  set(key: Arg, value: Arg, o: SetOptions = {}): this { this.q.push({ args: this.c.setArgs(key, value, o), map: (r) => r === "OK" }); return this; }
  get(key: Arg): this { this.q.push({ args: [Buffer.from("GET"), this.c.key(key)] }); return this; }
  del(...keys: Arg[]): this { this.q.push({ args: [Buffer.from("DEL"), ...keys.map((k) => this.c.key(k))] }); return this; }
  incr(key: Arg): this { this.q.push({ args: [Buffer.from("INCR"), this.c.key(key)] }); return this; }
  mget(keys: Arg[]): this { this.q.push({ args: [Buffer.from("MGET"), ...keys.map((k) => this.c.key(k))] }); return this; }
  qpush(queue: string, value: Arg, o: QPushOptions = {}): this { this.q.push({ args: this.c.qpushArgs(queue, value, o), map: (r) => r === "OK" }); return this; }

  /** Results in order. With raiseOnError=false, failed commands appear as ResponseError instances. */
  async execute(raiseOnError = true): Promise<unknown[]> {
    const q = this.q;
    this.q = [];
    if (!q.length) return [];
    const replies = await this.c.rawMany(q.map((x) => x.args));
    const out = replies.map((r, i) => (r instanceof ResponseError ? r : q[i].map ? q[i].map!(r) : r));
    const firstErr = replies.find((r) => r instanceof ResponseError) as ResponseError | undefined;
    if (raiseOnError && firstErr) throw firstErr;
    return out;
  }
}
