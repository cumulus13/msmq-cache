import { AuthenticationError, ProtocolError, ResponseError } from "./errors";

export type Reply = null | number | string | Buffer | ResponseError | Reply[];

export function encodeCommand(args: Buffer[]): Buffer {
  const parts: Buffer[] = [Buffer.from(`*${args.length}\r\n`)];
  for (const a of args) {
    parts.push(Buffer.from(`$${a.length}\r\n`), a, Buffer.from("\r\n"));
  }
  return Buffer.concat(parts);
}

export function makeError(text: string): ResponseError {
  return /^(NOAUTH|WRONGPASS)/.test(text) ? new AuthenticationError(text) : new ResponseError(text);
}

const MAX_LINE = 64 * 1024;

/** Parses one reply from buf at offset. Returns null when more bytes are needed. */
export function tryParse(buf: Buffer, offset = 0): { value: Reply; next: number } | null {
  if (offset >= buf.length) return null;
  const eol = buf.indexOf("\r\n", offset);
  if (eol < 0) {
    if (buf.length - offset > MAX_LINE) throw new ProtocolError("line too long");
    return null;
  }
  const type = String.fromCharCode(buf[offset]);
  const rest = buf.toString("utf8", offset + 1, eol);
  const after = eol + 2;
  const num = (what: string): number => {
    if (!/^-?\d+$/.test(rest)) throw new ProtocolError(`invalid ${what}: ${rest.slice(0, 40)}`);
    return parseInt(rest, 10);
  };
  switch (type) {
    case "+": return { value: rest, next: after };
    case "-": return { value: makeError(rest), next: after };
    case ":": return { value: num("integer"), next: after };
    case "$": {
      const n = num("bulk length");
      if (n < 0) return { value: null, next: after };
      if (buf.length < after + n + 2) return null;
      return { value: Buffer.from(buf.subarray(after, after + n)), next: after + n + 2 };
    }
    case "*": {
      const n = num("array length");
      if (n < 0) return { value: null, next: after };
      const items: Reply[] = [];
      let pos = after;
      for (let i = 0; i < n; i++) {
        const r = tryParse(buf, pos);
        if (!r) return null;
        items.push(r.value);
        pos = r.next;
      }
      return { value: items, next: pos };
    }
  }
  throw new ProtocolError(`unknown reply type '${type}'`);
}

export type Arg = string | Buffer | Uint8Array | number | bigint;

export function toBuffer(v: Arg): Buffer {
  if (Buffer.isBuffer(v)) return v;
  if (v instanceof Uint8Array) return Buffer.from(v);
  if (typeof v === "string") return Buffer.from(v, "utf8");
  if (typeof v === "number" || typeof v === "bigint") return Buffer.from(String(v), "ascii");
  throw new TypeError(`unsupported argument type ${typeof v}`);
}
