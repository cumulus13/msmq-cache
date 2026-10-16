#!/usr/bin/env python3

# File: sdk/python/src/msmqcache/_resp.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-17
# Description: msmq-cache - RESP encoding/decoding shared by the sync and asyncio clients (stdlib only).
# License: MIT

"""RESP encoding/decoding shared by the sync and asyncio clients (stdlib only)."""

from __future__ import annotations

from typing import Any, Iterable, List, Optional

from .errors import AuthenticationError, ProtocolError, ResponseError

MAX_LINE = 64 * 1024
CRLF = b"\r\n"


def encode_command(args: Iterable[bytes]) -> bytes:
    parts: List[bytes] = []
    items = list(args)
    parts.append(b"*%d\r\n" % len(items))
    for a in items:
        parts.append(b"$%d\r\n" % len(a))
        parts.append(a)
        parts.append(CRLF)
    return b"".join(parts)


def make_error(text: str) -> ResponseError:
    if text.startswith(("NOAUTH", "WRONGPASS")):
        return AuthenticationError(text)
    return ResponseError(text)


def parse_header(line: bytes):
    """Returns (type_char, payload) for one protocol line (CRLF included)."""
    if not line:
        raise EOFError("connection closed by server")
    if not line.endswith(CRLF):
        raise ProtocolError("truncated line: %r" % line[:80])
    return line[:1], line[1:-2]


def to_int(raw: bytes, what: str) -> int:
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError("invalid %s: %r" % (what, raw[:40])) from None


def read_reply(f) -> Any:
    """Blocking parser over a binary file-like object (socket.makefile('rb'))."""
    t, rest = parse_header(f.readline(MAX_LINE))
    if t == b"+":
        return rest.decode("utf-8", "replace")
    if t == b"-":
        return make_error(rest.decode("utf-8", "replace"))
    if t == b":":
        return to_int(rest, "integer")
    if t == b"$":
        n = to_int(rest, "bulk length")
        if n < 0:
            return None
        data = f.read(n + 2)
        if len(data) != n + 2:
            raise EOFError("connection closed mid-reply")
        return data[:-2]
    if t == b"*":
        n = to_int(rest, "array length")
        if n < 0:
            return None
        return [read_reply(f) for _ in range(n)]
    raise ProtocolError("unknown reply type %r" % t)


async def aread_reply(r) -> Any:
    """Same parser over an asyncio.StreamReader."""
    import asyncio

    try:
        line = await r.readline()
    except (ValueError, asyncio.LimitOverrunError) as e:
        raise ProtocolError("line too long") from e
    t, rest = parse_header(line)
    if t == b"+":
        return rest.decode("utf-8", "replace")
    if t == b"-":
        return make_error(rest.decode("utf-8", "replace"))
    if t == b":":
        return to_int(rest, "integer")
    if t == b"$":
        n = to_int(rest, "bulk length")
        if n < 0:
            return None
        try:
            data = await r.readexactly(n + 2)
        except asyncio.IncompleteReadError as e:
            raise EOFError("connection closed mid-reply") from e
        return data[:-2]
    if t == b"*":
        n = to_int(rest, "array length")
        if n < 0:
            return None
        return [await aread_reply(r) for _ in range(n)]
    raise ProtocolError("unknown reply type %r" % t)


def to_bytes(value: Any, encoding: str = "utf-8") -> bytes:
    if isinstance(value, bool) or value is None:
        raise TypeError("cannot send %r; convert it to str/bytes/int first" % (value,))
    if isinstance(value, bytes):
        return value
    if isinstance(value, (bytearray, memoryview)):
        return bytes(value)
    if isinstance(value, str):
        return value.encode(encoding)
    if isinstance(value, (int, float)):
        return repr(value).encode("ascii")
    raise TypeError("unsupported type %s" % type(value).__name__)
