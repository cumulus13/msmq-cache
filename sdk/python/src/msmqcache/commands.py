#!/usr/bin/env python3

# File: sdk/python/src/msmqcache/commands.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-17
# Description: msmq-cache -- Command set, written once and shared by Client, AsyncClient and Pipeline.
#              Every method builds the argument list and hands it to ``self._call(args, callback)``:
#              Client executes it and returns the value, AsyncClient returns a coroutine, Pipeline 
#              queues it.
# License: MIT

"""Command set, written once and shared by Client, AsyncClient and Pipeline.

Every method builds the argument list and hands it to ``self._call(args, callback)``:
Client executes it and returns the value, AsyncClient returns a coroutine, Pipeline queues it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Union

from ._resp import to_bytes

MAX_KEY_BYTES = 1024
Number = Union[int, timedelta]


def _secs(v: Number, name: str) -> int:
    if isinstance(v, timedelta):
        v = int(-(-v.total_seconds() // 1))  # ceil
    if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
        raise ValueError("%s must be a positive integer (or timedelta)" % name)
    return v


def _millis(v: Number, name: str) -> int:
    if isinstance(v, timedelta):
        v = int(-(-v.total_seconds() * 1000 // 1))
    if isinstance(v, bool) or not isinstance(v, int) or v <= 0:
        raise ValueError("%s must be a positive integer (or timedelta)" % name)
    return v


def parse_info(text: str) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    section = "default"
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#"):
            section = line.lstrip("# ").lower()
            continue
        k, _, v = line.partition(":")
        out.setdefault(section, {})[k] = int(v) if v.lstrip("-").isdigit() else v
    return out


class Commands:
    encoding: str = "utf-8"
    decode_responses: bool = False
    max_value_bytes: int = 4_000_000

    def _call(self, args: List[bytes], callback=None):  # pragma: no cover - implemented by subclasses
        raise NotImplementedError

    # ---- helpers -------------------------------------------------------
    def _key(self, key: Any) -> bytes:
        b = to_bytes(key, self.encoding)
        if not 1 <= len(b) <= MAX_KEY_BYTES:
            raise ValueError("key length must be 1..%d bytes" % MAX_KEY_BYTES)
        return b

    def _val(self, value: Any) -> bytes:
        b = to_bytes(value, self.encoding)
        if len(b) > self.max_value_bytes:
            raise ValueError("value is %d bytes; limit is %d (server max-value-bytes / MSMQ 4 MB)" % (len(b), self.max_value_bytes))
        return b

    def _dec(self, v: Any) -> Any:
        if not self.decode_responses:
            return v
        if isinstance(v, bytes):
            return v.decode(self.encoding)
        if isinstance(v, list):
            return [self._dec(x) for x in v]
        return v

    def execute_command(self, *args: Any):
        """Escape hatch: send any command, e.g. ``execute_command('INFO')``."""
        return self._call([to_bytes(a, self.encoding) for a in args], self._dec)

    # ---- connection / server ---------------------------------------------
    def ping(self):
        return self._call([b"PING"], lambda r: r == "PONG")

    def echo(self, message: Any):
        return self._call([b"ECHO", self._val(message)], self._dec)

    def info(self):
        return self._call([b"INFO"], lambda r: parse_info(r.decode("utf-8", "replace")))

    # ---- keys ---------------------------------------------------------------
    def set(self, key, value, ex: Optional[Number] = None, px: Optional[Number] = None,
            nx: bool = False, xx: bool = False, keepttl: bool = False):
        """SET. Returns True when stored, False when NX/XX prevented it. ``ex`` seconds, ``px`` milliseconds."""
        if nx and xx:
            raise ValueError("nx and xx are mutually exclusive")
        if ex is not None and px is not None:
            raise ValueError("use either ex or px")
        args = [b"SET", self._key(key), self._val(value)]
        if ex is not None:
            args += [b"EX", b"%d" % _secs(ex, "ex")]
        if px is not None:
            args += [b"PX", b"%d" % _millis(px, "px")]
        if nx:
            args.append(b"NX")
        if xx:
            args.append(b"XX")
        if keepttl:
            args.append(b"KEEPTTL")
        return self._call(args, lambda r: r == "OK")

    def get(self, key):
        return self._call([b"GET", self._key(key)], self._dec)

    def mget(self, keys: Sequence[Any]):
        if not keys:
            raise ValueError("keys must not be empty")
        return self._call([b"MGET"] + [self._key(k) for k in keys], self._dec)

    def mset(self, mapping: Mapping[Any, Any]):
        """Not atomic (one message per key inside the server)."""
        if not mapping:
            raise ValueError("mapping must not be empty")
        args = [b"MSET"]
        for k, v in mapping.items():
            args += [self._key(k), self._val(v)]
        return self._call(args, lambda r: r == "OK")

    def delete(self, *keys):
        if not keys:
            raise ValueError("at least one key required")
        return self._call([b"DEL"] + [self._key(k) for k in keys])

    def exists(self, *keys):
        if not keys:
            raise ValueError("at least one key required")
        return self._call([b"EXISTS"] + [self._key(k) for k in keys])

    def expire(self, key, seconds: Number):
        """Re-sends the value (MSMQ messages are immutable), so the cost grows with the value size."""
        return self._call([b"EXPIRE", self._key(key), b"%d" % _secs(seconds, "seconds")], bool)

    def pexpire(self, key, millis: Number):
        return self._call([b"PEXPIRE", self._key(key), b"%d" % _millis(millis, "millis")], bool)

    def persist(self, key):
        return self._call([b"PERSIST", self._key(key)], bool)

    def ttl(self, key):
        """Seconds left; -1 = no expiry, -2 = no such key."""
        return self._call([b"TTL", self._key(key)])

    def pttl(self, key):
        return self._call([b"PTTL", self._key(key)])

    def incr(self, key):
        return self._call([b"INCR", self._key(key)])

    def decr(self, key):
        return self._call([b"DECR", self._key(key)])

    def incrby(self, key, amount: int = 1):
        return self._call([b"INCRBY", self._key(key), b"%d" % int(amount)])

    def decrby(self, key, amount: int = 1):
        return self._call([b"DECRBY", self._key(key), b"%d" % int(amount)])

    def keys(self, pattern: str = "*"):
        """O(n) over the whole keyspace."""
        return self._call([b"KEYS", to_bytes(pattern, self.encoding)], self._dec)

    def dbsize(self):
        return self._call([b"DBSIZE"])

    def flushdb(self):
        return self._call([b"FLUSHDB"], lambda r: r == "OK")

    # ---- native MSMQ work queues ------------------------------------------------
    def qpush(self, queue: str, value, priority: Optional[int] = None, ttl: Optional[Number] = None):
        """Append to MSMQ queue ``queue`` (auto-created). priority 0..7 (7 = highest), ttl = MSMQ TimeToBeReceived."""
        args = [b"QPUSH", to_bytes(queue, self.encoding), self._val(value)]
        if priority is not None:
            if isinstance(priority, bool) or not isinstance(priority, int) or not 0 <= priority <= 7:
                raise ValueError("priority must be an int in 0..7")
            args += [b"PRIORITY", b"%d" % priority]
        if ttl is not None:
            args += [b"TTL", b"%d" % _secs(ttl, "ttl")]
        return self._call(args, lambda r: r == "OK")

    def qpop(self, queue: str):
        """Non-blocking: returns None when the queue is empty (poll, there is no blocking pop)."""
        return self._call([b"QPOP", to_bytes(queue, self.encoding)], self._dec)

    def qpeek(self, queue: str):
        return self._call([b"QPEEK", to_bytes(queue, self.encoding)], self._dec)

    def qlen(self, queue: str):
        """O(n): the server enumerates the MSMQ queue."""
        return self._call([b"QLEN", to_bytes(queue, self.encoding)])

    def qpurge(self, queue: str):
        return self._call([b"QPURGE", to_bytes(queue, self.encoding)], bool)

    def qdel(self, queue: str):
        return self._call([b"QDEL", to_bytes(queue, self.encoding)], bool)

    def qlist(self):
        return self._call([b"QLIST"], self._dec)
