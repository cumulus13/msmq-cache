#!/usr/bin/env python3

# File: sdk/python/src/msmqcache/client.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-17
# Description: msmq-cache - Blocking client and pipeline.
# License: MIT

"""Blocking client and pipeline."""

from __future__ import annotations

import socket
import threading
from typing import Any, Callable, List, Optional, Tuple

from ._resp import encode_command, read_reply
from .commands import Commands
from .errors import (AuthenticationError, CacheConnectionError, CacheTimeoutError, MsmqCacheError,
                     ProtocolError, ResponseError)

DEFAULT_PORT = 6390


class Client(Commands):
    """Thread-safe blocking client (one connection, calls are serialised).

    Delivery semantics follow the server: a command that failed with a connection error or timeout is *never*
    retried automatically because it may already have been executed (at-most-once).
    """

    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT, password: Optional[str] = None,
                 timeout: float = 10.0, connect_timeout: Optional[float] = 5.0, decode_responses: bool = False,
                 encoding: str = "utf-8", max_value_bytes: int = 4_000_000, keepalive: bool = True):
        self.host, self.port, self.password = host, port, password
        self.timeout, self.connect_timeout = timeout, connect_timeout
        self.decode_responses, self.encoding = decode_responses, encoding
        self.max_value_bytes, self.keepalive = max_value_bytes, keepalive
        self._sock: Optional[socket.socket] = None
        self._rfile = None
        self._lock = threading.RLock()

    # ---- connection -----------------------------------------------------
    def connect(self) -> "Client":
        with self._lock:
            if self._sock is not None:
                return self
            try:
                s = socket.create_connection((self.host, self.port), timeout=self.connect_timeout)
            except socket.timeout as e:
                raise CacheTimeoutError("connect to %s:%s timed out" % (self.host, self.port)) from e
            except OSError as e:
                raise CacheConnectionError("cannot connect to %s:%s: %s" % (self.host, self.port, e)) from e
            s.settimeout(self.timeout)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            if self.keepalive:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            self._sock, self._rfile = s, s.makefile("rb")
            if self.password:
                try:
                    reply = self._roundtrip(encode_command([b"AUTH", self.password.encode(self.encoding)]))
                except BaseException:
                    self.close()
                    raise
                if isinstance(reply, ResponseError):
                    self.close()
                    raise reply
            return self

    def close(self) -> None:
        with self._lock:
            for obj in (self._rfile, self._sock):
                try:
                    if obj is not None:
                        obj.close()
                except OSError:
                    pass
            self._rfile = self._sock = None

    def __enter__(self) -> "Client":
        return self.connect()

    def __exit__(self, *exc) -> None:
        self.close()

    # ---- I/O ----------------------------------------------------------------
    def _roundtrip(self, payload: bytes, replies: Optional[int] = None) -> Any:
        try:
            self._sock.sendall(payload)
            if replies is None:
                return read_reply(self._rfile)
            return [read_reply(self._rfile) for _ in range(replies)]
        except socket.timeout as e:
            self.close()
            raise CacheTimeoutError("no reply within %.1fs; connection closed, the command may still be executed" % self.timeout) from e
        except (OSError, EOFError) as e:
            self.close()
            raise CacheConnectionError("connection lost: %s" % e) from e
        except ProtocolError:
            self.close()
            raise

    def _call(self, args: List[bytes], callback: Optional[Callable[[Any], Any]] = None):
        with self._lock:
            self.connect()
            reply = self._roundtrip(encode_command(args))
        if isinstance(reply, ResponseError):
            raise reply
        return callback(reply) if callback else reply

    def pipeline(self) -> "Pipeline":
        return Pipeline(self)


class Pipeline(Commands):
    """Queues commands and sends them in one write. ``execute()`` returns the results in order.

    The server still runs them one after another (each is its own MSMQ message); this only saves round trips.
    """

    def __init__(self, client: Client):
        self._client = client
        self._queue: List[Tuple[List[bytes], Optional[Callable[[Any], Any]]]] = []
        self.encoding, self.decode_responses = client.encoding, client.decode_responses
        self.max_value_bytes = client.max_value_bytes

    def _call(self, args, callback=None):
        self._queue.append((args, callback))
        return self

    def __len__(self) -> int:
        return len(self._queue)

    def __enter__(self) -> "Pipeline":
        return self

    def __exit__(self, *exc) -> None:
        self._queue.clear()

    def execute(self, raise_on_error: bool = True) -> List[Any]:
        queue, self._queue = self._queue, []
        if not queue:
            return []
        c = self._client
        payload = b"".join(encode_command(a) for a, _ in queue)
        with c._lock:
            c.connect()
            replies = c._roundtrip(payload, replies=len(queue))
        out: List[Any] = []
        first_error: Optional[MsmqCacheError] = None
        for (_, cb), r in zip(queue, replies):
            if isinstance(r, ResponseError):
                first_error = first_error or r
                out.append(r)
            else:
                out.append(cb(r) if cb else r)
        if raise_on_error and first_error is not None:
            raise first_error
        return out
