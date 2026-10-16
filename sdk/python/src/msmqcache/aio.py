"""asyncio client (same command set; every command method returns an awaitable)."""
from __future__ import annotations

import asyncio
from typing import Any, Callable, List, Optional, Tuple

from ._resp import aread_reply, encode_command
from .commands import Commands
from .errors import CacheConnectionError, CacheTimeoutError, MsmqCacheError, ProtocolError, ResponseError

DEFAULT_PORT = 6390


class AsyncClient(Commands):
    """One connection; concurrent awaits are serialised by an internal lock (use pipeline() to batch)."""

    def __init__(self, host: str = "127.0.0.1", port: int = DEFAULT_PORT, password: Optional[str] = None,
                 timeout: float = 10.0, connect_timeout: Optional[float] = 5.0, decode_responses: bool = False,
                 encoding: str = "utf-8", max_value_bytes: int = 4_000_000):
        self.host, self.port, self.password = host, port, password
        self.timeout, self.connect_timeout = timeout, connect_timeout
        self.decode_responses, self.encoding = decode_responses, encoding
        self.max_value_bytes = max_value_bytes
        self._reader: Optional[asyncio.StreamReader] = None
        self._writer: Optional[asyncio.StreamWriter] = None
        self._lock: Optional[asyncio.Lock] = None

    def _get_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    async def connect(self) -> "AsyncClient":
        async with self._get_lock():
            await self._connect_locked()
        return self

    async def _connect_locked(self) -> None:
        if self._writer is not None:
            return
        try:
            self._reader, self._writer = await asyncio.wait_for(
                asyncio.open_connection(self.host, self.port), self.connect_timeout)
        except asyncio.TimeoutError as e:
            raise CacheTimeoutError("connect to %s:%s timed out" % (self.host, self.port)) from e
        except OSError as e:
            raise CacheConnectionError("cannot connect to %s:%s: %s" % (self.host, self.port, e)) from e
        if self.password:
            try:
                reply = await self._roundtrip(encode_command([b"AUTH", self.password.encode(self.encoding)]))
            except BaseException:
                await self._close_locked()
                raise
            if isinstance(reply, ResponseError):
                await self._close_locked()
                raise reply

    async def _close_locked(self) -> None:
        w, self._reader, self._writer = self._writer, None, None
        if w is not None:
            w.close()
            try:
                await w.wait_closed()
            except Exception:
                pass

    async def aclose(self) -> None:
        async with self._get_lock():
            await self._close_locked()

    async def __aenter__(self) -> "AsyncClient":
        return await self.connect()

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    async def _roundtrip(self, payload: bytes, replies: Optional[int] = None) -> Any:
        try:
            self._writer.write(payload)
            await asyncio.wait_for(self._writer.drain(), self.timeout)
            if replies is None:
                return await asyncio.wait_for(aread_reply(self._reader), self.timeout)
            return [await asyncio.wait_for(aread_reply(self._reader), self.timeout) for _ in range(replies)]
        except asyncio.TimeoutError as e:
            await self._close_locked()
            raise CacheTimeoutError("no reply within %.1fs; connection closed, the command may still be executed" % self.timeout) from e
        except asyncio.CancelledError:
            await self._close_locked()   # a half-read reply would desynchronise the stream
            raise
        except (OSError, EOFError) as e:
            await self._close_locked()
            raise CacheConnectionError("connection lost: %s" % e) from e
        except ProtocolError:
            await self._close_locked()
            raise

    async def _call(self, args: List[bytes], callback: Optional[Callable[[Any], Any]] = None):
        async with self._get_lock():
            await self._connect_locked()
            reply = await self._roundtrip(encode_command(args))
        if isinstance(reply, ResponseError):
            raise reply
        return callback(reply) if callback else reply

    def pipeline(self) -> "AsyncPipeline":
        return AsyncPipeline(self)


class AsyncPipeline(Commands):
    def __init__(self, client: AsyncClient):
        self._client = client
        self._queue: List[Tuple[List[bytes], Optional[Callable[[Any], Any]]]] = []
        self.encoding, self.decode_responses = client.encoding, client.decode_responses
        self.max_value_bytes = client.max_value_bytes

    def _call(self, args, callback=None):
        self._queue.append((args, callback))
        return self   # NOT awaited: queue first, then ``await p.execute()``

    async def execute(self, raise_on_error: bool = True) -> List[Any]:
        queue, self._queue = self._queue, []
        if not queue:
            return []
        c = self._client
        payload = b"".join(encode_command(a) for a, _ in queue)
        async with c._get_lock():
            await c._connect_locked()
            replies = await c._roundtrip(payload, replies=len(queue))
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
