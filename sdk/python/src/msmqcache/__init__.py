"""msmqcache - Python client for mcache-server (MSMQ-backed cache / queue server)."""
from .aio import AsyncClient, AsyncPipeline
from .client import Client, Pipeline
from .errors import (AuthenticationError, CacheConnectionError, CacheTimeoutError, MsmqCacheError,
                     ProtocolError, ResponseError)

__all__ = [
    "Client", "Pipeline", "AsyncClient", "AsyncPipeline",
    "MsmqCacheError", "CacheConnectionError", "CacheTimeoutError", "ProtocolError", "ResponseError",
    "AuthenticationError",
]
__version__ = "1.0.0"
