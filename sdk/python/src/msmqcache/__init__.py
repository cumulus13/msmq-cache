#!/usr/bin/env python3

# File: sdk/python/src/msmqcache/__init__.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-17
# Description: msmqcache - Python client for mcache-server (MSMQ-backed cache / queue server).
# License: MIT

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
