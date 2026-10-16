#!/usr/bin/env python3

# File: sdk/python/src/msmqcache/errors.py
# Author: Hadi Cahyadi <cumulus13@gmail.com>
# Date: 2026-10-17
# Description: msmq-cache - Exceptions raised by the msmqcache client.
# License: MIT

"""Exceptions raised by the msmqcache client."""


class MsmqCacheError(Exception):
    """Base class of every error raised by this package."""


class CacheConnectionError(MsmqCacheError):
    """The TCP connection could not be opened or was lost. The command may or may not have been executed."""


class CacheTimeoutError(CacheConnectionError):
    """No reply within the client timeout. The connection is closed; the command may still be executed by the server."""


class ProtocolError(MsmqCacheError):
    """The server sent something that is not valid RESP."""


class ResponseError(MsmqCacheError):
    """The server answered with an error reply (``-ERR ...``)."""


class AuthenticationError(ResponseError):
    """``NOAUTH`` / ``WRONGPASS`` replies."""
