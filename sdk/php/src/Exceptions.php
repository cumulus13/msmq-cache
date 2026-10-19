<?php
declare(strict_types=1);

namespace Cumulus13\MsmqCache;

/** Base class of every error thrown by this client. */
class MsmqCacheException extends \RuntimeException {}

/** TCP failure; the command may or may not have run. */
class ConnectionException extends MsmqCacheException {}

/** No reply in time. The connection was closed; the command may still be executed by the server. */
class TimeoutException extends ConnectionException {}

class ProtocolException extends MsmqCacheException {}

/** Server replied with -ERR ... */
class ResponseException extends MsmqCacheException {}

/** NOAUTH / WRONGPASS. */
class AuthenticationException extends ResponseException {}

/** A simple-string reply such as +OK (bulk replies are plain PHP strings, nil is null). */
final class Status
{
    public function __construct(public readonly string $text) {}
    public function __toString(): string { return $this->text; }
}
