<?php
declare(strict_types=1);

namespace Cumulus13\MsmqCache;

/**
 * Blocking client for mcache-server (MSMQ-backed cache / queue server). One connection per instance.
 * A command that fails with a connection error or timeout is never retried (the server is at-most-once);
 * the connection is closed and re-opened by the next call.
 *
 * Reply mapping for execute(): Status (simple string), int, string (bulk, binary safe), null (nil), array.
 * Server errors are thrown as ResponseException.
 */
final class Client
{
    private const MAX_KEY = 1024;
    private const MAX_LINE = 65536;

    /** @var resource|null */
    private $sock = null;

    public function __construct(
        private string $host = '127.0.0.1',
        private int $port = 6390,
        private ?string $password = null,
        private float $timeout = 10.0,
        private float $connectTimeout = 5.0,
        private int $maxValueBytes = 4_000_000,
    ) {}

    public function __destruct() { $this->close(); }

    public function close(): void
    {
        if (is_resource($this->sock)) { fclose($this->sock); }
        $this->sock = null;
    }

    private function ensure(): void
    {
        if (is_resource($this->sock)) { return; }
        $s = @stream_socket_client("tcp://{$this->host}:{$this->port}", $errno, $err, $this->connectTimeout);
        if ($s === false) {
            throw new ConnectionException("cannot connect to {$this->host}:{$this->port}: $err ($errno)");
        }
        $sec = (int) floor($this->timeout);
        stream_set_timeout($s, $sec, (int) (($this->timeout - $sec) * 1_000_000));
        $this->sock = $s;
        if ($this->password !== null && $this->password !== '') {
            try {
                $r = $this->roundtrip([['AUTH', $this->password]])[0];
            } catch (\Throwable $e) { $this->close(); throw $e; }
            if ($r instanceof ResponseException) { $this->close(); throw $r; }
        }
    }

    /** @param list<list<string>> $cmds @return list<mixed> */
    private function roundtrip(array $cmds): array
    {
        $payload = '';
        foreach ($cmds as $args) {
            $payload .= '*' . count($args) . "\r\n";
            foreach ($args as $a) { $payload .= '$' . strlen($a) . "\r\n" . $a . "\r\n"; }
        }
        try {
            $this->writeAll($payload);
            $out = [];
            foreach ($cmds as $_) { $out[] = $this->readReply(); }
            return $out;
        } catch (\Throwable $e) {
            $this->close();   // unread bytes would be misread as the next reply
            throw $e;
        }
    }

    private function writeAll(string $data): void
    {
        $len = strlen($data);
        for ($off = 0; $off < $len;) {
            $n = @fwrite($this->sock, substr($data, $off));
            if ($n === false || $n === 0) { throw $this->ioError('write failed'); }
            $off += $n;
        }
    }

    private function ioError(string $what): ConnectionException
    {
        $meta = is_resource($this->sock) ? stream_get_meta_data($this->sock) : [];
        if (!empty($meta['timed_out'])) {
            return new TimeoutException("no reply within {$this->timeout}s; connection closed, the command may still be executed");
        }
        return new ConnectionException("connection lost: $what");
    }

    private function readLine(): string
    {
        $line = fgets($this->sock, self::MAX_LINE + 1);
        if ($line === false) { throw $this->ioError('connection closed by server'); }
        if (!str_ends_with($line, "\r\n")) {
            if (strlen($line) > self::MAX_LINE) { throw new ProtocolException('line too long'); }
            throw $this->ioError('truncated line');
        }
        return substr($line, 0, -2);
    }

    private function readExact(int $n): string
    {
        $buf = '';
        while (strlen($buf) < $n) {
            $chunk = fread($this->sock, $n - strlen($buf));
            if ($chunk === false || $chunk === '') { throw $this->ioError('connection closed mid-reply'); }
            $buf .= $chunk;
        }
        return $buf;
    }

    private static function num(string $s, string $what): int
    {
        if (!preg_match('/^-?\d+$/', $s)) { throw new ProtocolException("invalid $what: " . substr($s, 0, 40)); }
        return (int) $s;
    }

    private function readReply(): mixed
    {
        $line = $this->readLine();
        if ($line === '') { throw new ProtocolException('empty reply line'); }
        $rest = substr($line, 1);
        switch ($line[0]) {
            case '+': return new Status($rest);
            case '-': return self::serverError($rest);
            case ':': return self::num($rest, 'integer');
            case '$':
                $n = self::num($rest, 'bulk length');
                if ($n < 0) { return null; }
                $data = $this->readExact($n + 2);
                return substr($data, 0, $n);
            case '*':
                $n = self::num($rest, 'array length');
                if ($n < 0) { return null; }
                $items = [];
                for ($i = 0; $i < $n; $i++) { $items[] = $this->readReply(); }
                return $items;
        }
        throw new ProtocolException("unknown reply type '{$line[0]}'");
    }

    private static function serverError(string $m): ResponseException
    {
        return (str_starts_with($m, 'NOAUTH') || str_starts_with($m, 'WRONGPASS')) ? new AuthenticationException($m) : new ResponseException($m);
    }

    /** @param list<list<string>> $cmds */
    private function exec(array $cmds): array
    {
        $this->ensure();
        return $this->roundtrip($cmds);
    }

    private static function str(mixed $v): string
    {
        if (is_string($v)) { return $v; }
        if (is_int($v)) { return (string) $v; }
        throw new \InvalidArgumentException('unsupported argument type ' . get_debug_type($v) . ' (use string or int)');
    }

    private function key(mixed $k): string
    {
        $s = self::str($k);
        if (strlen($s) < 1 || strlen($s) > self::MAX_KEY) { throw new \InvalidArgumentException('key length must be 1..' . self::MAX_KEY . ' bytes'); }
        return $s;
    }

    private function val(mixed $v): string
    {
        $s = self::str($v);
        if (strlen($s) > $this->maxValueBytes) { throw new \InvalidArgumentException('value is ' . strlen($s) . " bytes; limit is {$this->maxValueBytes}"); }
        return $s;
    }

    private function one(array $cmd): mixed
    {
        $r = $this->exec([$cmd])[0];
        if ($r instanceof ResponseException) { throw $r; }
        return $r;
    }

    private static function bytesList(mixed $r): array
    {
        if (!is_array($r)) { throw new ProtocolException('expected array reply'); }
        return $r;
    }

    // ---- commands -----------------------------------------------------------
    /** Run any command: $c->execute('INFO'). */
    public function execute(string|int ...$args): mixed { return $this->one(array_map(self::str(...), $args)); }

    public function ping(): bool { $r = $this->one(['PING']); return $r instanceof Status && $r->text === 'PONG'; }

    /** @return list<string> */
    private function setArgs(string|int $key, string|int $value, ?int $ex, ?int $px, bool $nx, bool $xx, bool $keepTtl): array
    {
        if ($nx && $xx) { throw new \InvalidArgumentException('nx and xx are mutually exclusive'); }
        if ($ex !== null && $px !== null) { throw new \InvalidArgumentException('use either ex or px'); }
        if (($ex !== null && $ex <= 0) || ($px !== null && $px <= 0)) { throw new \InvalidArgumentException('ex/px must be positive'); }
        $a = ['SET', $this->key($key), $this->val($value)];
        if ($ex !== null) { array_push($a, 'EX', (string) $ex); }
        if ($px !== null) { array_push($a, 'PX', (string) $px); }
        if ($nx) { $a[] = 'NX'; }
        if ($xx) { $a[] = 'XX'; }
        if ($keepTtl) { $a[] = 'KEEPTTL'; }
        return $a;
    }

    /** true = stored, false = NX/XX prevented it. ex = seconds, px = milliseconds. */
    public function set(string|int $key, string|int $value, ?int $ex = null, ?int $px = null, bool $nx = false, bool $xx = false, bool $keepTtl = false): bool
    {
        return $this->one($this->setArgs($key, $value, $ex, $px, $nx, $xx, $keepTtl)) !== null;
    }

    /** null for a missing key; an empty value is ''. */
    public function get(string|int $key): ?string { return $this->one(['GET', $this->key($key)]); }

    /** @return list<?string> */
    public function mget(string|int ...$keys): array
    {
        if (!$keys) { throw new \InvalidArgumentException('keys must not be empty'); }
        return self::bytesList($this->one(['MGET', ...array_map($this->key(...), $keys)]));
    }

    /** Not atomic on the server (one MSMQ message per key). @param array<string,string|int> $entries */
    public function mset(array $entries): void
    {
        if (!$entries) { throw new \InvalidArgumentException('entries must not be empty'); }
        $a = ['MSET'];
        foreach ($entries as $k => $v) { array_push($a, $this->key((string) $k), $this->val($v)); }
        $this->one($a);
    }

    public function del(string|int ...$keys): int { return $this->one(['DEL', ...array_map($this->key(...), $keys)]); }
    public function exists(string|int ...$keys): int { return $this->one(['EXISTS', ...array_map($this->key(...), $keys)]); }

    /** Re-sends the value on the server (MSMQ messages are immutable). */
    public function expire(string|int $key, int $seconds): bool
    {
        if ($seconds <= 0) { throw new \InvalidArgumentException('seconds must be positive'); }
        return $this->one(['EXPIRE', $this->key($key), (string) $seconds]) === 1;
    }

    /** Seconds left; -1 no expiry; -2 no such key. */
    public function ttl(string|int $key): int { return $this->one(['TTL', $this->key($key)]); }
    public function incr(string|int $key): int { return $this->one(['INCR', $this->key($key)]); }
    public function decr(string|int $key): int { return $this->one(['DECR', $this->key($key)]); }
    public function incrBy(string|int $key, int $n): int { return $this->one(['INCRBY', $this->key($key), (string) $n]); }
    /** O(n) on the server. @return list<string> */
    public function keys(string $pattern = '*'): array { return self::bytesList($this->one(['KEYS', $pattern])); }
    public function dbsize(): int { return $this->one(['DBSIZE']); }
    public function flushdb(): void { $this->one(['FLUSHDB']); }

    /** @return list<string> */
    private function qpushArgs(string $queue, string|int $value, ?int $priority, ?int $ttl): array
    {
        $a = ['QPUSH', $queue, $this->val($value)];
        if ($priority !== null) {
            if ($priority < 0 || $priority > 7) { throw new \InvalidArgumentException('priority must be 0..7'); }
            array_push($a, 'PRIORITY', (string) $priority);
        }
        if ($ttl !== null) {
            if ($ttl <= 0) { throw new \InvalidArgumentException('ttl must be positive'); }
            array_push($a, 'TTL', (string) $ttl);
        }
        return $a;
    }

    /** Native MSMQ queue (auto-created). priority 0..7 (7 highest), ttl seconds. */
    public function qpush(string $queue, string|int $value, ?int $priority = null, ?int $ttl = null): void
    {
        $this->one($this->qpushArgs($queue, $value, $priority, $ttl));
    }
    /** Non-blocking: null when empty. */
    public function qpop(string $queue): ?string { return $this->one(['QPOP', $queue]); }
    public function qpeek(string $queue): ?string { return $this->one(['QPEEK', $queue]); }
    public function qlen(string $queue): int { return $this->one(['QLEN', $queue]); }
    /** @return list<string> */
    public function qlist(): array { return self::bytesList($this->one(['QLIST'])); }

    /** @return array<string,array<string,string>> */
    public function info(): array
    {
        $out = [];
        $section = 'default';
        foreach (preg_split('/\r?\n/', (string) $this->one(['INFO'])) as $line) {
            $line = trim($line);
            if ($line === '') { continue; }
            if ($line[0] === '#') { $section = strtolower(trim(ltrim($line, '# '))); continue; }
            [$k, $v] = explode(':', $line, 2);
            $out[$section][$k] = $v;
        }
        return $out;
    }

    public function pipeline(): Pipeline { return new Pipeline($this, fn (array $cmds) => $this->exec($cmds)); }

    /** @internal used by Pipeline */
    public function buildSet(string|int $key, string|int $value, ?int $ex, ?int $px, bool $nx, bool $xx): array { return $this->setArgs($key, $value, $ex, $px, $nx, $xx, false); }
    /** @internal */
    public function buildKey(string|int $key): string { return $this->key($key); }
    /** @internal */
    public function buildArgs(array $args): array { return array_map(self::str(...), $args); }
}

/** Queues commands and sends them in one write. The server still runs them one after another. */
final class Pipeline
{
    /** @var list<list<string>> */
    private array $cmds = [];

    public function __construct(private Client $c, private \Closure $run) {}

    public function cmd(string|int ...$args): self { $this->cmds[] = $this->c->buildArgs($args); return $this; }
    public function set(string|int $key, string|int $value, ?int $ex = null, ?int $px = null, bool $nx = false, bool $xx = false): self
    {
        $this->cmds[] = $this->c->buildSet($key, $value, $ex, $px, $nx, $xx);
        return $this;
    }
    public function get(string|int $key): self { $this->cmds[] = ['GET', $this->c->buildKey($key)]; return $this; }
    public function incr(string|int $key): self { $this->cmds[] = ['INCR', $this->c->buildKey($key)]; return $this; }

    /** Replies in order; server errors appear as ResponseException objects in the list. */
    public function execLenient(): array
    {
        $cmds = $this->cmds;
        $this->cmds = [];
        return $cmds ? ($this->run)($cmds) : [];
    }

    /** Like execLenient() but throws the first server error. */
    public function exec(): array
    {
        $out = $this->execLenient();
        foreach ($out as $r) { if ($r instanceof ResponseException) { throw $r; } }
        return $out;
    }
}
