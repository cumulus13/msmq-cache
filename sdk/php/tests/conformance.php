<?php
declare(strict_types=1);
// Dependency-free runner: sdk/conformance/run.sh php tests/conformance.php
require __DIR__ . '/../src/Exceptions.php';
require __DIR__ . '/../src/Client.php';

use Cumulus13\MsmqCache\{AuthenticationException, Client, ConnectionException, ResponseException, Status, TimeoutException};

$port = (int) (getenv('MCACHE_TEST_PORT') ?: 0);
$authPort = (int) (getenv('MCACHE_TEST_AUTH_PORT') ?: 0);
$password = (string) getenv('MCACHE_TEST_PASSWORD');
if (!$port || !$authPort) { fwrite(STDERR, "run through sdk/conformance/run.sh\n"); exit(2); }

function c(array $o = []): Client { global $port; return new Client('127.0.0.1', $port, null, $o['timeout'] ?? 2.0); }
function eq(mixed $want, mixed $got, string $m = ''): void { if ($want !== $got) { throw new Exception("$m want " . var_export($want, true) . ' got ' . var_export($got, true)); } }
function throwsA(string $cls, callable $f): void {
    try { $f(); } catch (Throwable $e) { if ($e instanceof $cls) { return; } throw new Exception("want $cls got " . get_class($e) . ': ' . $e->getMessage()); }
    throw new Exception("expected $cls");
}
function last(Client $c): array { return $c->execute('LASTCMD'); }

$tests = [
 'ping' => fn () => eq(true, c()->ping()),
 'set_get_delete' => function () { $c = c(); eq(true, $c->set('p:k', 'v')); eq('v', $c->get('p:k')); eq(1, $c->del('p:k', 'p:missing')); eq(null, $c->get('p:k')); },
 'binary_safe' => function () {
     $c = c(); $blob = str_repeat("\x00\r\n\$5\r\n*1\r\n\xff", 100);
     $c->set('p:blob', $blob); eq($blob, $c->get('p:blob'));
 },
 'nil_vs_empty' => function () { $c = c(); $c->set('p:empty', ''); eq('', $c->get('p:empty')); eq(null, $c->get('p:never')); },
 'set_options' => function () {
     $c = c(); $c->del('p:opt');
     eq(true, $c->set('p:opt', 1, ex: 30, nx: true));
     eq(['SET', 'p:opt', '1', 'EX', '30', 'NX'], last($c));
     eq(false, $c->set('p:opt', 2, nx: true));
     eq(false, $c->set('p:nokey', 2, xx: true));
     $c->set('p:opt', 'x', px: 1500);
     eq(['PX', '1500'], array_slice(last($c), 3, 2));
     throwsA(InvalidArgumentException::class, fn () => $c->set('k', 'v', nx: true, xx: true));
     throwsA(InvalidArgumentException::class, fn () => $c->set('', 'v'));
     throwsA(InvalidArgumentException::class, fn () => $c->set('k', 'v', ex: 0));
 },
 'numbers_and_multi' => function () {
     $c = c(); $c->del('p:cnt');
     eq(1, $c->incr('p:cnt')); eq(10, $c->incrBy('p:cnt', 9));
     $c->mset(['pm:1' => 'a', 'pm:2' => 'b']);
     eq(['a', null, 'b'], $c->mget('pm:1', 'pm:none', 'pm:2'));
     $k = $c->keys('pm:*'); sort($k); eq(['pm:1', 'pm:2'], $k);
 },
 'server_error' => function () {
     $c = c(); $c->set('p:str', 'abc');
     try { $c->incr('p:str'); throw new Exception('expected ResponseException'); }
     catch (ResponseException $e) { if (!str_contains($e->getMessage(), 'not an integer')) { throw new Exception($e->getMessage()); } }
     throwsA(ResponseException::class, fn () => $c->execute('BOGUS'));
     eq(true, $c->ping(), 'connection must stay usable');
 },
 'auth' => function () {
     global $authPort, $password;
     eq(true, (new Client('127.0.0.1', $authPort, $password, 2.0))->ping());
     throwsA(AuthenticationException::class, fn () => (new Client('127.0.0.1', $authPort, 'wrong', 2.0))->ping());
     throwsA(AuthenticationException::class, fn () => (new Client('127.0.0.1', $authPort, null, 2.0))->ping());
 },
 'timeout_reconnect' => function () {
     $c = c(['timeout' => 0.3]);
     throwsA(TimeoutException::class, fn () => $c->execute('SLOW', 1));
     eq(true, $c->ping(), 'fresh connection');
 },
 'dropped_connection' => function () {
     $c = c();
     try { $c->execute('DROP'); throw new Exception('expected exception'); }
     catch (TimeoutException $e) { throw new Exception('must not be a timeout'); }
     catch (ConnectionException $e) { /* expected */ }
     eq(true, $c->ping());
 },
 'pipeline' => function () {
     $c = c(); $c->del('pp:1', 'pp:2', 'pp:cnt');
     $r = $c->pipeline()->set('pp:1', 'a')->set('pp:2', 'b')->get('pp:1')->incr('pp:cnt')->exec();
     eq('a', $r[2]); eq(1, $r[3]);
     eq(1, count($c->pipeline()->get('pp:1')->exec()));
     $m = $c->pipeline()->set('pp:s', 'abc')->cmd('INCR', 'pp:s')->get('pp:s')->execLenient();
     eq(true, $m[1] instanceof ResponseException); eq('abc', $m[2]);
     throwsA(ResponseException::class, fn () => $c->pipeline()->cmd('INCR', 'pp:s')->exec());
 },
 'queues' => function () {
     $c = c(); $c->qpush('p:jobs', 'a', priority: 5, ttl: 60);
     eq(['QPUSH', 'p:jobs', 'a', 'PRIORITY', '5', 'TTL', '60'], last($c));
     eq('a', $c->qpop('p:jobs')); eq(null, $c->qpop('p:jobs'));
     throwsA(InvalidArgumentException::class, fn () => $c->qpush('q', 'v', priority: 8));
 },
 'info' => function () { eq('1.0.0', c()->info()['server']['mcache_version']); },
];

$failed = 0;
foreach ($tests as $name => $t) {
    try { $t(); echo "ok   $name\n"; }
    catch (Throwable $e) { $failed++; echo "FAIL $name: " . $e->getMessage() . "\n"; }
}
echo (count($tests) - $failed) . '/' . count($tests) . " passed\n";
exit($failed ? 1 : 0);
