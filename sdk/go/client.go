// File: sdk\go\client.go
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-19
// Description: Package msmqcache is a zero-dependency client for mcache-server (MSMQ-backed cache and queue server).
// License: MIT

// Package msmqcache is a zero-dependency client for mcache-server (MSMQ-backed cache and queue server).
package msmqcache

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

// Options configures a Client. Zero values select the defaults.
type Options struct {
	Addr           string        // default "127.0.0.1:6390"
	Password       string        // sent with AUTH on every (re)connect when non-empty
	Timeout        time.Duration // per command reply timeout, default 10s (a ctx deadline wins if sooner)
	ConnectTimeout time.Duration // default 5s
	MaxValueBytes  int           // default 4,000,000 (MSMQ message limit is 4 MB)
}

// Client is safe for concurrent use; commands are serialised over one connection.
// A command that fails with a connection error or timeout is never retried (the server is at-most-once);
// the connection is closed and re-opened by the next call.
type Client struct {
	o    Options
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
}

// New returns a Client; it connects lazily.
func New(o Options) *Client {
	if o.Addr == "" {
		o.Addr = "127.0.0.1:6390"
	}
	if o.Timeout == 0 {
		o.Timeout = 10 * time.Second
	}
	if o.ConnectTimeout == 0 {
		o.ConnectTimeout = 5 * time.Second
	}
	if o.MaxValueBytes == 0 {
		o.MaxValueBytes = 4_000_000
	}
	return &Client{o: o}
}

// Close closes the connection (a later call reconnects).
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked()
}

func (c *Client) closeLocked() error {
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn, c.r = nil, nil
	return err
}

func (c *Client) connectLocked(ctx context.Context) error {
	if c.conn != nil {
		return nil
	}
	d := net.Dialer{Timeout: c.o.ConnectTimeout, KeepAlive: 30 * time.Second}
	conn, err := d.DialContext(ctx, "tcp", c.o.Addr)
	if err != nil {
		return &ConnectionError{Err: err, Timeout: isTimeout(err)}
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	c.conn, c.r = conn, bufio.NewReader(conn)
	if c.o.Password != "" {
		reps, err := c.roundtripLocked(ctx, [][][]byte{{[]byte("AUTH"), []byte(c.o.Password)}})
		if err != nil {
			return err
		}
		if e, ok := reps[0].(error); ok {
			_ = c.closeLocked()
			return e
		}
	}
	return nil
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout() || errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, context.DeadlineExceeded)
}

// roundtripLocked writes all commands in one go and reads one reply per command.
func (c *Client) roundtripLocked(ctx context.Context, cmds [][][]byte) ([]any, error) {
	deadline := time.Now().Add(c.o.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = c.conn.SetDeadline(deadline)
	var buf []byte
	for _, cmd := range cmds {
		buf = appendCommand(buf, cmd)
	}
	fail := func(err error) ([]any, error) {
		_ = c.closeLocked() // unread bytes would be misread as the next reply
		var pe *ProtocolError
		if errors.As(err, &pe) {
			return nil, err
		}
		return nil, &ConnectionError{Err: err, Timeout: isTimeout(err)}
	}
	if _, err := c.conn.Write(buf); err != nil {
		return fail(err)
	}
	out := make([]any, 0, len(cmds))
	for range cmds {
		rep, err := readReply(c.r)
		if err != nil {
			return fail(err)
		}
		out = append(out, rep)
	}
	return out, nil
}

func (c *Client) exec(ctx context.Context, cmds [][][]byte) ([]any, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.connectLocked(ctx); err != nil {
		return nil, err
	}
	return c.roundtripLocked(ctx, cmds)
}

func (c *Client) args(a []any) ([][]byte, error) {
	out := make([][]byte, len(a))
	for i, v := range a {
		b, err := toBytes(v)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}

// Do runs any command, e.g. c.Do(ctx, "INFO"). Reply types: string (simple), int64, []byte (bulk, nil for nil),
// []any. A server error reply is returned as the error.
func (c *Client) Do(ctx context.Context, args ...any) (any, error) {
	b, err := c.args(args)
	if err != nil {
		return nil, err
	}
	reps, err := c.exec(ctx, [][][]byte{b})
	if err != nil {
		return nil, err
	}
	if e, ok := reps[0].(error); ok {
		return nil, e
	}
	return reps[0], nil
}

func (c *Client) checkKey(k string) error {
	if len(k) < 1 || len(k) > 1024 {
		return fmt.Errorf("msmqcache: key length must be 1..1024 bytes, got %d", len(k))
	}
	return nil
}

func (c *Client) checkVal(v []byte) error {
	if len(v) > c.o.MaxValueBytes {
		return fmt.Errorf("msmqcache: value is %d bytes, limit is %d", len(v), c.o.MaxValueBytes)
	}
	return nil
}

func asBytes(v any) ([]byte, error) {
	if v == nil {
		return nil, ErrNil
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, &ProtocolError{fmt.Sprintf("expected bulk reply, got %T", v)}
	}
	return b, nil
}

func asInt(v any, err error) (int64, error) {
	if err != nil {
		return 0, err
	}
	n, ok := v.(int64)
	if !ok {
		return 0, &ProtocolError{fmt.Sprintf("expected integer reply, got %T", v)}
	}
	return n, nil
}

func asBytesList(v any, err error) ([][]byte, error) {
	if err != nil {
		return nil, err
	}
	items, ok := v.([]any)
	if !ok {
		return nil, &ProtocolError{fmt.Sprintf("expected array reply, got %T", v)}
	}
	out := make([][]byte, len(items))
	for i, it := range items {
		if it != nil {
			if b, ok := it.([]byte); ok {
				out[i] = b
			}
		}
	}
	return out, nil
}

// Ping returns nil when the server answers PONG.
func (c *Client) Ping(ctx context.Context) error {
	v, err := c.Do(ctx, "PING")
	if err != nil {
		return err
	}
	if s, _ := v.(string); s != "PONG" {
		return &ProtocolError{fmt.Sprintf("unexpected PING reply %v", v)}
	}
	return nil
}

// SetOptions of SET. EX is seconds, PX milliseconds; zero means unset.
type SetOptions struct {
	EX, PX  int64
	NX, XX  bool
	KeepTTL bool
}

func (c *Client) setArgs(key string, value any, o SetOptions) ([]any, error) {
	if err := c.checkKey(key); err != nil {
		return nil, err
	}
	vb, err := toBytes(value)
	if err != nil {
		return nil, err
	}
	if err := c.checkVal(vb); err != nil {
		return nil, err
	}
	if o.NX && o.XX {
		return nil, errors.New("msmqcache: NX and XX are mutually exclusive")
	}
	if o.EX != 0 && o.PX != 0 {
		return nil, errors.New("msmqcache: use either EX or PX")
	}
	if o.EX < 0 || o.PX < 0 {
		return nil, errors.New("msmqcache: EX/PX must be positive")
	}
	a := []any{"SET", key, vb}
	if o.EX > 0 {
		a = append(a, "EX", o.EX)
	}
	if o.PX > 0 {
		a = append(a, "PX", o.PX)
	}
	if o.NX {
		a = append(a, "NX")
	}
	if o.XX {
		a = append(a, "XX")
	}
	if o.KeepTTL {
		a = append(a, "KEEPTTL")
	}
	return a, nil
}

// Set returns true when the value was stored, false when NX/XX prevented it.
func (c *Client) Set(ctx context.Context, key string, value any, o SetOptions) (bool, error) {
	a, err := c.setArgs(key, value, o)
	if err != nil {
		return false, err
	}
	v, err := c.Do(ctx, a...)
	if err != nil {
		return false, err
	}
	return v != nil, nil
}

// Get returns ErrNil for a missing key; an empty value is a non-nil empty slice.
func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	if err := c.checkKey(key); err != nil {
		return nil, err
	}
	v, err := c.Do(ctx, "GET", key)
	if err != nil {
		return nil, err
	}
	return asBytes(v)
}

// MGet returns one entry per key; missing keys are nil slices.
func (c *Client) MGet(ctx context.Context, keys ...string) ([][]byte, error) {
	a := []any{"MGET"}
	for _, k := range keys {
		if err := c.checkKey(k); err != nil {
			return nil, err
		}
		a = append(a, k)
	}
	return asBytesList(c.Do(ctx, a...))
}

// MSet is not atomic on the server (one MSMQ message per key).
func (c *Client) MSet(ctx context.Context, kv map[string]any) error {
	a := []any{"MSET"}
	for k, v := range kv {
		if err := c.checkKey(k); err != nil {
			return err
		}
		vb, err := toBytes(v)
		if err != nil {
			return err
		}
		if err := c.checkVal(vb); err != nil {
			return err
		}
		a = append(a, k, vb)
	}
	_, err := c.Do(ctx, a...)
	return err
}

func (c *Client) keysCmd(ctx context.Context, cmd string, keys []string) (int64, error) {
	a := []any{cmd}
	for _, k := range keys {
		if err := c.checkKey(k); err != nil {
			return 0, err
		}
		a = append(a, k)
	}
	return asInt(c.Do(ctx, a...))
}

func (c *Client) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.keysCmd(ctx, "DEL", keys)
}
func (c *Client) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.keysCmd(ctx, "EXISTS", keys)
}

// Expire re-sends the value on the server (MSMQ messages are immutable).
func (c *Client) Expire(ctx context.Context, key string, seconds int64) (bool, error) {
	n, err := asInt(c.Do(ctx, "EXPIRE", key, seconds))
	return n == 1, err
}

// TTL: seconds left; -1 no expiry; -2 no such key.
func (c *Client) TTL(ctx context.Context, key string) (int64, error) {
	return asInt(c.Do(ctx, "TTL", key))
}
func (c *Client) Incr(ctx context.Context, key string) (int64, error) {
	return asInt(c.Do(ctx, "INCR", key))
}
func (c *Client) Decr(ctx context.Context, key string) (int64, error) {
	return asInt(c.Do(ctx, "DECR", key))
}
func (c *Client) IncrBy(ctx context.Context, key string, n int64) (int64, error) {
	return asInt(c.Do(ctx, "INCRBY", key, n))
}

// Keys is O(n) on the server.
func (c *Client) Keys(ctx context.Context, pattern string) ([][]byte, error) {
	return asBytesList(c.Do(ctx, "KEYS", pattern))
}
func (c *Client) DBSize(ctx context.Context) (int64, error) { return asInt(c.Do(ctx, "DBSIZE")) }
func (c *Client) FlushDB(ctx context.Context) error         { _, err := c.Do(ctx, "FLUSHDB"); return err }

// QPushOptions: Priority 0..7 (7 highest; -1 = server default), TTLSeconds 0 = none.
type QPushOptions struct {
	Priority   int
	TTLSeconds int64
}

func (c *Client) qpushArgs(queue string, value any, o QPushOptions) ([]any, error) {
	vb, err := toBytes(value)
	if err != nil {
		return nil, err
	}
	if err := c.checkVal(vb); err != nil {
		return nil, err
	}
	a := []any{"QPUSH", queue, vb}
	if o.Priority < -1 || o.Priority > 7 {
		return nil, errors.New("msmqcache: priority must be 0..7")
	}
	if o.Priority >= 0 {
		a = append(a, "PRIORITY", o.Priority)
	}
	if o.TTLSeconds < 0 {
		return nil, errors.New("msmqcache: TTLSeconds must be positive")
	}
	if o.TTLSeconds > 0 {
		a = append(a, "TTL", o.TTLSeconds)
	}
	return a, nil
}

// QPush appends to a native MSMQ queue. Use QPushOptions{Priority: -1} for the default priority.
func (c *Client) QPush(ctx context.Context, queue string, value any, o QPushOptions) error {
	a, err := c.qpushArgs(queue, value, o)
	if err != nil {
		return err
	}
	_, err = c.Do(ctx, a...)
	return err
}

// QPop is non-blocking: ErrNil when the queue is empty.
func (c *Client) QPop(ctx context.Context, queue string) ([]byte, error) {
	v, err := c.Do(ctx, "QPOP", queue)
	if err != nil {
		return nil, err
	}
	return asBytes(v)
}
func (c *Client) QPeek(ctx context.Context, queue string) ([]byte, error) {
	v, err := c.Do(ctx, "QPEEK", queue)
	if err != nil {
		return nil, err
	}
	return asBytes(v)
}
func (c *Client) QLen(ctx context.Context, queue string) (int64, error) {
	return asInt(c.Do(ctx, "QLEN", queue))
}
func (c *Client) QList(ctx context.Context) ([][]byte, error) { return asBytesList(c.Do(ctx, "QLIST")) }

// Info returns INFO parsed into section -> key -> value (all values as strings).
func (c *Client) Info(ctx context.Context) (map[string]map[string]string, error) {
	v, err := c.Do(ctx, "INFO")
	if err != nil {
		return nil, err
	}
	b, err := asBytes(v)
	if err != nil {
		return nil, err
	}
	out := map[string]map[string]string{}
	section := "default"
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			section = strings.ToLower(strings.TrimSpace(strings.TrimLeft(line, "# ")))
			continue
		}
		k, v, _ := strings.Cut(line, ":")
		if out[section] == nil {
			out[section] = map[string]string{}
		}
		out[section][k] = v
	}
	return out, nil
}

// Pipeline sends several commands in one write. The server still runs them one after another.
type Pipeline struct {
	c    *Client
	cmds [][]any
	err  error
}

func (c *Client) Pipeline() *Pipeline { return &Pipeline{c: c} }

// Cmd queues any command.
func (p *Pipeline) Cmd(args ...any) *Pipeline { p.cmds = append(p.cmds, args); return p }
func (p *Pipeline) Set(key string, value any, o SetOptions) *Pipeline {
	a, err := p.c.setArgs(key, value, o)
	if err != nil && p.err == nil {
		p.err = err
	}
	return p.Cmd(a...)
}
func (p *Pipeline) Get(key string) *Pipeline  { return p.Cmd("GET", key) }
func (p *Pipeline) Incr(key string) *Pipeline { return p.Cmd("INCR", key) }
func (p *Pipeline) Del(keys ...string) *Pipeline {
	a := []any{"DEL"}
	for _, k := range keys {
		a = append(a, k)
	}
	return p.Cmd(a...)
}

// Exec returns one raw reply per command (server errors appear as error values in the slice) and the first
// server error, if any. Connection-level failures return (nil, err).
func (p *Pipeline) Exec(ctx context.Context) ([]any, error) {
	cmds, perr := p.cmds, p.err
	p.cmds, p.err = nil, nil
	if perr != nil {
		return nil, perr
	}
	if len(cmds) == 0 {
		return nil, nil
	}
	enc := make([][][]byte, len(cmds))
	for i, a := range cmds {
		b, err := p.c.args(a)
		if err != nil {
			return nil, err
		}
		enc[i] = b
	}
	reps, err := p.c.exec(ctx, enc)
	if err != nil {
		return nil, err
	}
	var first error
	for _, r := range reps {
		if e, ok := r.(error); ok && first == nil {
			first = e
		}
	}
	return reps, first
}
