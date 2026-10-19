// File: sdk\go\conformance_test.go
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-19
// Description: 
// License: MIT

package msmqcache

import (
	"bytes"
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func port(t *testing.T, env string) string {
	p := os.Getenv(env)
	if p == "" {
		t.Skipf("%s not set: run through sdk/conformance/run.sh", env)
	}
	return "127.0.0.1:" + p
}

func client(t *testing.T, o Options) *Client {
	if o.Addr == "" {
		o.Addr = port(t, "MCACHE_TEST_PORT")
	}
	if o.Timeout == 0 {
		o.Timeout = 2 * time.Second
	}
	c := New(o)
	t.Cleanup(func() { c.Close() })
	return c
}

func lastCmd(t *testing.T, c *Client) []string {
	v, err := c.Do(context.Background(), "LASTCMD")
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, it := range v.([]any) {
		out = append(out, string(it.([]byte)))
	}
	return out
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	if err := client(t, Options{}).Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestSetGetDelete(t *testing.T) {
	c := client(t, Options{})
	if ok, err := c.Set(ctx, "g:k", "v", SetOptions{}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if v, err := c.Get(ctx, "g:k"); err != nil || string(v) != "v" {
		t.Fatal(string(v), err)
	}
	if n, _ := c.Del(ctx, "g:k", "g:missing"); n != 1 {
		t.Fatal(n)
	}
	if _, err := c.Get(ctx, "g:k"); !errors.Is(err, ErrNil) {
		t.Fatal("want ErrNil, got", err)
	}
}

func TestBinarySafe(t *testing.T) {
	c := client(t, Options{})
	blob := bytes.Repeat([]byte{0, 13, 10, '$', '5', 13, 10, '*', '1', 13, 10, 255}, 100)
	c.Set(ctx, "g:blob", blob, SetOptions{})
	got, err := c.Get(ctx, "g:blob")
	if err != nil || !bytes.Equal(got, blob) {
		t.Fatal("binary value did not round-trip", err)
	}
}

func TestNilVsEmpty(t *testing.T) {
	c := client(t, Options{})
	c.Set(ctx, "g:empty", "", SetOptions{})
	v, err := c.Get(ctx, "g:empty")
	if err != nil || v == nil || len(v) != 0 {
		t.Fatalf("empty value must be a non-nil empty slice, got %#v %v", v, err)
	}
	if _, err := c.Get(ctx, "g:never"); !errors.Is(err, ErrNil) {
		t.Fatal(err)
	}
}

func TestSetOptions(t *testing.T) {
	c := client(t, Options{})
	c.Del(ctx, "g:opt")
	ok, _ := c.Set(ctx, "g:opt", 1, SetOptions{EX: 30, NX: true})
	if !ok || !reflect.DeepEqual(lastCmd(t, c), []string{"SET", "g:opt", "1", "EX", "30", "NX"}) {
		t.Fatal(ok, lastCmd(t, c))
	}
	if ok, _ := c.Set(ctx, "g:opt", 2, SetOptions{NX: true}); ok {
		t.Fatal("NX on existing key must be false")
	}
	if ok, _ := c.Set(ctx, "g:nokey", 2, SetOptions{XX: true}); ok {
		t.Fatal("XX on missing key must be false")
	}
	c.Set(ctx, "g:opt", "x", SetOptions{PX: 1500})
	if l := lastCmd(t, c); !reflect.DeepEqual(l[3:], []string{"PX", "1500"}) {
		t.Fatal(l)
	}
	for _, bad := range []func() error{
		func() error { _, e := c.Set(ctx, "k", "v", SetOptions{NX: true, XX: true}); return e },
		func() error { _, e := c.Set(ctx, "", "v", SetOptions{}); return e },
		func() error { _, e := c.Set(ctx, "k", "v", SetOptions{EX: 1, PX: 1}); return e },
		func() error { _, e := c.Set(ctx, "k", nil, SetOptions{}); return e },
	} {
		if bad() == nil {
			t.Fatal("expected client-side validation error")
		}
	}
}

func TestNumbersAndMulti(t *testing.T) {
	c := client(t, Options{})
	c.Del(ctx, "g:cnt")
	if n, _ := c.Incr(ctx, "g:cnt"); n != 1 {
		t.Fatal(n)
	}
	if n, _ := c.IncrBy(ctx, "g:cnt", 9); n != 10 {
		t.Fatal(n)
	}
	if err := c.MSet(ctx, map[string]any{"gm:1": "a", "gm:2": "b"}); err != nil {
		t.Fatal(err)
	}
	vals, _ := c.MGet(ctx, "gm:1", "gm:none", "gm:2")
	if string(vals[0]) != "a" || vals[1] != nil || string(vals[2]) != "b" {
		t.Fatalf("%q", vals)
	}
	keys, _ := c.Keys(ctx, "gm:*")
	if len(keys) != 2 {
		t.Fatal(keys)
	}
}

func TestServerError(t *testing.T) {
	c := client(t, Options{})
	c.Set(ctx, "g:str", "abc", SetOptions{})
	_, err := c.Incr(ctx, "g:str")
	var re *ResponseError
	if !errors.As(err, &re) || !strings.Contains(re.Message, "not an integer") {
		t.Fatal(err)
	}
	if _, err := c.Do(ctx, "BOGUS"); !errors.As(err, &re) {
		t.Fatal(err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal("connection must stay usable:", err)
	}
}

func TestAuth(t *testing.T) {
	addr := port(t, "MCACHE_TEST_AUTH_PORT")
	pw := os.Getenv("MCACHE_TEST_PASSWORD")
	if err := client(t, Options{Addr: addr, Password: pw}).Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := client(t, Options{Addr: addr, Password: "wrong"}).Ping(ctx); !IsAuth(err) {
		t.Fatal("wrong password must be an AuthError, got", err)
	}
	if err := client(t, Options{Addr: addr}).Ping(ctx); !IsAuth(err) {
		t.Fatal("missing password must be an AuthError, got", err)
	}
}

func TestTimeoutReconnect(t *testing.T) {
	c := client(t, Options{Timeout: 300 * time.Millisecond})
	if _, err := c.Do(ctx, "SLOW", 1); !IsTimeout(err) {
		t.Fatal("want timeout, got", err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal("must work on a fresh connection:", err)
	}
}

func TestDroppedConnection(t *testing.T) {
	c := client(t, Options{})
	_, err := c.Do(ctx, "DROP")
	var ce *ConnectionError
	if !errors.As(err, &ce) || ce.Timeout {
		t.Fatal("want non-timeout ConnectionError, got", err)
	}
	if err := c.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPipeline(t *testing.T) {
	c := client(t, Options{})
	c.Del(ctx, "gp:1", "gp:2", "gp:cnt")
	res, err := c.Pipeline().Set("gp:1", "a", SetOptions{}).Set("gp:2", "b", SetOptions{}).Get("gp:1").Incr("gp:cnt").Exec(ctx)
	if err != nil || len(res) != 4 || string(res[2].([]byte)) != "a" || res[3].(int64) != 1 {
		t.Fatal(res, err)
	}
	one, err := c.Pipeline().Get("gp:1").Exec(ctx)
	if err != nil || len(one) != 1 {
		t.Fatal("single-command pipeline must still return a slice:", one, err)
	}
	mixed, err := c.Pipeline().Set("gp:s", "abc", SetOptions{}).Incr("gp:s").Get("gp:s").Exec(ctx)
	var re *ResponseError
	if !errors.As(err, &re) || len(mixed) != 3 || string(mixed[2].([]byte)) != "abc" {
		t.Fatal(mixed, err)
	}
}

func TestQueues(t *testing.T) {
	c := client(t, Options{})
	if err := c.QPush(ctx, "g:jobs", "a", QPushOptions{Priority: 5, TTLSeconds: 60}); err != nil {
		t.Fatal(err)
	}
	if l := lastCmd(t, c); !reflect.DeepEqual(l, []string{"QPUSH", "g:jobs", "a", "PRIORITY", "5", "TTL", "60"}) {
		t.Fatal(l)
	}
	if v, err := c.QPop(ctx, "g:jobs"); err != nil || string(v) != "a" {
		t.Fatal(string(v), err)
	}
	if _, err := c.QPop(ctx, "g:jobs"); !errors.Is(err, ErrNil) {
		t.Fatal(err)
	}
	if err := c.QPush(ctx, "q", "v", QPushOptions{Priority: 8}); err == nil {
		t.Fatal("priority 8 must be rejected")
	}
}

func TestInfo(t *testing.T) {
	info, err := client(t, Options{}).Info(ctx)
	if err != nil || info["server"]["mcache_version"] != "1.0.0" {
		t.Fatal(info, err)
	}
}
