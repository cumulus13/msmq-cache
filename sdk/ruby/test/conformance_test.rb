# frozen_string_literal: true

# Run through sdk/conformance/run.sh:  ../conformance/run.sh ruby -Ilib test/conformance_test.rb
require "minitest/autorun"
require "msmqcache"

class ConformanceTest < Minitest::Test
  PORT = Integer(ENV.fetch("MCACHE_TEST_PORT") { abort "run through sdk/conformance/run.sh" })
  AUTH_PORT = Integer(ENV.fetch("MCACHE_TEST_AUTH_PORT"))
  PASSWORD = ENV.fetch("MCACHE_TEST_PASSWORD")

  def client(**o) = MsmqCache::Client.new(port: PORT, timeout: 2.0, **o)
  def last(c) = c.execute("LASTCMD")

  def test_ping = assert(client.ping)

  def test_set_get_delete
    c = client
    assert c.set("rb:k", "v")
    assert_equal "v", c.get("rb:k")
    assert_equal 1, c.del("rb:k", "rb:missing")
    assert_nil c.get("rb:k")
  end

  def test_binary_safe
    c = client
    blob = ("\x00\r\n$5\r\n*1\r\n\xFF".b * 100)
    c.set("rb:blob", blob)
    assert_equal blob, c.get("rb:blob")
  end

  def test_nil_vs_empty
    c = client
    c.set("rb:empty", "")
    assert_equal "", c.get("rb:empty")
    assert_nil c.get("rb:never")
  end

  def test_set_options
    c = client
    c.del("rb:opt")
    assert c.set("rb:opt", 1, ex: 30, nx: true)
    assert_equal ["SET", "rb:opt", "1", "EX", "30", "NX"], last(c)
    refute c.set("rb:opt", 2, nx: true)
    refute c.set("rb:nokey", 2, xx: true)
    c.set("rb:opt", "x", px: 1500)
    assert_equal %w[PX 1500], last(c)[3, 2]
    assert_raises(ArgumentError) { c.set("k", "v", nx: true, xx: true) }
    assert_raises(ArgumentError) { c.set("", "v") }
    assert_raises(ArgumentError) { c.set("k", "v", ex: 0) }
  end

  def test_numbers_and_multi
    c = client
    c.del("rb:cnt")
    assert_equal 1, c.incr("rb:cnt")
    assert_equal 10, c.incr_by("rb:cnt", 9)
    c.mset("rbm:1" => "a", "rbm:2" => "b")
    assert_equal ["a", nil, "b"], c.mget("rbm:1", "rbm:none", "rbm:2")
    assert_equal %w[rbm:1 rbm:2], c.keys("rbm:*").sort
  end

  def test_server_error
    c = client
    c.set("rb:str", "abc")
    e = assert_raises(MsmqCache::ResponseError) { c.incr("rb:str") }
    assert_match(/not an integer/, e.message)
    assert_raises(MsmqCache::ResponseError) { c.execute("BOGUS") }
    assert c.ping, "connection must stay usable"
  end

  def test_auth
    assert MsmqCache::Client.new(port: AUTH_PORT, password: PASSWORD).ping
    assert_raises(MsmqCache::AuthenticationError) { MsmqCache::Client.new(port: AUTH_PORT, password: "wrong").ping }
    assert_raises(MsmqCache::AuthenticationError) { MsmqCache::Client.new(port: AUTH_PORT).ping }
  end

  def test_timeout_reconnect
    c = client(timeout: 0.3)
    assert_raises(MsmqCache::TimeoutError) { c.execute("SLOW", 1) }
    assert c.ping, "fresh connection"
  end

  def test_dropped_connection
    c = client
    e = assert_raises(MsmqCache::ConnectionError) { c.execute("DROP") }
    refute_kind_of MsmqCache::TimeoutError, e
    assert c.ping
  end

  def test_pipeline
    c = client
    c.del("rbp:1", "rbp:2", "rbp:cnt")
    r = c.pipeline.set("rbp:1", "a").set("rbp:2", "b").get("rbp:1").incr("rbp:cnt").exec
    assert_equal "a", r[2]
    assert_equal 1, r[3]
    assert_equal 1, c.pipeline.get("rbp:1").exec.length
    m = c.pipeline.set("rbp:s", "abc").cmd("INCR", "rbp:s").get("rbp:s").exec_lenient
    assert_kind_of MsmqCache::ResponseError, m[1]
    assert_equal "abc", m[2]
    assert_raises(MsmqCache::ResponseError) { c.pipeline.cmd("INCR", "rbp:s").exec }
    assert_equal [true, "a"].length, c.pipeline { |p| p.set("rbp:b", "a").get("rbp:b") }.length
  end

  def test_queues
    c = client
    c.qpush("rb:jobs", "a", priority: 5, ttl: 60)
    assert_equal ["QPUSH", "rb:jobs", "a", "PRIORITY", "5", "TTL", "60"], last(c)
    assert_equal "a", c.qpop("rb:jobs")
    assert_nil c.qpop("rb:jobs")
    assert_raises(ArgumentError) { c.qpush("q", "v", priority: 8) }
  end

  def test_info_and_decode
    c = client(decode: true)
    assert_equal "1.0.0", c.info["server"]["mcache_version"]
    c.set("rb:u", "héllo")
    assert_equal "héllo", c.get("rb:u")
  end
end
