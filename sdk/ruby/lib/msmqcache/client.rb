# frozen_string_literal: true

require "monitor"
require "socket"
require "io/wait"

module MsmqCache
  # Blocking client for mcache-server (MSMQ-backed cache / queue server). Thread-safe (calls are serialised).
  # A command that fails with a connection error or timeout is never retried (the server is at-most-once);
  # the connection is closed and re-opened by the next call.
  #
  # Reply mapping for #execute: Symbol-free: simple strings => Status, integers => Integer, bulk => binary String,
  # nil => nil, arrays => Array. Server errors raise ResponseError.
  class Client
    MAX_KEY = 1024
    MAX_LINE = 64 * 1024

    # A simple-string reply such as +OK (bulk replies are plain Strings).
    Status = Struct.new(:text) { def to_s = text }

    attr_reader :host, :port

    def initialize(host: "127.0.0.1", port: 6390, password: nil, timeout: 10.0, connect_timeout: 5.0,
                   max_value_bytes: 4_000_000, decode: false)
      @host, @port, @password = host, port, password
      @timeout, @connect_timeout = timeout, connect_timeout
      @max_value_bytes, @decode = max_value_bytes, decode
      @sock = nil
      @buf = String.new(encoding: Encoding::BINARY)
      @lock = Monitor.new
    end

    def close
      @lock.synchronize do
        @sock&.close
      rescue IOError, SystemCallError
        nil
      ensure
        @sock = nil
        @buf = String.new(encoding: Encoding::BINARY)
      end
    end

    # ---- generic ----------------------------------------------------------
    # Run any command: client.execute("INFO"). Server error replies raise ResponseError.
    def execute(*args)
      one(args.map { |a| str(a) })
    end

    def ping = one(%w[PING]).then { |r| r.is_a?(Status) && r.text == "PONG" }

    # true = stored, false = NX/XX prevented it. ex = seconds, px = milliseconds.
    def set(key, value, ex: nil, px: nil, nx: false, xx: false, keepttl: false)
      !one(set_args(key, value, ex: ex, px: px, nx: nx, xx: xx, keepttl: keepttl)).nil?
    end

    # nil for a missing key; an empty value is "".
    def get(key) = out(one(["GET", key!(key)]))

    def mget(*keys)
      raise ArgumentError, "keys must not be empty" if keys.empty?

      out(one(["MGET", *keys.map { |k| key!(k) }]))
    end

    # Not atomic on the server (one MSMQ message per key).
    def mset(entries)
      raise ArgumentError, "entries must not be empty" if entries.empty?

      one(["MSET", *entries.flat_map { |k, v| [key!(k), val!(v)] }])
      true
    end

    def del(*keys) = one(["DEL", *keys.map { |k| key!(k) }])
    def exists(*keys) = one(["EXISTS", *keys.map { |k| key!(k) }])

    # Re-sends the value on the server (MSMQ messages are immutable).
    def expire(key, seconds)
      raise ArgumentError, "seconds must be positive" unless seconds.is_a?(Integer) && seconds.positive?

      one(["EXPIRE", key!(key), seconds.to_s]) == 1
    end

    # Seconds left; -1 no expiry; -2 no such key.
    def ttl(key) = one(["TTL", key!(key)])
    def incr(key) = one(["INCR", key!(key)])
    def decr(key) = one(["DECR", key!(key)])
    def incr_by(key, n) = one(["INCRBY", key!(key), Integer(n).to_s])
    # O(n) on the server.
    def keys(pattern = "*") = out(one(["KEYS", pattern]))
    def dbsize = one(%w[DBSIZE])
    def flushdb = one(%w[FLUSHDB]) && true

    # Native MSMQ queue (auto-created). priority 0..7 (7 highest), ttl seconds.
    def qpush(queue, value, priority: nil, ttl: nil)
      one(qpush_args(queue, value, priority: priority, ttl: ttl))
      true
    end

    # Non-blocking: nil when empty.
    def qpop(queue) = out(one(["QPOP", queue]))
    def qpeek(queue) = out(one(["QPEEK", queue]))
    def qlen(queue) = one(["QLEN", queue])
    def qlist = out(one(%w[QLIST]))

    def info
      result = {}
      section = "default"
      one(%w[INFO]).to_s.each_line do |line|
        l = line.strip
        next if l.empty?

        if l.start_with?("#")
          section = l.sub(/\A#+\s*/, "").downcase
        else
          k, v = l.split(":", 2)
          (result[section] ||= {})[k] = v
        end
      end
      result
    end

    # Queues commands and sends them in one write. The server still runs them one after another.
    def pipeline
      pipe = Pipeline.new(self)
      return pipe unless block_given?

      yield pipe
      pipe.exec
    end

    class Pipeline
      def initialize(client)
        @c = client
        @cmds = []
      end

      def cmd(*args) = tap { @cmds << args.map { |a| @c.send(:str, a) } }
      def set(key, value, **o) = tap { @cmds << @c.send(:set_args, key, value, **o) }
      def get(key) = tap { @cmds << ["GET", @c.send(:key!, key)] }
      def incr(key) = tap { @cmds << ["INCR", @c.send(:key!, key)] }
      def length = @cmds.length

      # Replies in order; server errors appear as ResponseError objects in the array.
      def exec_lenient
        cmds, @cmds = @cmds, []
        cmds.empty? ? [] : @c.send(:exchange, cmds)
      end

      # Like exec_lenient but raises the first server error.
      def exec
        replies = exec_lenient
        err = replies.find { |r| r.is_a?(ResponseError) }
        raise err if err

        replies
      end
    end

    private

    # ---- argument helpers -------------------------------------------------
    def str(v)
      case v
      when String then v
      when Integer then v.to_s
      when Symbol then v.to_s
      else raise ArgumentError, "unsupported argument type #{v.class} (use String or Integer)"
      end
    end

    def key!(k)
      s = str(k)
      raise ArgumentError, "key length must be 1..#{MAX_KEY} bytes" unless (1..MAX_KEY).cover?(s.bytesize)

      s
    end

    def val!(v)
      s = str(v)
      raise ArgumentError, "value is #{s.bytesize} bytes; limit is #{@max_value_bytes}" if s.bytesize > @max_value_bytes

      s
    end

    def pos!(n, name)
      raise ArgumentError, "#{name} must be a positive Integer" unless n.is_a?(Integer) && n.positive?

      n.to_s
    end

    def set_args(key, value, ex: nil, px: nil, nx: false, xx: false, keepttl: false)
      raise ArgumentError, "nx and xx are mutually exclusive" if nx && xx
      raise ArgumentError, "use either ex or px" if ex && px

      a = ["SET", key!(key), val!(value)]
      a.push("EX", pos!(ex, "ex")) if ex
      a.push("PX", pos!(px, "px")) if px
      a << "NX" if nx
      a << "XX" if xx
      a << "KEEPTTL" if keepttl
      a
    end

    def qpush_args(queue, value, priority: nil, ttl: nil)
      a = ["QPUSH", str(queue), val!(value)]
      if priority
        raise ArgumentError, "priority must be 0..7" unless priority.is_a?(Integer) && (0..7).cover?(priority)

        a.push("PRIORITY", priority.to_s)
      end
      a.push("TTL", pos!(ttl, "ttl")) if ttl
      a
    end

    def out(v)
      return v unless @decode

      case v
      when String then v.dup.force_encoding(Encoding::UTF_8)
      when Array then v.map { |x| out(x) }
      else v
      end
    end

    # ---- wire ---------------------------------------------------------------
    def one(cmd)
      r = exchange([cmd]).first
      raise r if r.is_a?(ResponseError)

      r
    end

    def exchange(cmds)
      @lock.synchronize do
        ensure_connected
        roundtrip(cmds)
      end
    end

    def ensure_connected
      return if @sock

      begin
        @sock = Socket.tcp(@host, @port, connect_timeout: @connect_timeout)
      rescue Errno::ETIMEDOUT => e
        raise TimeoutError, "connect to #{@host}:#{@port} timed out: #{e.message}"
      rescue SocketError, SystemCallError, IOError => e
        raise ConnectionError, "cannot connect to #{@host}:#{@port}: #{e.message}"
      end
      @sock.setsockopt(Socket::IPPROTO_TCP, Socket::TCP_NODELAY, 1)
      return unless @password && !@password.empty?

      begin
        r = roundtrip([["AUTH", @password]]).first
      rescue StandardError
        close
        raise
      end
      return unless r.is_a?(ResponseError)

      close
      raise r
    end

    def roundtrip(cmds)
      payload = String.new(encoding: Encoding::BINARY)
      cmds.each do |args|
        payload << "*#{args.length}\r\n"
        args.each { |a| payload << "$#{a.bytesize}\r\n" << a.b << "\r\n" }
      end
      @sock.write(payload)
      Array.new(cmds.length) { read_reply }
    rescue ProtocolError
      close
      raise
    rescue TimeoutError
      close
      raise
    rescue EOFError, IOError, SystemCallError => e
      close
      raise ConnectionError, "connection lost: #{e.message}"
    end

    def fill
      raise TimeoutError, "no reply within #{@timeout}s; connection closed, the command may still be executed" unless @sock.wait_readable(@timeout)

      chunk = @sock.read_nonblock(65_536, exception: false)
      raise EOFError, "connection closed by server" if chunk.nil?
      return if chunk == :wait_readable

      @buf << chunk
    end

    def read_line
      loop do
        i = @buf.index("\r\n")
        if i
          line = @buf.slice!(0, i + 2)
          return line[0, i]
        end
        raise ProtocolError, "line too long" if @buf.bytesize > MAX_LINE

        fill
      end
    end

    def read_exact(n)
      fill while @buf.bytesize < n
      @buf.slice!(0, n)
    end

    def int!(s, what)
      Integer(s, 10)
    rescue ArgumentError
      raise ProtocolError, "invalid #{what}: #{s[0, 40].inspect}"
    end

    def read_reply
      line = read_line
      raise ProtocolError, "empty reply line" if line.empty?

      rest = line[1..]
      case line[0]
      when "+" then Status.new(rest)
      when "-" then error_for(rest)
      when ":" then int!(rest, "integer")
      when "$"
        n = int!(rest, "bulk length")
        return nil if n.negative?

        data = read_exact(n + 2)
        data[0, n]
      when "*"
        n = int!(rest, "array length")
        return nil if n.negative?

        Array.new(n) { read_reply }
      else raise ProtocolError, "unknown reply type #{line[0].inspect}"
      end
    end

    def error_for(msg)
      msg.start_with?("NOAUTH", "WRONGPASS") ? AuthenticationError.new(msg) : ResponseError.new(msg)
    end
  end
end
