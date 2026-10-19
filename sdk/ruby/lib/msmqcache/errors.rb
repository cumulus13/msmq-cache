# frozen_string_literal: true

module MsmqCache
  class Error < StandardError; end
  # TCP failure; the command may or may not have run.
  class ConnectionError < Error; end
  # No reply in time. The connection is closed; the command may still be executed by the server.
  class TimeoutError < ConnectionError; end
  class ProtocolError < Error; end
  # Server replied with -ERR ...
  class ResponseError < Error; end
  # NOAUTH / WRONGPASS
  class AuthenticationError < ResponseError; end
end
