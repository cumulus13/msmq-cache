export class MsmqCacheError extends Error {
  constructor(message: string) { super(message); this.name = new.target.name; }
}
/** TCP connection failed or was lost; the command may or may not have run. */
export class CacheConnectionError extends MsmqCacheError {}
/** No reply in time. The connection is closed; the command may still be executed by the server. */
export class CacheTimeoutError extends CacheConnectionError {}
export class ProtocolError extends MsmqCacheError {}
/** Server replied with -ERR ... */
export class ResponseError extends MsmqCacheError {}
/** NOAUTH / WRONGPASS */
export class AuthenticationError extends ResponseError {}
