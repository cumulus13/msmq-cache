package com.cumulus13.msmqcache;

/** No reply in time. The connection was closed; the command may still be executed by the server. */
public class CacheTimeoutException extends CacheConnectionException {
    private static final long serialVersionUID = 1L;
    public CacheTimeoutException(String message, Throwable cause) { super(message, cause); }
}
