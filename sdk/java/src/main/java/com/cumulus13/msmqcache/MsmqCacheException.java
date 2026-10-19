package com.cumulus13.msmqcache;

/** Base class of all errors raised by this client (unchecked). */
public class MsmqCacheException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    public MsmqCacheException(String message) { super(message); }
    public MsmqCacheException(String message, Throwable cause) { super(message, cause); }
}
