package com.cumulus13.msmqcache;

/** TCP failure; the command may or may not have run. */
public class CacheConnectionException extends MsmqCacheException {
    private static final long serialVersionUID = 1L;
    public CacheConnectionException(String message, Throwable cause) { super(message, cause); }
}
