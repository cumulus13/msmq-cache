package com.cumulus13.msmqcache;

/** Server replied with -ERR ... */
public class ResponseException extends MsmqCacheException {
    private static final long serialVersionUID = 1L;
    public ResponseException(String message) { super(message); }
}
