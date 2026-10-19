package com.cumulus13.msmqcache;

/** NOAUTH / WRONGPASS. */
public class AuthenticationException extends ResponseException {
    private static final long serialVersionUID = 1L;
    public AuthenticationException(String message) { super(message); }
}
