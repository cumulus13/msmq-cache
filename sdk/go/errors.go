// File: sdk\go\errors.go
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-19
// Description: 
// License: MIT

package msmqcache

import (
	"errors"
	"fmt"
)

// ErrNil is returned by Get/QPop/QPeek when the server replied nil (missing key / empty queue).
var ErrNil = errors.New("msmqcache: nil reply")

// ResponseError is a server "-ERR ..." reply.
type ResponseError struct{ Message string }

func (e *ResponseError) Error() string { return "msmqcache: server error: " + e.Message }

// AuthError is NOAUTH / WRONGPASS.
type AuthError struct{ ResponseError }

// ConnectionError means the TCP connection failed or was lost; the command may or may not have run.
type ConnectionError struct {
	Err     error
	Timeout bool // no reply in time: the connection was closed, the command may still be executed by the server
}

func (e *ConnectionError) Error() string {
	if e.Timeout {
		return fmt.Sprintf("msmqcache: timeout, connection closed, command may still be executed: %v", e.Err)
	}
	return fmt.Sprintf("msmqcache: connection error: %v", e.Err)
}
func (e *ConnectionError) Unwrap() error { return e.Err }

// ProtocolError means the server sent invalid RESP.
type ProtocolError struct{ Message string }

func (e *ProtocolError) Error() string { return "msmqcache: protocol error: " + e.Message }

// IsTimeout reports whether err is a reply timeout.
func IsTimeout(err error) bool {
	var ce *ConnectionError
	return errors.As(err, &ce) && ce.Timeout
}

// IsAuth reports whether err is an authentication failure.
func IsAuth(err error) bool {
	var ae *AuthError
	return errors.As(err, &ae)
}

func newResponseError(msg string) error {
	if len(msg) >= 6 && (msg[:6] == "NOAUTH" || (len(msg) >= 9 && msg[:9] == "WRONGPASS")) {
		return &AuthError{ResponseError{msg}}
	}
	return &ResponseError{msg}
}
