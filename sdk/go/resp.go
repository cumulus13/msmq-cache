// File: sdk\go\resp.go
// Author: Hadi Cahyadi <cumulus13@gmail.com>
// Date: 2026-10-19
// Description: 
// License: MIT

package msmqcache

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
)

const maxLine = 64 * 1024

func appendCommand(dst []byte, args [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(args)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range args {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

func readLine(r *bufio.Reader) ([]byte, error) {
	var line []byte
	for {
		part, isPrefix, err := r.ReadLine()
		if err != nil {
			if err == io.EOF && len(line) > 0 {
				return nil, io.ErrUnexpectedEOF
			}
			return nil, err
		}
		line = append(line, part...)
		if len(line) > maxLine {
			return nil, &ProtocolError{"line too long"}
		}
		if !isPrefix {
			return line, nil
		}
	}
}

// readReply returns: string (simple), int64, []byte (bulk), nil, []any, or error values (*ResponseError) inside arrays.
func readReply(r *bufio.Reader) (any, error) {
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, &ProtocolError{"empty reply line"}
	}
	t, rest := line[0], string(line[1:])
	switch t {
	case '+':
		return rest, nil
	case '-':
		return newResponseError(rest), nil
	case ':':
		n, err := strconv.ParseInt(rest, 10, 64)
		if err != nil {
			return nil, &ProtocolError{fmt.Sprintf("invalid integer %q", rest)}
		}
		return n, nil
	case '$':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return nil, &ProtocolError{fmt.Sprintf("invalid bulk length %q", rest)}
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return buf[:n], nil
	case '*':
		n, err := strconv.Atoi(rest)
		if err != nil {
			return nil, &ProtocolError{fmt.Sprintf("invalid array length %q", rest)}
		}
		if n < 0 {
			return nil, nil
		}
		items := make([]any, 0, n)
		for i := 0; i < n; i++ {
			it, err := readReply(r)
			if err != nil {
				return nil, err
			}
			items = append(items, it)
		}
		return items, nil
	}
	return nil, &ProtocolError{fmt.Sprintf("unknown reply type %q", t)}
}

func toBytes(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return x, nil
	case string:
		return []byte(x), nil
	case int:
		return strconv.AppendInt(nil, int64(x), 10), nil
	case int32:
		return strconv.AppendInt(nil, int64(x), 10), nil
	case int64:
		return strconv.AppendInt(nil, x, 10), nil
	case uint:
		return strconv.AppendUint(nil, uint64(x), 10), nil
	case uint32:
		return strconv.AppendUint(nil, uint64(x), 10), nil
	case uint64:
		return strconv.AppendUint(nil, x, 10), nil
	case float64:
		return strconv.AppendFloat(nil, x, 'g', -1, 64), nil
	}
	return nil, fmt.Errorf("msmqcache: unsupported argument type %T", v)
}
