package protocol

import (
	"bufio"
	"errors"
	"io"
	"strconv"
	"strings"
)

var ErrInvalidRESP = errors.New("invalid RESP format")

func ReadCommand(r *bufio.Reader) ([]string, error) {
	// Peek first byte to check for RESP array
	b, err := r.Peek(1)
	if err != nil {
		return nil, err
	}

	// Inline command (e.g., "PING", "INFO replication")
	if b[0] != '*' {
		line, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			return nil, ErrInvalidRESP
		}
		return strings.Fields(line), nil
	}

	// RESP Array
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
    }

	line = strings.TrimSpace(line)
	// We already checked b[0] == '*', but standard parsing continues:
	count, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, ErrInvalidRESP
	}

	args := make([]string, 0, count)

	for i := 0; i < count; i++ {
		// Read $<length>
		lenLine, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}

		lenLine = strings.TrimSpace(lenLine)
		if len(lenLine) == 0 || lenLine[0] != '$' {
			return nil, ErrInvalidRESP
		}

		strLen, err := strconv.Atoi(lenLine[1:])
		if err != nil {
			return nil, ErrInvalidRESP
		}

		// Read actual string + CRLF
		buf := make([]byte, strLen+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}

		args = append(args, string(buf[:strLen]))
	}

	return args, nil
}

func ReadBulkString(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}

	line = strings.TrimSpace(line)
	if len(line) == 0 || line[0] != '$' {
		return "", ErrInvalidRESP
	}

	length, err := strconv.Atoi(line[1:])
	if err != nil {
		return "", err
	}

	if length == -1 {
		return "", nil
	}

	buf := make([]byte, length+2)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	return string(buf[:length]), nil
}
