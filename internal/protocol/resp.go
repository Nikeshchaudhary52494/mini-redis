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
    line, err := r.ReadString('\n')
    if err != nil {
        return nil, err
    }

    line = strings.TrimSpace(line)
    if len(line) == 0 {
        return nil, ErrInvalidRESP
    }

    // Must start with *
    if line[0] != '*' {
        return nil, ErrInvalidRESP
    }

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
        if lenLine[0] != '$' {
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
