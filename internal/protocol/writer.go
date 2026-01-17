package protocol

import (
    "fmt"
    "io"
)

func WriteSimpleString(w io.Writer, s string) {
    fmt.Fprintf(w, "+%s\r\n", s)
}

func WriteError(w io.Writer, s string) {
    fmt.Fprintf(w, "-ERR %s\r\n", s)
}

func WriteBulkString(w io.Writer, s *string) {
    if s == nil {
        fmt.Fprint(w, "$-1\r\n")
        return
    }
    fmt.Fprintf(w, "$%d\r\n%s\r\n", len(*s), *s)
}

func WriteInteger(w io.Writer, v int64) {
    fmt.Fprintf(w, ":%d\r\n", v)
}
