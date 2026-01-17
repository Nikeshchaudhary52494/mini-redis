package server

import (
	"fmt"
	"mini-redis/internal/protocol"
	"net"
	"strconv"
	"strings"
)

func (l *EventLoop) handleConfig(conn net.Conn, args []string) {
	if len(args) < 3 {
		protocol.WriteError(conn, "wrong number of arguments")
		return
	}

	switch strings.ToUpper(args[1]) {
	case "GET":
		l.handleConfigGet(conn, args[2:])
	case "SET":
		l.handleConfigSet(conn, args[2:])
	}
}

func (l *EventLoop) handleConfigGet(conn net.Conn, args []string) {
	key := strings.ToLower(args[0])

	var result string

	switch key {
	case "maxmemory":
		result = strconv.FormatInt(l.MaxMemory, 10)
	case "appendfsync":
		result = l.AOF.PolicyString()
	default:
		protocol.WriteBulkString(conn, nil)
		return
	}

	// Redis returns array [key, value]
	resp := fmt.Sprintf("*2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(key), key,
		len(result), result,
	)

	conn.Write([]byte(resp))
}

func (l *EventLoop) handleConfigSet(conn net.Conn, args []string) {
	if len(args) != 2 {
		protocol.WriteError(conn, "wrong number of arguments")
		return
	}

	key := strings.ToLower(args[0])
	val := args[1]

	switch key {
	case "maxmemory":
		v, err := strconv.ParseInt(val, 10, 64)
		if err != nil || v < 0 {
			protocol.WriteError(conn, "invalid maxmemory value")
			return
		}
		l.MaxMemory = v
		protocol.WriteSimpleString(conn, "OK")

	default:
		protocol.WriteError(conn, "unsupported config parameter")
	}
}
