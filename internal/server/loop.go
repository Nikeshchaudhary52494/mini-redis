package server

import (
	"io"
	"mini-redis/internal/persistence"
	"mini-redis/internal/protocol"
	"mini-redis/internal/store"
	"net"
	"strconv"
	"strings"
	"time"
)

type Command struct {
	Conn net.Conn
	Args []string
}

type EventLoop struct {
	Store    *store.Store
	Commands chan Command
	AOF      *persistence.AOF
}

func NewEventLoop(store *store.Store, aof *persistence.AOF) *EventLoop {
	return &EventLoop{
		Store:    store,
		Commands: make(chan Command, 1024),
		AOF:      aof,
	}
}

func (l *EventLoop) Start() {
	for cmd := range l.Commands {
		l.execute(cmd)
	}
}

func (l *EventLoop) execute(cmd Command) {
	args := cmd.Args
	conn := cmd.Conn

	if len(args) == 0 {
		protocol.WriteError(conn, "empty command")
		return
	}

	switch strings.ToUpper(args[0]) {

	case "SET":
		if len(args) < 3 {
			protocol.WriteError(conn, "wrong number of arguments")
			return
		}

		ttl := time.Duration(0)
		if len(args) == 5 && strings.ToUpper(args[3]) == "EX" {
			sec, err := strconv.Atoi(args[4])
			if err == nil {
				ttl = time.Duration(sec) * time.Second
			}
		}

		l.Store.Set(args[1], args[2], ttl)

		if l.AOF != nil {
			_ = l.AOF.Append(args)
		}

		protocol.WriteSimpleString(conn, "OK")

	case "GET":
		val, ok := l.Store.Get(args[1])
		if !ok {
			protocol.WriteBulkString(conn, nil)
			return
		}
		protocol.WriteBulkString(conn, &val)

	case "DEL":
		deleted := l.Store.Del(args[1])

		if deleted && l.AOF != nil {
			_ = l.AOF.Append(args)
		}

		if deleted {
			protocol.WriteInteger(conn, 1)
		} else {
			protocol.WriteInteger(conn, 0)
		}

	case "EXISTS":
		if l.Store.Exists(args[1]) {
			protocol.WriteInteger(conn, 1)
		} else {
			protocol.WriteInteger(conn, 0)
		}

	case "TTL":
		ttl := l.Store.TTL(args[1])
		protocol.WriteInteger(conn, int64(ttl.Seconds()))

	case "PING":
		protocol.WriteSimpleString(conn, "PONG")

	case "BGREWRITEAOF":
		if l.AOF == nil {
			protocol.WriteError(conn, "AOF is not enabled")
			return
		}

		// Snapshot is captured INSIDE event loop (safe)
		snapshot := func(w io.Writer) error {
			return l.Store.Snapshot(w)
		}

		// Rewrite runs in background
		go func() {
			_ = l.AOF.Rewrite(snapshot)
		}()

		protocol.WriteSimpleString(conn, "OK")

	default:
		protocol.WriteError(conn, "unknown command")
	}
}
