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

type CommandType int

const (
	ClientCommand CommandType = iota
	InternalExpireCommand
)

type Command struct {
	Type CommandType
	Conn net.Conn
	Args []string
}

type EventLoop struct {
	Store      *store.Store
	Commands   chan Command
	AOF        *persistence.AOF
	MaxMemory  int64
	LRUSamples int
}

func NewEventLoop(store *store.Store, aof *persistence.AOF, maxMemory int64) *EventLoop {
	return &EventLoop{
		Store:      store,
		Commands:   make(chan Command, 1024),
		AOF:        aof,
		MaxMemory:  maxMemory,
		LRUSamples: 5, // Redis default
	}
}

func (l *EventLoop) Start() {
	for cmd := range l.Commands {
		switch cmd.Type {
		case ClientCommand:
			l.execute(cmd)
		case InternalExpireCommand:
			l.runActiveExpiry()
		}
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
		l.enforceMaxMemory()

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

func (l *EventLoop) runActiveExpiry() {
	const sampleSize = 20
	const maxDeletes = 25

	keys := l.Store.RandomKeysWithTTL(sampleSize)
	deleted := 0

	for _, key := range keys {
		if l.Store.DeleteIfExpired(key) {
			deleted++
			if deleted >= maxDeletes {
				break
			}
		}
	}
}

func StartExpiryTicker(loop *EventLoop) {
	ticker := time.NewTicker(100 * time.Millisecond)

	go func() {
		for range ticker.C {
			loop.Commands <- Command{
				Type: InternalExpireCommand,
			}
		}
	}()
}

func (l *EventLoop) enforceMaxMemory() {
	for l.Store.ApproxSize() > l.MaxMemory {
		evicted := l.Store.EvictLRU(l.LRUSamples)
		if !evicted {
			break
		}
	}
}
