package server

import (
	"fmt"
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

type CommandSource int

const (
	SourceClient CommandSource = iota
	SourceAOFReplay
	SourceInternal
)

type Command struct {
	Type   CommandType
	Conn   net.Conn
	Args   []string
	Source CommandSource
}

type EventLoop struct {
	Store        *store.Store
	Commands     chan Command
	AOF          *persistence.AOF
	MaxMemory    int64
	LRUSamples   int
	StartTime    time.Time
	CommandsSeen int64
}

func NewEventLoop(store *store.Store, aof *persistence.AOF, maxMemory int64) *EventLoop {
	return &EventLoop{
		Store:      store,
		Commands:   make(chan Command, 1024),
		AOF:        aof,
		MaxMemory:  maxMemory,
		LRUSamples: 5, // Redis default
		StartTime:  time.Now(),
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
	if cmd.Source == SourceClient {
		l.CommandsSeen++
	}
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

	case "INFO":
		section := "all"
		if len(args) == 2 {
			section = strings.ToLower(args[1])
		}

		info := l.buildInfo(section)
		protocol.WriteBulkString(conn, &info)

	case "CONFIG":
		if len(args) < 3 {
			protocol.WriteError(conn, "wrong number of arguments")
			return
		}

		sub := strings.ToUpper(args[1])

		switch sub {

		case "GET":
			l.handleConfigGet(conn, args[2:])

		case "SET":
			l.handleConfigSet(conn, args[2:])

		default:
			protocol.WriteError(conn, "unknown subcommand")
		}

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

func (l *EventLoop) buildInfo(section string) string {
	var b strings.Builder

	uptime := int64(time.Since(l.StartTime).Seconds())

	if section == "all" || section == "server" {
		b.WriteString("# Server\n")
		b.WriteString("uptime_in_seconds:")
		b.WriteString(strconv.FormatInt(uptime, 10))
		b.WriteString("\n\n")
	}

	if section == "all" || section == "memory" {
		b.WriteString("# Memory\n")
		b.WriteString("used_memory:")
		b.WriteString(strconv.FormatInt(l.Store.ApproxSize(), 10))
		b.WriteString("\n")

		b.WriteString("maxmemory:")
		b.WriteString(strconv.FormatInt(l.MaxMemory, 10))
		b.WriteString("\n\n")
	}

	if section == "all" || section == "stats" {
		b.WriteString("# Stats\n")
		b.WriteString("total_commands_processed:")
		b.WriteString(strconv.FormatInt(l.CommandsSeen, 10))
		b.WriteString("\n\n")
	}

	if section == "all" || section == "persistence" {
		b.WriteString("# Persistence\n")
		if l.AOF != nil {
			b.WriteString("aof_enabled:1\n")
		} else {
			b.WriteString("aof_enabled:0\n")
		}
		b.WriteString("\n")
	}

	return b.String()
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
