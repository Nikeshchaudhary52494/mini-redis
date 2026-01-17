package server

import (
	"bufio"
	"fmt"
	"mini-redis/internal/protocol"
	"mini-redis/internal/store"
	"net"
	"strconv"
	"strings"
	"time"
)

type TCPServer struct {
	Addr  string
	Store *store.Store
}

func NewTCPServer(addr string, store *store.Store) *TCPServer {
	return &TCPServer{
		Addr:  addr,
		Store: store,
	}
}

func (s *TCPServer) Start() error {
	ln, err := net.Listen("tcp", s.Addr)
	if err != nil {
		return err
	}

	fmt.Println("Mini Redis listening on", s.Addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go s.handleClient(conn)
	}
}

func (s *TCPServer) handleClient(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			return
		}

		s.executeRESP(conn, args)
	}
}

func (s *TCPServer) executeRESP(conn net.Conn, args []string) {
	if len(args) == 0 {
		protocol.WriteError(conn, "empty command")
		return
	}

	cmd := strings.ToUpper(args[0])

	switch cmd {

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

		s.Store.Set(args[1], args[2], ttl)
		protocol.WriteSimpleString(conn, "OK")

	case "GET":
		if len(args) != 2 {
			protocol.WriteError(conn, "wrong number of arguments")
			return
		}

		val, ok := s.Store.Get(args[1])
		if !ok {
			protocol.WriteBulkString(conn, nil)
			return
		}

		protocol.WriteBulkString(conn, &val)

	case "DEL":
		if len(args) != 2 {
			protocol.WriteError(conn, "wrong number of arguments")
			return
		}

		if s.Store.Del(args[1]) {
			protocol.WriteInteger(conn, 1)
		} else {
			protocol.WriteInteger(conn, 0)
		}

	case "EXISTS":
		if len(args) != 2 {
			protocol.WriteError(conn, "wrong number of arguments")
			return
		}

		if s.Store.Exists(args[1]) {
			protocol.WriteInteger(conn, 1)
		} else {
			protocol.WriteInteger(conn, 0)
		}

	case "TTL":
		if len(args) != 2 {
			protocol.WriteError(conn, "wrong number of arguments")
			return
		}

		ttl := s.Store.TTL(args[1])
		protocol.WriteInteger(conn, int64(ttl.Seconds()))

	case "PING":
		protocol.WriteSimpleString(conn, "PONG")

	default:
		protocol.WriteError(conn, "unknown command")
	}
}
