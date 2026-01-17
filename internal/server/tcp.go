package server

import (
	"bufio"
	"fmt"
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

	reader := bufio.NewScanner(conn)
	writer := bufio.NewWriter(conn)

	writer.WriteString("+OK Mini Redis ready\r\n")
	writer.Flush()

	for reader.Scan() {
		line := strings.TrimSpace(reader.Text())
		if line == "" {
			continue
		}

		response := s.executeCommand(line)
		writer.WriteString(response)
		writer.Flush()
	}
}

func (s *TCPServer) executeCommand(input string) string {
	parts := strings.Split(input, " ")
	cmd := strings.ToUpper(parts[0])

	switch cmd {

	case "SET":
		if len(parts) < 3 {
			return "-ERR wrong number of arguments\r\n"
		}

		ttl := time.Duration(0)
		if len(parts) == 4 {
			sec, err := strconv.Atoi(parts[3])
			if err == nil {
				ttl = time.Duration(sec) * time.Second
			}
		}

		s.Store.Set(parts[1], parts[2], ttl)
		return "+OK\r\n"

	case "GET":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments\r\n"
		}

		val, ok := s.Store.Get(parts[1])
		if !ok {
			return "$-1\r\n"
		}
		return fmt.Sprintf("$%d\r\n%s\r\n", len(val), val)

	case "DEL":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments\r\n"
		}

		if s.Store.Del(parts[1]) {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "EXISTS":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments\r\n"
		}

		if s.Store.Exists(parts[1]) {
			return ":1\r\n"
		}
		return ":0\r\n"

	case "TTL":
		if len(parts) != 2 {
			return "-ERR wrong number of arguments\r\n"
		}

		ttl := s.Store.TTL(parts[1])
		return fmt.Sprintf(":%d\r\n", int(ttl.Seconds()))

	case "QUIT":
		return "+OK\r\n"

	default:
		return "-ERR unknown command\r\n"
	}
}
