package server

import (
	"bufio"
	"fmt"
	"mini-redis/internal/protocol"
	"net"
	"strings"
)

type TCPServer struct {
	Addr string
	Loop *EventLoop
}

func NewTCPServer(addr string, loop *EventLoop) *TCPServer {
	return &TCPServer{
		Addr: addr,
		Loop: loop,
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
	reader := bufio.NewReader(conn)

	args, err := protocol.ReadCommand(reader)
	if err != nil {
		conn.Close()
		return
	}

	cmd := strings.ToUpper(args[0])

	// ===== Replica handshake =====
	if cmd == "SYNC" {
		fmt.Println("Replica connected:", conn.RemoteAddr())

		// ❌ TCP handler kuch bhi write nahi karega

		replica := &Replica{
			Conn: conn,
			Ch:   make(chan []string, 1024),
		}

		// 🔥 Replica ko event loop ko handover karo
		s.Loop.Commands <- Command{
			Type:    ReplicaRegister,
			Replica: replica,
		}

		// TCP goroutine yahin khatam
		return
	}

	// ===== Normal client =====
	s.Loop.Commands <- Command{Conn: conn, Args: args}

	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			conn.Close()
			return
		}

		s.Loop.Commands <- Command{Conn: conn, Args: args}
	}
}
