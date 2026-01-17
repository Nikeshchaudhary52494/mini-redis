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

	// Read first command
	args, err := protocol.ReadCommand(reader)
	if err != nil {
		conn.Close()
		return
	}

	cmd := strings.ToUpper(args[0])

	// 🔥 REPLICA HANDSHAKE (LEADER SIDE)
	// Replica connects to leader and sends: SYNC
	if cmd == "SYNC" {
		fmt.Println("Replica connected:", conn.RemoteAddr())

		// Register replica connection on leader
		s.Loop.Replicas = append(s.Loop.Replicas, conn)

		// Do NOT close connection
		// Leader will only WRITE to this connection
		select {} // block forever
	}

	// 🔹 NORMAL CLIENT PATH

	// Send first command to event loop
	s.Loop.Commands <- Command{
		Conn: conn,
		Args: args,
	}

	// Keep reading client commands
	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			conn.Close()
			return
		}

		s.Loop.Commands <- Command{
			Conn: conn,
			Args: args,
		}
	}
}
