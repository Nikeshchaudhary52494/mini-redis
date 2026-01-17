package server

import (
	"bufio"
	"fmt"
	"mini-redis/internal/protocol"
	"net"
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
	defer conn.Close()

	reader := bufio.NewReader(conn)

	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			return
		}

		s.Loop.Commands <- Command{
			Conn: conn,
			Args: args,
		}
	}
}
