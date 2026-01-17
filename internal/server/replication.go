package server

import (
	"bufio"
	"fmt"
	"mini-redis/internal/protocol"
	"net"
)

func (l *EventLoop) startReplication(host, port string) {
	addr := host + ":" + port

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("replication connect failed:", err)
		return
	}

	// 🔥 send handshake to leader
	protocol.WriteArray(conn, []string{"SYNC"})

	reader := bufio.NewReader(conn)

	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			return
		}

		// Apply replicated write
		l.applyReplicaCommand(args)
	}
}
