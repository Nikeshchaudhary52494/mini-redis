package server

import (
	"bufio"
	"fmt"
	"mini-redis/internal/protocol"
	"net"
	"strings"
)

func (l *EventLoop) startReplication(host, port string) {
	fmt.Println("replicate: connecting to master at", host+":"+port)
	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		fmt.Println("replicate: connection failed:", err)
		return
	}
	fmt.Println("replicate: connected to master")

	// Send SYNC
	fmt.Println("replicate: sending SYNC")
	_ = protocol.WriteArray(conn, []string{"SYNC"})

	reader := bufio.NewReader(conn)

	// Expect FULLRESYNC
	args, err := protocol.ReadCommand(reader)
	if err != nil {
		fmt.Println("replicate: error reading command:", err)
		return
	}
	fmt.Println("replicate: received:", strings.Join(args, " "))
	if strings.ToUpper(args[0]) != "FULLRESYNC" {
		fmt.Println("replicate: expected FULLRESYNC, got:", args)
		return
	}
	fmt.Println("replicate: received FULLRESYNC, starting snapshot sync")

	// Read snapshot
	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			fmt.Println("replicate: error reading snapshot command:", err)
			return
		}

		if strings.ToUpper(args[0]) == "STREAM" {
			fmt.Println("replicate: finished snapshot sync, starting live stream")
			break
		}

		fmt.Println("replicate: applying snapshot command:", strings.Join(args, " "))
		l.applyReplicaCommand(args)
	}

	// Live stream
	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			fmt.Println("replicate: error reading live command:", err)
			return
		}

		fmt.Println("replicate: applying live command:", strings.Join(args, " "))
		l.applyReplicaCommand(args)
	}
}
