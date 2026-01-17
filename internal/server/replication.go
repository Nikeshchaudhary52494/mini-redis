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
	l.MasterUp = true
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
		select {
		case <-l.StopReplication:
			fmt.Println("[replica] replication stopped")
			return
		default:
			args, err := protocol.ReadCommand(reader)
			if err != nil {
				return
			}
			l.applyReplicaCommand(args)
		}
	}

}

func (l *EventLoop) handleReplica(r *Replica) {
	fmt.Println("[leader] FULLRESYNC")

	protocol.WriteArray(r.Conn, []string{"FULLRESYNC"})

	for _, cmd := range l.Store.SnapshotCommands() {
		protocol.WriteArray(r.Conn, cmd)
	}

	protocol.WriteArray(r.Conn, []string{"STREAM"})

	l.Replicas = append(l.Replicas, r)

	go func() {
		for args := range r.Ch {
			protocol.WriteArray(r.Conn, args)
		}
	}()
}

func (l *EventLoop) propagateToReplicas(args []string) {
	for _, r := range l.Replicas {
		select {
		case r.Ch <- args:
		default:
			fmt.Println("replica lagging")
		}
	}
}

func (l *EventLoop) applyReplicaCommand(args []string) {
	switch strings.ToUpper(args[0]) {
	case "SET":
		l.Store.Set(args[1], args[2], 0)
	case "DEL":
		l.Store.Del(args[1])
	}
}

func (l *EventLoop) promoteToLeader() {
	fmt.Println("[failover] promoting replica to leader")

	// Change role
	l.Role = RoleLeader

	// Clear master info
	l.MasterHost = ""
	l.MasterPort = ""
	l.MasterUp = false

	// Stop replication stream
	close(l.StopReplication)
	// (simplest: rely on connection close)
	// More advanced: use context / channel close

	fmt.Println("[failover] promotion complete")
}
