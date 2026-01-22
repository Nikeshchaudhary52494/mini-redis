package server

import (
	"bufio"
	"fmt"
	"math/rand"
	"mini-redis/internal/protocol"
	"net"
	"strconv"
	"strings"
	"time"
)

func (l *EventLoop) startReplication(host, port string) {
	conn, err := net.Dial("tcp", host+":"+port)
	if err != nil {
		fmt.Println("[replica] failed to connect to master")
		l.MasterUp = false
		go l.scheduleAutoPromote()
		return
	}

	l.MasterUp = true

	protocol.WriteArray(conn, []string{"SYNC"})
	reader := bufio.NewReader(conn)

	// FULLRESYNC
	args, err := protocol.ReadCommand(reader)
	if err != nil || strings.ToUpper(args[0]) != "FULLRESYNC" {
		l.MasterUp = false
		go l.scheduleAutoPromote()
		return
	}

	// Snapshot
	for {
		args, err := protocol.ReadCommand(reader)
		if err != nil {
			l.handleMasterDown()
			return
		}
		if strings.ToUpper(args[0]) == "STREAM" {
			break
		}
		l.applyReplicaCommand(args)
	}

	// Streaming
	for {
		select {
		case <-l.StopReplication:
			return
		default:
			args, err := protocol.ReadCommand(reader)
			if err != nil {
				l.handleMasterDown()
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
		cmd := append([]string{
			"REPL",
			strconv.FormatInt(l.CurrentEpoch, 10),
		}, args...)

		select {
		case r.Ch <- cmd:
		default:
			fmt.Println("replica lagging")
		}
	}
}

func (l *EventLoop) applyReplicaCommand(args []string) {
	if args[0] != "REPL" {
		return
	}

	epoch, _ := strconv.ParseInt(args[1], 10, 64)

	// Reject stale leader
	if epoch < l.MasterEpoch {
		fmt.Println("[replica] stale epoch ignored:", epoch)
		return
	}

	// Accept new leader epoch
	l.MasterEpoch = epoch

	switch strings.ToUpper(args[2]) {
	case "SET":
		l.Store.Set(args[3], args[4], 0)
	case "DEL":
		l.Store.Del(args[3])
	}
}

func (l *EventLoop) promoteToLeader() {
	if l.Role != RoleReplica {
		return
	}

	fmt.Println("[failover] auto-promoting replica to leader")

	l.CurrentEpoch = max(l.CurrentEpoch, l.MasterEpoch) + 1
	l.MasterEpoch = 0
	l.Role = RoleLeader
	l.MasterHost = ""
	l.MasterPort = ""
	l.MasterUp = false

	close(l.StopReplication)

	fmt.Println("[failover] promotion complete")
}

func (l *EventLoop) handleMasterDown() {
	if !l.MasterUp {
		return // already handled
	}

	fmt.Println("[replica] master link down")

	l.MasterUp = false

	go l.scheduleAutoPromote()
}

func (l *EventLoop) scheduleAutoPromote() {
	// only replicas can auto-promote
	if l.Role != RoleReplica {
		return
	}

	fmt.Println("[failover] scheduling auto-promotion")

	// Randomized sleep to avoid split vote/promotion
	randSleep := time.Duration(rand.Intn(1000)) * time.Millisecond
	time.Sleep(3*time.Second + randSleep)

	// Check again (maybe master came back)
	if l.MasterUp {
		fmt.Println("[failover] master recovered, aborting promotion")
		return
	}

	// Check peers if any of them is already leader
	for _, peer := range l.Peers {
		if l.checkPeerLeader(peer) {
			fmt.Println("[failover] found new leader, following", peer)
			parts := strings.Split(peer, ":")
			if len(parts) != 2 {
				continue
			}
			// Switch to replica of new leader
			l.MasterHost = parts[0]
			l.MasterPort = parts[1]
			l.MasterUp = false
			go l.startReplication(l.MasterHost, l.MasterPort)
			return
		}
	}

	l.promoteToLeader()
}

func (l *EventLoop) checkPeerLeader(peer string) bool {
	conn, err := net.DialTimeout("tcp", peer, 500*time.Millisecond)
	if err != nil {
		return false
	}
	defer conn.Close()

	// Send INFO replication
	protocol.WriteArray(conn, []string{"INFO", "replication"})

	reader := bufio.NewReader(conn)
	val, err := protocol.ReadBulkString(reader)
	if err != nil {
		return false
	}

	return strings.Contains(val, "role:leader")
}