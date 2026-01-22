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
	if epoch > l.CurrentEpoch {
		l.CurrentEpoch = epoch
	}

	switch strings.ToUpper(args[2]) {
	case "SET":
		l.Store.Set(args[3], args[4], 0)
	case "DEL":
		l.Store.Del(args[3])
	}
}

func (l *EventLoop) promoteToLeader() {
	if l.Role == RoleLeader {
		return
	}

	fmt.Println("[failover] promoting to leader, term:", l.CurrentEpoch)

	l.Role = RoleLeader
	l.MasterHost = ""
	l.MasterPort = ""
	l.MasterUp = false

	// If we were replicating, stop it
	select {
	case l.StopReplication <- struct{}{}:
	default:
	}

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

	fmt.Println("[failover] scheduling auto-promotion check")

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

	// Start Election
	l.Commands <- Command{
		Type: StartElectionCommand,
	}
}

// checkPeerLeader returns true if peer is a leader
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

func (l *EventLoop) startElection() {
	l.Role = RoleCandidate
	l.CurrentEpoch++
	l.VotedFor = l.NodeID
	l.VotesReceived = 1 // Vote for self
	l.ElectionStartTime = time.Now()
	l.ElectionDuration = time.Duration(1500+rand.Intn(1500)) * time.Millisecond

	fmt.Printf("[election] starting election for term %d. Peers: %d. Timeout: %v\n", l.CurrentEpoch, len(l.Peers), l.ElectionDuration)

	// Determine majority
	needed := (len(l.Peers)+1)/2 + 1

	// Check if we already won (single node cluster)
	if l.VotesReceived >= needed {
		l.promoteToLeader()
		return
	}

	for _, peer := range l.Peers {
		go l.requestVoteFromPeer(peer, l.CurrentEpoch, l.NodeID)
	}
}

func (l *EventLoop) requestVoteFromPeer(peer string, term int64, candidateID string) {
	conn, err := net.DialTimeout("tcp", peer, 500*time.Millisecond)
	if err != nil {
		if !strings.Contains(err.Error(), "connection refused") {
			fmt.Printf("[election] failed to dial peer %s: %v\n", peer, err)
		}
		return
	}
	defer conn.Close()

	protocol.WriteArray(conn, []string{"REQUEST_VOTE", strconv.FormatInt(term, 10), candidateID})

	reader := bufio.NewReader(conn)
	resp, err := protocol.ReadCommand(reader) // Expect ["VOTE", "YES"|"NO", "term"]
	if err != nil {
		fmt.Printf("[election] failed to read vote from %s: %v\n", peer, err)
		return
	}

	if len(resp) < 3 {
		return
	}

	voteGranted := (resp[1] == "YES")
	respTerm, _ := strconv.ParseInt(resp[2], 10, 64)

	l.Commands <- Command{
		Type:        VoteResultCommand,
		Term:        respTerm,
		VoteGranted: voteGranted,
	}
}

func (l *EventLoop) broadcastHeartbeat() {
	for _, peer := range l.Peers {
		go func(peer string) {
			conn, err := net.DialTimeout("tcp", peer, 500*time.Millisecond)
			if err != nil {
				return
			}
			defer conn.Close()

			protocol.WriteArray(conn, []string{
				"HEARTBEAT",
				strconv.FormatInt(l.CurrentEpoch, 10),
				l.NodeID,
			})
		}(peer)
	}
}

func (l *EventLoop) handleVoteResult(cmd Command) {
	// If the term in response is higher, we step down
	if cmd.Term > l.CurrentEpoch {
		l.CurrentEpoch = cmd.Term
		l.Role = RoleReplica
		l.VotedFor = ""
		l.VotesReceived = 0
		return
	}

	// Ignore if not candidate or old term
	if l.Role != RoleCandidate || cmd.Term != l.CurrentEpoch {
		return
	}

	if cmd.VoteGranted {
		l.VotesReceived++
		needed := (len(l.Peers)+1)/2 + 1
		fmt.Printf("[election] vote received. Total: %d/%d\n", l.VotesReceived, needed)

		if l.VotesReceived >= needed {
			l.promoteToLeader()
		}
	}
}