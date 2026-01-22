package server

import (
	"mini-redis/internal/persistence"
	"mini-redis/internal/store"
	"net"
	"time"
)

// CommandType represents the internal event type processed by the event loop.
type CommandType int

const (
	ClientCommand CommandType = iota
	InternalExpireCommand
	ReplicaRegister
	VoteResultCommand
	StartElectionCommand
)

// ServerRole defines the node's state in the Raft-like consensus algorithm.
type ServerRole int

const (
	// RoleLeader handles all writes and replicates to followers.
	RoleLeader ServerRole = iota
	// RoleReplica follows a leader and serves read-only traffic.
	RoleReplica
	// RoleCandidate is a temporary state during leader election.
	RoleCandidate
)

// Command is a unified structure for all events (network, timer, internal)
// passed into the single-threaded event loop.
type Command struct {
	Type        CommandType
	Conn        net.Conn
	Args        []string
	Replica     *Replica
	Term        int64
	CandidateID string
	VoteGranted bool
}

// Replica represents a connection to a follower node.
type Replica struct {
	Conn net.Conn
	Ch   chan []string // Channel to buffer commands for asynchronous replication
}

// EventLoop is the core single-threaded engine that manages state,
// executes commands, and handles distributed consensus.
type EventLoop struct {
	Store           *store.Store
	Commands        chan Command
	AOF             *persistence.AOF
	MaxMemory       int64
	LRUSamples      int
	StartTime       time.Time
	CommandsSeen    int64
	
	// Distributed System State
	Role            ServerRole
	Replicas        []*Replica
	MasterHost      string
	MasterPort      string
	MasterUp        bool
	StopReplication chan struct{}
	CurrentEpoch    int64 // Current term (Raft term)
	MasterEpoch     int64 // The term of the leader we are following
	NodeID          string
	Peers           []string // List of other nodes in the cluster
	
	// Election State
	VotedFor        string
	VotesReceived   int
	ElectionStartTime time.Time
	ElectionDuration  time.Duration
}
