package server

import (
	"mini-redis/internal/persistence"
	"mini-redis/internal/store"
	"net"
	"time"
)

type CommandType int

const (
	ClientCommand CommandType = iota
	InternalExpireCommand
	ReplicaRegister
)

type ServerRole int

const (
	RoleLeader ServerRole = iota
	RoleReplica
)

type Command struct {
	Type    CommandType
	Conn    net.Conn
	Args    []string
	Replica *Replica
}

type Replica struct {
	Conn net.Conn
	Ch   chan []string
}

type EventLoop struct {
	Store           *store.Store
	Commands        chan Command
	AOF             *persistence.AOF
	MaxMemory       int64
	LRUSamples      int
	StartTime       time.Time
	CommandsSeen    int64
	Role            ServerRole
	Replicas        []*Replica
	MasterHost      string
	MasterPort      string
	MasterUp        bool
	StopReplication chan struct{}
	CurrentEpoch    int64 // my epoch if leader
	MasterEpoch     int64 // leader epoch I follow (if replica)
}
