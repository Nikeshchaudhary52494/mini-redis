package server

import (
	"mini-redis/internal/persistence"
	"mini-redis/internal/store"
	"time"
)

func NewEventLoop(store *store.Store, aof *persistence.AOF, maxMemory int64) *EventLoop {
	return &EventLoop{
		Store:           store,
		Commands:        make(chan Command, 1024),
		AOF:             aof,
		MaxMemory:       maxMemory,
		LRUSamples:      5,
		StartTime:       time.Now(),
		Role:            RoleLeader,
		StopReplication: make(chan struct{}),
		CurrentEpoch:    1,
		MasterEpoch:     0,
	}
}

func (l *EventLoop) Start() {
	for cmd := range l.Commands {
		switch cmd.Type {
		case ClientCommand:
			l.execute(cmd)
		case InternalExpireCommand:
			l.runActiveExpiry()
		case ReplicaRegister:
			l.handleReplica(cmd.Replica)
		}
	}
}
