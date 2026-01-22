package server

import (
	"fmt"
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
	if l.Role == RoleReplica && l.MasterHost == "" {
		go l.scheduleAutoPromote()
	}

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case cmd := <-l.Commands:
			switch cmd.Type {
			case ClientCommand:
				l.execute(cmd)
			case InternalExpireCommand:
				l.runActiveExpiry()
			case ReplicaRegister:
				l.handleReplica(cmd.Replica)
			case VoteResultCommand:
				l.handleVoteResult(cmd)
			case StartElectionCommand:
				l.startElection()
			}
		case <-ticker.C:
			l.tick()
		}
	}
}

func (l *EventLoop) tick() {
	if l.Role == RoleCandidate {
		if time.Since(l.ElectionStartTime) > l.ElectionDuration {
			fmt.Println("[election] timeout, restarting election")
			l.startElection()
		}
	}

	if l.Role == RoleLeader {
		l.broadcastHeartbeat()
	}
}
