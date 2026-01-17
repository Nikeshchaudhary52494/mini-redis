package main

import (
	"fmt"
	"mini-redis/internal/persistence"
	"mini-redis/internal/server"
	"mini-redis/internal/store"
	"os"
	"strings"
)

func main() {
	fmt.Println("mini-redis PID:", os.Getpid())
	st := store.NewStore()

	aof, err := persistence.NewAOF(
		"appendonly.aof",
		persistence.FsyncEverySec,
	)
	if err != nil {
		panic(err)
	}

	// Replay persisted commands
	_ = aof.Replay(func(cmd []string) {
		// Apply without writing again
		switch strings.ToUpper(cmd[0]) {
		case "SET":
			st.Set(cmd[1], cmd[2], 0)
		case "DEL":
			st.Del(cmd[1])
		}
	})
	const maxMemory = 1024 * 1024 // 1MB
	loop := server.NewEventLoop(st, aof, maxMemory)
	go loop.Start()
	server.StartExpiryTicker(loop)
	tcp := server.NewTCPServer(":6379", loop)
	if err := tcp.Start(); err != nil {
		panic(err)
	}
}
