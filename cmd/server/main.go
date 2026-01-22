package main

import (
	"flag"
	"fmt"
	"mini-redis/internal/persistence"
	"mini-redis/internal/server"
	"mini-redis/internal/store"
	"os"
	"strings"
)

func main() {
	// 🔥 Read port from CLI
	port := flag.Int("port", 6379, "port to run server on")
	peers := flag.String("peers", "", "comma-separated list of peer addresses")
	flag.Parse()

	addr := fmt.Sprintf(":%d", *port)

	fmt.Println("mini-redis starting on", addr)
	fmt.Println("PID:", os.Getpid())

	st := store.NewStore()

	// 🔥 Separate AOF per instance
	aof, err := persistence.NewAOF(
		fmt.Sprintf("appendonly-%d.aof", *port),
		persistence.FsyncEverySec,
	)
	if err != nil {
		panic(err)
	}

	// Replay persisted commands
	_ = aof.Replay(func(cmd []string) {
		switch strings.ToUpper(cmd[0]) {
		case "SET":
			st.Set(cmd[1], cmd[2], 0)
		case "DEL":
			st.Del(cmd[1])
		}
	})

	const maxMemory = 1024 * 1024 // 1MB
	loop := server.NewEventLoop(st, aof, maxMemory)
	loop.NodeID = addr

	if *peers != "" {
		loop.Peers = strings.Split(*peers, ",")
		loop.Role = server.RoleReplica
	}

	go loop.Start()
	server.StartExpiryTicker(loop)

	tcp := server.NewTCPServer(addr, loop)
	if err := tcp.Start(); err != nil {
		panic(err)
	}
}
