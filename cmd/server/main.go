package main

import (
	"mini-redis/internal/server"
	"mini-redis/internal/store"
)

func main() {
	s := store.NewStore()

	tcpServer := server.NewTCPServer(":6379", s)
	if err := tcpServer.Start(); err != nil {
		panic(err)
	}
}
