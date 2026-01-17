package main

import (
    "mini-redis/internal/server"
    "mini-redis/internal/store"
)

func main() {
    st := store.NewStore()
    loop := server.NewEventLoop(st)

    go loop.Start() // 🔥 Redis heart

    tcp := server.NewTCPServer(":6379", loop)
    if err := tcp.Start(); err != nil {
        panic(err)
    }
}
