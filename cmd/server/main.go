package main

import (
    "bufio"
    "fmt"
    "mini-redis/internal/store"
    "os"
    "strconv"
    "strings"
    "time"
)

func main() {
    s := store.NewStore()
    reader := bufio.NewScanner(os.Stdin)

    fmt.Println("Mini Redis started 🚀")
    fmt.Println("Commands: SET key value [ttl], GET key, DEL key, EXISTS key, TTL key")

    for {
        fmt.Print("> ")
        if !reader.Scan() {
            break
        }

        input := strings.TrimSpace(reader.Text())
        parts := strings.Split(input, " ")

        if len(parts) == 0 {
            continue
        }

        switch strings.ToUpper(parts[0]) {
        case "SET":
            if len(parts) < 3 {
                fmt.Println("ERR wrong number of arguments")
                continue
            }
            ttl := time.Duration(0)
            if len(parts) == 4 {
                t, err := strconv.Atoi(parts[3])
                if err == nil {
                    ttl = time.Duration(t) * time.Second
                }
            }
            s.Set(parts[1], parts[2], ttl)
            fmt.Println("OK")

        case "GET":
            if len(parts) != 2 {
                fmt.Println("ERR wrong number of arguments")
                continue
            }
            val, ok := s.Get(parts[1])
            if !ok {
                fmt.Println("(nil)")
            } else {
                fmt.Println(val)
            }

        case "DEL":
            if len(parts) != 2 {
                fmt.Println("ERR wrong number of arguments")
                continue
            }
            if s.Del(parts[1]) {
                fmt.Println("(1)")
            } else {
                fmt.Println("(0)")
            }

        case "EXISTS":
            if len(parts) != 2 {
                fmt.Println("ERR wrong number of arguments")
                continue
            }
            if s.Exists(parts[1]) {
                fmt.Println("(1)")
            } else {
                fmt.Println("(0)")
            }

        case "TTL":
            if len(parts) != 2 {
                fmt.Println("ERR wrong number of arguments")
                continue
            }
            ttl := s.TTL(parts[1])
            fmt.Println(int(ttl.Seconds()))

        case "EXIT":
            fmt.Println("Bye 👋")
            return

        default:
            fmt.Println("ERR unknown command")
        }
    }
}
