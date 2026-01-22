package main

import (
	"fmt"
	"log"
	"mini-redis/client"
	"time"
)

func main() {
	// These hostnames work inside the docker network
	addrs := []string{"redis-1:6379", "redis-2:6379", "redis-3:6379"}

	fmt.Println("Initializing Smart Client...")
	// Retry connection loop
	var cli *client.Client
	var err error
	for i := 0; i < 10; i++ {
		cli, err = client.NewClient(addrs)
		if err == nil {
			break
		}
		fmt.Printf("Failed to connect: %v. Retrying...\n", err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("Could not connect to cluster: %v", err)
	}
	defer cli.Close()

	fmt.Println("Connected! Starting workload...")

	// 1. Write to Leader
	key := "framework"
	val := "mini-redis-client"
	fmt.Printf("[WRITE] SET %s %s\n", key, val)
	if err := cli.Set(key, val); err != nil {
		log.Fatalf("SET failed: %v", err)
	}

	// Allow replication to catch up
	time.Sleep(500 * time.Millisecond)

	// 2. Read from Replicas (Load Balanced)
	fmt.Println("[READ] Starting 10 reads (should hit different replicas)...")
	for i := 0; i < 10; i++ {
		val, err := cli.Get(key)
		if err != nil {
			fmt.Printf("GET error: %v\n", err)
		} else {
			fmt.Printf("GET %s = %s\n", key, val)
		}
		time.Sleep(200 * time.Millisecond)
	}
	
	fmt.Println("Done.")
}
