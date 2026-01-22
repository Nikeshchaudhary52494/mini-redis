# 🧠 Mini-Redis (Distributed In-Memory Store)

A high-performance, distributed **key-value store** written in Go, compatible with the Redis RESP protocol. This project implements core distributed systems concepts including **Leader-Follower Replication**, **Automatic Failover (Raft-lite)**, **AOF Persistence**, and a **Smart Client** for topology-aware routing.

It is designed to be a learning resource for understanding how distributed databases work under the hood.

---

## ✨ Key Features

### 🚀 Core Engine

- **RESP Protocol Compatible**: Works with standard `redis-cli` and Redis libraries.
- **In-Memory Storage**: Fast key-value operations.
- **TTL Support**: Keys expire automatically after a set time.
- **LRU Eviction**: Automatically removes old keys when memory limit is reached.

### 🛡️ Distributed Architecture

- **Replication**: One Leader (Writes) + Multiple Replicas (Reads).
- **Automatic Failover**: If the Leader crashes, the cluster detects it and elects a new Leader automatically using a quorum-based election (similar to Raft).
- **Split-Brain Protection**: Uses Epoch (Term) numbers to reject stale leaders.

### 💾 Persistence

- **AOF (Append-Only File)**: Durability ensures data isn't lost on restart.
- **Background Rewrite**: `BGREWRITEAOF` compacts logs without blocking the main thread.

### 🧠 Smart Client

- **Topology Discovery**: Automatically finds the Leader and Replicas.
- **Read/Write Splitting**: Routes `SET` commands to the Leader and `GET` commands to Replicas (Round-Robin).
- **Auto-Retry**: Seamlessly handles failovers with automatic retries, providing near-zero downtime to the application.

---

## 🏗️ Architecture

The system runs as a cluster of nodes. An external load balancer (HAProxy) is provided for legacy clients, while modern applications use the Smart Client.

```
                               ┌─────────────────┐
                               │  Application    │
                               │ (Smart Client)  │
                               └────────┬────────┘
                   ┌────────────────────┼────────────────────┐
                   │                    │                    │
          ┌────────▼────────┐  ┌────────▼────────┐  ┌────────▼────────┐
          │  Redis Node 1   │  │  Redis Node 2   │  │  Redis Node 3   │
          │    (Leader)     │◄─┤    (Replica)    │◄─┤    (Replica)    │
          └─────────────────┘  └─────────────────┘  └─────────────────┘
                   ▲
                   │
           ┌───────┴───────┐
           │    HAProxy    │◄─── Standard redis-cli
           └───────────────┘
```

---

## 🚀 Getting Started

The easiest way to run the cluster is using **Docker Compose**. This spins up 3 Redis nodes, an HAProxy load balancer, and an example client application.

### Prerequisites

- Docker & Docker Compose
- `redis-cli` (optional, for manual testing)

### 1. Start the Cluster

```bash
docker-compose up --build
```

You will see logs from 3 redis nodes, haproxy, and the example client.

### 2. Connect Manually (via CLI)

You can connect to the cluster using the standard Redis CLI through the HAProxy load balancer on port **6379**.

```bash
redis-cli -p 6379
```

Try running commands:

```bash
SET mykey "Hello Distributed World"
GET mykey
INFO replication
```

### 3. Smart Client Demo

The `client-app` service in Docker demonstrates the Go Smart Client. It connects to the cluster, performs writes to the leader, and reads from replicas. Check the docker logs:

```
client-app_1  | Initializing Smart Client...
client-app_1  | Connected! Starting workload...
client-app_1  | [WRITE] SET framework mini-redis-client
client-app_1  | [READ] GET framework = mini-redis-client
```

---

## 🎮 Testing Failover

You can simulate a crash to see the system recover automatically.

1.  **Check who is the leader**:

    ```bash
    redis-cli -p 6379 INFO replication
    # Output: role:leader (and check the container logs to see which node this is, e.g., redis-1)
    ```

2.  **Kill the Leader**:
    Stop the container corresponding to the leader (e.g., `redis-1`).

    ```bash
    docker-compose stop redis-1
    ```

3.  **Watch the Election**:
    Observe the logs of the other nodes (`redis-2`, `redis-3`).
    - They will detect the master is down.
    - Start an election.
    - One will become the new Leader.

4.  **Verify Client Recovery**:
    - **HAProxy**: Will automatically switch traffic to the new leader after a brief check interval.
    - **Smart Client**: Will catch the connection error, refresh its topology map, and retry the operation against the new leader automatically.

---

## 📜 Supported Commands

| Command  | Usage                        | Description                                   |
| :------- | :--------------------------- | :-------------------------------------------- |
| `SET`    | `SET key value [EX seconds]` | Set a key with optional TTL.                  |
| `GET`    | `GET key`                    | Get the value of a key.                       |
| `DEL`    | `DEL key`                    | Delete a key.                                 |
| `TTL`    | `TTL key`                    | Get remaining time to live (in seconds).      |
| `EXISTS` | `EXISTS key`                 | Check if a key exists (1) or not (0).         |
| `PING`   | `PING`                       | Returns PONG.                                 |
| `INFO`   | `INFO [section]`             | Get server info (replication, memory, stats). |
| `CONFIG` | `CONFIG GET/SET param`       | Get or set configuration (e.g., maxmemory).   |

---

## 💻 Development (Running Locally)

If you want to run without Docker (e.g., for development), you can start nodes manually.

**1. Build:**

```bash
go build -o mini-redis cmd/server/main.go
```

**2. Start Leader (Port 6379):**

```bash
./mini-redis -port 6379 -peers "localhost:6380,localhost:6381"
```

**3. Start Replicas:**

```bash
./mini-redis -port 6380 -peers "localhost:6381"
./mini-redis -port 6381 -peers "localhost:6380"
```

_(Note: Replicas will auto-discover the leader via the peers list or need `REPLICAOF` command manually if not using the discovery logic)._

---

---

Summary:

- redis-cli -> HAProxy -> Leader (Writes & Reads)
- client-app (Smart Client) -> Leader (Writes) / Replicas (Reads)

---

## 📁 Project Structure

```
mini-redis/
├── cmd/
│   ├── server/          # Main entry point for the Server
│   └── example-client/  # Demo application using the Smart Client
├── internal/
│   ├── server/          # Core logic: Event loop, Replication, Election, TCP
│   ├── store/           # In-memory data structures (Map, TTL, LRU)
│   ├── persistence/     # AOF file handling
│   └── protocol/        # RESP parser & writer
├── client/              # 📦 Go Smart Client Library
├── docker-compose.yml   # Cluster orchestration
├── Dockerfile           # Server container definition
├── Dockerfile.client    # Client container definition
└── haproxy.cfg          # Load Balancer configuration
```
