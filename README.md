Awesome 👍
# 🧠 Mini-Redis (Redis-like In-Memory Data Store in Go)

A Redis-inspired **in-memory key–value store** written in Go, built from scratch to understand **systems internals, replication, failover, and distributed coordination**.

This project intentionally focuses on **core Redis mechanics** rather than feature completeness.

---

## ✨ Features

### Core

* RESP protocol compatible (`redis-cli` works)
* In-memory key–value store
* Single-threaded event loop (Redis-style)
* TTL support with active expiry
* LRU eviction with `maxmemory`

### Persistence

* Append-Only File (AOF)
* Configurable fsync policy
* AOF replay on startup
* Background AOF rewrite (`BGREWRITEAOF`)

### Replication & Availability

* Leader–replica replication
* FULLRESYNC + streaming replication
* Read-only replicas
* Automatic failover
* Epoch-based fencing (split-brain prevention)
* Quorum-based leader election
* Old leader reattachment as replica

### Observability

* `INFO server`
* `INFO memory`
* `INFO persistence`
* `INFO replication`
* `CONFIG GET / SET`

---

## 🏗️ Architecture Overview

```
           Clients (redis-cli / app)
                    |
                    v
           ┌───────────────────┐
           │     Leader Node   │
           │  (writes enabled) │
           └───────┬───────────┘
                   │
        ┌──────────┴──────────┐
        │                     │
┌───────────────┐   ┌───────────────┐
│   Replica A   │   │   Replica B   │
│ (read-only)   │   │ (read-only)   │
└───────────────┘   └───────────────┘
```

* Replicas automatically promote on leader failure
* Elections are **replica-only**
* Highest epoch always wins
* Static membership (configured at startup)

---

## 🚀 Getting Started

### Prerequisites

* Go 1.20+
* `redis-cli` installed

---

## 🧩 Running the System (1 Leader + 2 Replicas)

### Leader

```bash
go run main.go \
  --port=6379 \
  --node-id=node-6379 \
  --peers=127.0.0.1:6380,127.0.0.1:6381
```

### Replica A

```bash
go run main.go \
  --port=6380 \
  --node-id=node-6380 \
  --peers=127.0.0.1:6381
```

### Replica B

```bash
go run main.go \
  --port=6381 \
  --node-id=node-6381 \
  --peers=127.0.0.1:6380
```

> **Peers = same replication group replicas (leader excluded)**

---

## 🔌 Connecting with redis-cli

```bash
redis-cli -p 6379
```

Example:

```bash
SET name nikesh
GET name
TTL name
INFO replication
```

---

## 🔁 Failover Demo

1. Kill the leader (`Ctrl+C`)
2. Replicas detect leader failure
3. Quorum election starts
4. One replica promotes to leader
5. Writes continue on new leader

```bash
INFO replication
```

Example output:

```
# Replication
role:leader
connected_replicas:1
```

---

## 📦 Persistence

### Append-Only File (AOF)

* Every write command is appended
* Replay on startup restores state
* Separate AOF per node

### Rewrite

```bash
BGREWRITEAOF
```

---

## 🧠 Configuration

### Max Memory

```bash
CONFIG SET maxmemory 1048576
```

### Read Config

```bash
CONFIG GET maxmemory
```

---

## 📊 INFO Sections

```bash
INFO
INFO server
INFO memory
INFO persistence
INFO replication
```

---

## ⚠️ Design Decisions & Trade-offs

### Why static membership?

* Simpler
* Safer elections
* Matches Redis Sentinel philosophy
* Dynamic membership adds major complexity

### Why no clustering?

* Single leader + replicas cover most real-world use cases
* Avoids unnecessary complexity
* Sharding is optional, not required

---

## ❌ What This Is NOT

* ❌ Redis Cluster
* ❌ Strongly consistent database
* ❌ Multi-region datastore
* ❌ Production Redis replacement

This project is about **learning and demonstrating systems design**, not competing with Redis.

---

## 🧠 What This Project Demonstrates

* Event-loop based server design
* Persistence internals
* Replication protocols
* Failover mechanics
* Epoch-based fencing
* Quorum consensus basics
* Trade-offs in distributed systems

---

## 📁 Project Structure

```
mini-redis/
├── cmd/
│   └── server/
│       └── main.go
├── internal/
│   ├── server/       # TCP server, event loop, replication, election
│   ├── store/        # In-memory store, TTL, LRU
│   ├── persistence/  # AOF, rewrite, fsync
│   └── protocol/     # RESP parser & writer
└── README.md
```

---

## 🧪 Use Cases

* Cache layer
* Session store
* Rate limiting
* Feature flags
* Learning distributed systems