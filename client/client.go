package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrNoLeader is returned when no leader can be found in the cluster
var ErrNoLeader = errors.New("no leader found")

// ErrNoReplica is returned when no replica is available
var ErrNoReplica = errors.New("no replica available")

type Client struct {
	addrs []string // Initial seed addresses

	mu           sync.RWMutex
	leaderAddr   string
	replicaAddrs []string
	conns        map[string]net.Conn // Cache connections
}

func NewClient(addrs []string) (*Client, error) {
	c := &Client{
		addrs: addrs,
		conns: make(map[string]net.Conn),
	}
	if err := c.refreshTopology(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, conn := range c.conns {
		conn.Close()
	}
}

// Set executes a SET command on the Leader with auto-retry
func (c *Client) Set(key, value string) error {
	// Try up to 2 times
	for attempt := 0; attempt < 2; attempt++ {
		c.mu.RLock()
		leader := c.leaderAddr
		c.mu.RUnlock()

		if leader == "" {
			if err := c.refreshTopology(); err != nil {
				return err // No leader found at all
			}
			c.mu.RLock()
			leader = c.leaderAddr
			c.mu.RUnlock()
		}

		conn, err := c.getConn(leader)
		if err == nil {
			cmd := fmt.Sprintf("*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(key), key, len(value), value)
			_, writeErr := conn.Write([]byte(cmd))
			if writeErr == nil {
				resp, readErr := readResp(conn)
				if readErr == nil {
					if strings.HasPrefix(resp, "-") {
						return errors.New(strings.TrimPrefix(resp, "-"))
					}
					return nil // Success!
				}
				err = readErr
			} else {
				err = writeErr
			}
		}

		// If we are here, something failed (connection, write, or read)
		// Close the bad connection
		c.closeConn(leader)
		
		// If this was the last attempt, return the error
		if attempt == 1 {
			return fmt.Errorf("operation failed after retry: %v", err)
		}

		// Otherwise, refresh topology and loop again
		fmt.Printf("Connection to leader %s failed (%v), refreshing topology...\n", leader, err)
		if refreshErr := c.refreshTopology(); refreshErr != nil {
			return fmt.Errorf("failed to refresh topology: %v (original error: %v)", refreshErr, err)
		}
	}
	return nil
}

// Get executes a GET command on a Replica with auto-retry
func (c *Client) Get(key string) (string, error) {
	var err error
	for attempt := 0; attempt < 2; attempt++ {
		replica, pickErr := c.pickReplica()
		if pickErr != nil {
			// No replicas? Try refreshing topology
			c.refreshTopology()
			replica, pickErr = c.pickReplica()
			if pickErr != nil {
				return "", pickErr
			}
		}

		conn, connErr := c.getConn(replica)
		if connErr == nil {
			cmd := fmt.Sprintf("*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n", len(key), key)
			_, writeErr := conn.Write([]byte(cmd))
			if writeErr == nil {
				resp, readErr := readResp(conn)
				if readErr == nil {
					if strings.HasPrefix(resp, "-") {
						return "", errors.New(strings.TrimPrefix(resp, "-"))
					}
					if resp == "$-1" {
						return "", nil
					}
					return parseBulkString(resp), nil
				}
				err = readErr
			} else {
				err = writeErr
			}
		} else {
			err = connErr
		}

		// Failure handling
		c.closeConn(replica)
		
		if attempt == 1 {
			return "", fmt.Errorf("read failed after retry: %v", err)
		}
		
		// Refresh topology to find new/healthy replicas
		// We could just try another replica from the existing list, 
		// but refreshing ensures we don't keep hitting a dead one if the list is stale.
		c.refreshTopology()
	}
	return "", err
}

func (c *Client) pickReplica() (string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if len(c.replicaAddrs) == 0 {
		return "", ErrNoReplica
	}

	// Simple Random Load Balancing
	idx := rand.Intn(len(c.replicaAddrs))
	return c.replicaAddrs[idx], nil
}

func (c *Client) refreshTopology() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var newLeader string
	var newReplicas []string

	// Check all known addresses
	// In a real system, we might discover new nodes.
	// Here we rely on the initial seed list + any we might have added (TODO: add discovery logic if needed)
	// For this task, checking seeds is sufficient as we know the cluster size.

	for _, addr := range c.addrs {
		role, err := c.checkRole(addr)
		if err != nil {
			fmt.Printf("Failed to check %s: %v\n", addr, err)
			continue
		}

		if role == "leader" {
			newLeader = addr
		} else if role == "replica" {
			newReplicas = append(newReplicas, addr)
		}
	}

	c.leaderAddr = newLeader
	c.replicaAddrs = newReplicas

	if newLeader == "" {
		return ErrNoLeader
	}
	return nil
}

func (c *Client) checkRole(addr string) (string, error) {
	conn, err := net.DialTimeout("tcp", addr, 1*time.Second)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	if _, err := conn.Write([]byte("INFO replication\r\n")); err != nil {
		return "", err
	}

	resp, err := readResp(conn)
	if err != nil {
		return "", err
	}

	// Resp is likely a bulk string "$len\r\nContent..." or just the content if readResp was smarter.
	// My readResp returns raw protocol string like "$20\r\nrole:leader..."
	// Use parseBulkString to get the actual content
	info := parseBulkString(resp)

	if strings.Contains(info, "role:leader") {
		return "leader", nil
	}
	if strings.Contains(info, "role:replica") {
		return "replica", nil
	}
	return "", errors.New("unknown role")
}

func (c *Client) getConn(addr string) (net.Conn, error) {
	if conn, ok := c.conns[addr]; ok {
		return conn, nil
	}

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	c.conns[addr] = conn
	return conn, nil
}

func (c *Client) closeConn(addr string) {
	if conn, ok := c.conns[addr]; ok {
		conn.Close()
		delete(c.conns, addr)
	}
}

// Helpers

func readResp(conn net.Conn) (string, error) {
	reader := bufio.NewReader(conn)
	line, err := reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimSpace(line)

	// Simple String
	if strings.HasPrefix(line, "+") {
		return line[1:], nil
	}
	// Error
	if strings.HasPrefix(line, "-") {
		return line, nil // keep - to indicate error
	}
	// Bulk String ($len)
	if strings.HasPrefix(line, "$") {
		length, err := strconv.Atoi(line[1:])
		if err != nil {
			return "", err
		}
		if length == -1 {
			return "$-1", nil
		}

		// Read exactly length bytes + 2 bytes for CRLF
		// We use io.ReadFull to ensure we get everything
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(reader, buf); err != nil {
			return "", err
		}

		// Return strictly formatted RESP: $len\r\ncontent
		// We ignore the trailing \r\n from buf when appending to return value to match expectation if needed,
		// but standard RESP is "$len\r\ncontent\r\n".
		// parseBulkString expects "$len\r\ncontent".
		// Let's return "$len\r\n" + string(buf[:length])
		return fmt.Sprintf("$%d\r\n%s", length, string(buf[:length])), nil
	}

	return line, nil
}

func parseBulkString(raw string) string {
	// Raw: "$3\r\nbar" or "$35\r\nline1\nline2"
	// Find first \r\n
	idx := strings.Index(raw, "\r\n")
	if idx == -1 {
		return ""
	}
	// Return everything after the first \r\n
	return raw[idx+2:]
}
