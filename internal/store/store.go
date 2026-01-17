package store

import (
	"fmt"
	"io"
	"time"
)

type Store struct {
	data map[string]Value
}

func NewStore() *Store {
	return &Store{
		data: make(map[string]Value),
	}
}

func (s *Store) Set(key, value string, ttl time.Duration) {
	v := Value{
		Type:       StringType,
		Data:       value,
		LastAccess: time.Now().UnixNano(),
	}

	if ttl > 0 {
		v.Expiry = time.Now().Add(ttl)
	}

	s.data[key] = v
}

func (s *Store) Get(key string) (string, bool) {
	v, ok := s.data[key]
	if !ok || isExpired(v) {
		delete(s.data, key)
		return "", false
	}
	v.LastAccess = time.Now().UnixNano()
	return v.Data, true
}

func (s *Store) Del(key string) bool {
	if _, ok := s.data[key]; ok {
		delete(s.data, key)
		return true
	}
	return false
}

func (s *Store) Exists(key string) bool {
	v, ok := s.data[key]
	if !ok || isExpired(v) {
		delete(s.data, key)
		return false
	}
	v.LastAccess = time.Now().UnixNano()
	return true
}

func (s *Store) TTL(key string) time.Duration {
	v, ok := s.data[key]
	if !ok {
		return -2 * time.Second
	}
	if v.Expiry.IsZero() {
		return -1 * time.Second
	}
	ttl := time.Until(v.Expiry)
	if ttl <= 0 {
		delete(s.data, key)
		return -2 * time.Second
	}
	return ttl
}

func (s *Store) Snapshot(w io.Writer) error {
	for key, v := range s.data {
		if isExpired(v) {
			continue
		}
		fmt.Fprintf(w, "SET %s %s\n", key, v.Data)
	}
	return nil
}

func (s *Store) RandomKeysWithTTL(limit int) []string {
	keys := make([]string, 0, limit)

	for k, v := range s.data {
		if !v.Expiry.IsZero() {
			keys = append(keys, k)
			if len(keys) >= limit {
				break
			}
		}
	}
	return keys
}

func (s *Store) DeleteIfExpired(key string) bool {
	v, ok := s.data[key]
	if !ok {
		return false
	}
	if isExpired(v) {
		delete(s.data, key)
		return true
	}
	return false
}

func (s *Store) ApproxSize() int64 {
	var size int64
	for k, v := range s.data {
		size += int64(len(k))
		size += int64(len(v.Data))
		size += 64 // overhead estimate
	}
	return size
}

func (s *Store) SampleKeys(limit int) []string {
	keys := make([]string, 0, limit)
	for k := range s.data {
		keys = append(keys, k)
		if len(keys) >= limit {
			break
		}
	}
	return keys
}

func (s *Store) EvictLRU(sampleSize int) bool {
	keys := s.SampleKeys(sampleSize)
	if len(keys) == 0 {
		return false
	}

	var oldestKey string
	var oldestTime int64 = time.Now().UnixNano()

	for _, k := range keys {
		v := s.data[k]
		if v.LastAccess < oldestTime {
			oldestTime = v.LastAccess
			oldestKey = k
		}
	}

	if oldestKey != "" {
		delete(s.data, oldestKey)
		return true
	}

	return false
}

func (s *Store) SnapshotCommands() [][]string {
	cmds := make([][]string, 0)

	for k, v := range s.data {
		if isExpired(v) {
			continue
		}
		cmds = append(cmds, []string{"SET", k, v.Data})
	}

	return cmds
}
