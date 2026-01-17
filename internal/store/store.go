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
		Type: StringType,
		Data: value,
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
