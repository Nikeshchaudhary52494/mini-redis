package store

import (
    "sync"
    "time"
)

type Store struct {
    mu   sync.RWMutex
    data map[string]Value
}

func NewStore() *Store {
    return &Store{
        data: make(map[string]Value),
    }
}

func (s *Store) Set(key, value string, ttl time.Duration) {
    s.mu.Lock()
    defer s.mu.Unlock()

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
    s.mu.RLock()
    v, ok := s.data[key]
    s.mu.RUnlock()

    if !ok {
        return "", false
    }

    if isExpired(v) {
        s.mu.Lock()
        delete(s.data, key)
        s.mu.Unlock()
        return "", false
    }

    return v.Data, true
}

func (s *Store) Del(key string) bool {
    s.mu.Lock()
    defer s.mu.Unlock()

    if _, ok := s.data[key]; ok {
        delete(s.data, key)
        return true
    }
    return false
}

func (s *Store) Exists(key string) bool {
    s.mu.RLock()
    v, ok := s.data[key]
    s.mu.RUnlock()

    if !ok {
        return false
    }

    if isExpired(v) {
        s.mu.Lock()
        delete(s.data, key)
        s.mu.Unlock()
        return false
    }

    return true
}

func (s *Store) TTL(key string) time.Duration {
    s.mu.RLock()
    v, ok := s.data[key]
    s.mu.RUnlock()

    if !ok {
        return -2 * time.Second // Redis-style: key not found
    }

    if v.Expiry.IsZero() {
        return -1 * time.Second // no expiry
    }

    ttl := time.Until(v.Expiry)
    if ttl <= 0 {
        s.mu.Lock()
        delete(s.data, key)
        s.mu.Unlock()
        return -2 * time.Second
    }

    return ttl
}
