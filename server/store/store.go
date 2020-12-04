package store

import (
	"encoding/json"
	"sync"
)

type Store struct {
	mu sync.RWMutex
	kv map[string]string
}

func New() *Store {
	return new(Store)
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if val, found := s.kv[key]; found {
		return val, true
	}
	return "", false
}

func (s *Store) Set(key string, val string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.kv[key] = val
}

func (s *Store) Del(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.kv, key)
}

func (s *Store) ToJSON() []byte {
	s.mu.RLock()
	defer s.mu.RUnlock()

	data, err := json.Marshal(s.kv)
	if err != nil {
		panic(err)
	}
	return data
}

func (s *Store) LoadJSON(data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := json.Unmarshal(data, &s.kv)
	if err != nil {
		panic(err)
	}
}
