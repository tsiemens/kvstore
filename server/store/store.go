package store

import (
	"errors"
	"github.com/tsiemens/kvstore/shared/util"
	"sort"
)

// representation of the consistent hashing store
type Store struct {
	m    map[Key][]byte
	Lock util.Semaphore
}

func New() *Store {
	return &Store{
		m:    make(map[Key][]byte),
		Lock: util.NewSemaphore(),
	}
}

func (s *Store) Get(key Key) ([]byte, error) {
	var val []byte
	s.Lock.Lock()
	defer s.Lock.Unlock()
	v, ok := s.m[key]
	val = v
	if !ok {
		return nil, errors.New("No value for " + key.String())
	}
	return val, nil
}

func (s *Store) Put(key Key, value []byte) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.m[key] = value
	return nil
}

func (s *Store) Remove(key Key) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		return nil
	} else {
		return errors.New("No value for " + key.String())
	}
}

func (s *Store) GetKeys() []Key {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	keys := make([]Key, 0, len(s.m))
	for k := range s.m {
		keys = append(keys, k)
	}
	return keys
}

func (s *Store) GetSortedKeys() []Key {
	keys := s.GetKeys()
	sort.Sort(Keys(keys))
	return keys
}
