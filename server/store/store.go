package store

import (
	"errors"
	"github.com/tsiemens/kvstore/shared/util"
	"sort"
)

type StoreVal struct {
	Val       []byte
	Active    bool
	Timestamp int // a logical timestamp
}

// representation of the consistent hashing store
type Store struct {
	m    map[Key]*StoreVal
	Lock util.Semaphore
}

func New() *Store {
	return &Store{
		m:    make(map[Key]*StoreVal),
		Lock: util.NewSemaphore(),
	}
}

func (s *Store) Get(key Key) (*StoreVal, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	v, ok := s.m[key]
	if !ok {
		return nil, errors.New("No value for " + key.String())
	}
	return v, nil
}

func (s *Store) Put(key Key, value []byte, timestamp int) error {
	v := &StoreVal{Val: value, Active: true, Timestamp: timestamp}
	s.PutDirect(key, v)
	return nil
}

func (s *Store) PutDirect(key Key, value *StoreVal) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	s.m[key] = value
}

func (s *Store) Remove(key Key, timestamp int) error {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	if v, ok := s.m[key]; ok {
		v.Val = make([]byte, 0)
		v.Active = false
		v.Timestamp = timestamp
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
