package store

import (
	"errors"
	"github.com/tsiemens/kvstore/shared/log"
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

// Puts this value and increments the timestamp. Returns the new timestamp for the value
func (s *Store) WriteInc(key Key, value []byte, active bool) (int, error) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	lastVal, ok := s.m[key]
	v := &StoreVal{Val: value, Active: active}
	if ok {
		v.Timestamp = lastVal.Timestamp + 1
	} else if active {
		v.Timestamp = 1
	} else {
		return 0, errors.New("No value for " + key.String())
	}
	s.m[key] = v
	return v.Timestamp, nil
}

func (s *Store) WriteIfNewer(key Key, value []byte, active bool, timestamp int) {
	s.Lock.Lock()
	defer s.Lock.Unlock()
	lastVal, ok := s.m[key]
	if ok && lastVal.Timestamp > timestamp {
		log.E.Printf("Tried to put lower timestamp for %s\n", key.String())
	} else {
		s.m[key] = &StoreVal{Val: value, Active: active, Timestamp: timestamp}
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
