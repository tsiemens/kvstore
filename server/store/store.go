package store

import "errors"
import "github.com/tsiemens/kvstore/shared/log"

// representation of the consistent hashing store
type Store struct {
	m       map[Key][]byte
	baseKey Key
	maxKey  Key
}

func New() *Store {
	return &Store{
		m: make(map[Key][]byte),
		// Temporary for A3
		baseKey: Key([32]byte{}),
		maxKey:  makeMaxKey(),
	}
}

func (s *Store) Get(key Key) ([]byte, error) {
	var val []byte
	if s.IsMyKey(key) {
		v, ok := s.m[key]
		val = v
		if !ok {
			return nil, errors.New("No value for " + key.String())
		}
	} else {
		log.D.Println("TODO Was told to get not my key")
	}
	return val, nil
}

func (s *Store) Put(key Key, value []byte) error {
	if s.IsMyKey(key) {
		s.m[key] = value
	} else {
		log.D.Println("TODO Was told to put not my key")
	}
	return nil
}

func (s *Store) Remove(key Key) error {
	if s.IsMyKey(key) {
		if _, ok := s.m[key]; ok {
			delete(s.m, key)
		} else {
			return errors.New("No value for " + key.String())
		}
	} else {
		log.D.Println("TODO Was told to remove not my key")
	}
	return nil
}

func (s *Store) IsMyKey(key Key) bool {
	return key.GreaterEquals(s.baseKey) && key.LessEquals(s.maxKey)
}

func makeMaxKey() Key {
	key := [32]byte{}
	for i, _ := range key {
		key[i] = 0xFF
	}
	return Key(key)
}
