package store

import "fmt"

type Key [32]byte

func (k *Key) Equals(other Key) bool {
	for i, _ := range k {
		if k[i] != other[i] {
			return false
		}
	}
	return true
}

func (k *Key) greater(other Key, equals bool) bool {
	for i, _ := range k {
		if k[i] > other[i] {
			return true
		} else if k[i] < other[i] {
			return false
		}
	}
	return equals
}

func (k *Key) lesser(other Key, equals bool) bool {
	for i, _ := range k {
		if k[i] < other[i] {
			return true
		} else if k[i] > other[i] {
			return false
		}
	}
	return equals
}

func (k *Key) GreaterThan(other Key) bool {
	return k.greater(other, false)
}

func (k *Key) GreaterEquals(other Key) bool {
	return k.greater(other, true)
}

func (k *Key) LessThan(other Key) bool {
	return k.lesser(other, false)
}

func (k *Key) LessEquals(other Key) bool {
	return k.lesser(other, true)
}

func (k *Key) String() string {
	return fmt.Sprintf("%x", k[:])
}
