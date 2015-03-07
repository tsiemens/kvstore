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

func (k *Key) Between(lower Key, higher Key) bool {
	if lower.Equals(higher) {
		return true
	} else if lower.GreaterThan(higher) {
		// Looped the circle
		return (lower.LessThan(*k) && higher.LessEquals(*k)) ||
			(lower.GreaterThan(*k) && higher.GreaterEquals(*k))
	} else {
		return (lower.LessThan(*k) && higher.GreaterEquals(*k))
	}
}

func (k *Key) String() string {
	return fmt.Sprintf("%x", k[:])
}

// Needed to implement sort.Interface
type Keys []Key

func (slice Keys) Len() int {
	return len(slice)
}

func (slice Keys) Less(i, j int) bool {
	return slice[i].LessThan(slice[j])
}

func (slice Keys) Swap(i, j int) {
	slice[i], slice[j] = slice[j], slice[i]
}
