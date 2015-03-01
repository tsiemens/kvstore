package api

import "errors"
import "crypto/rand"
import "encoding/hex"

// Utilities for creating byte arrays
// (useful in fixed size portions of messages)

// Creates a 32 byte key.
func NewKey(slice []byte) ([32]byte, error) {
	if len(slice) > 32 {
		return [32]byte{}, errors.New("Key too large: must be 32 bytes max")
	} else {
		return ByteArray32(slice), nil
	}
}

func NewRandKey() ([32]byte, error) {
	k := make([]byte, 32)
	_, err := rand.Read(k)
	if err != nil {
		return [32]byte{}, err
	}
	key, err := NewKey(k)
	if err != nil {
		return [32]byte{}, err
	}
	return key, nil
}

func KeyHex(key [32]byte) string {
	return hex.EncodeToString(key[:])
}

func KeyFromHex(keystring string) (key [32]byte, err error) {
	keyslice, err := hex.DecodeString(keystring)
	if err != nil {
		return
	}

	key, err = NewKey(keyslice)
	return
}

// slice must be at most 32 bytes.
// If slice is less than 32 bytes, it will be right-packed
func ByteArray32(slice []byte) [32]byte {
	var arr [32]byte
	offset := 32 - len(slice)
	for i := 0; i < len(slice); i++ {
		arr[i+offset] = slice[i]
	}
	return arr
}

// slice must be at most 16 bytes.
// If slice is less than 16 bytes, it will be right-packed
func ByteArray16(slice []byte) [16]byte {
	var arr [16]byte
	offset := 16 - len(slice)
	for i := 0; i < len(slice); i++ {
		arr[i+offset] = slice[i]
	}
	return arr
}
