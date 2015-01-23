package api

// Utilities for creating byte arrays
// (useful in fixed size portions of messages)

func byteArray32(slice []byte) [32]byte {
	var arr [32]byte
	for i := 0; i < 32; i++ {
		arr[i] = slice[i]
	}
	return arr
}

func byteArray16(slice []byte) [16]byte {
	var arr [16]byte
	for i := 0; i < 16; i++ {
		arr[i] = slice[i]
	}
	return arr
}
