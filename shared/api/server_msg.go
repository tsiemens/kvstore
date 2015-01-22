package api

import (
	"bytes"
	"encoding/binary"
	"errors"
)

type ServerMessage struct {
	UID  []byte // The identifier sent in the corresponding request
	Code []byte
}

// Parses a server datagram, and returns a ServerMessage representation
// Server message is of form [request uid [16]byte | codeLen int32 |
//							  code []byte | padding ]
func parseServerMessage(dgram []byte) (*ServerMessage, error) {
	if len(dgram) <= 20 {
		return nil, errors.New("Datagram header improperly formatted")
	}

	uid := dgram[:16]
	buf := bytes.NewBuffer(dgram[16:])

	var codeLen int32
	err := binary.Read(buf, binary.LittleEndian, &codeLen)
	if err != nil {
		return nil, err
	}

	if int(codeLen) > len(dgram)-4 {
		return nil, errors.New("Server datagram invalid")
	}

	code := make([]byte, codeLen)
	bytesRead, err := buf.Read(code)
	if err != nil {
		return nil, err
	}

	return &ServerMessage{UID: uid, Code: code[:bytesRead]}, nil
}
