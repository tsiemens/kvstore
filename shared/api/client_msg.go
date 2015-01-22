package api

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"net"
	"time"
)

var myRand = rand.New(rand.NewSource(makeTimestamp()))

type ClientMessage struct {
	HostAddr  *net.UDPAddr
	rand      int16
	timestamp int64
	ID        int32
}

func newClientMessage(addr *net.UDPAddr, studentID int) *ClientMessage {
	if addr == nil {
		return nil
	}
	msg := &ClientMessage{
		HostAddr:  addr,
		rand:      int16(myRand.Int()),
		timestamp: makeTimestamp(),
		ID:        int32(studentID),
	}
	return msg
}

// Returns the 16 byte Unique ID for this message
// Is of form [ip [4]byte | port int16 | rand int16 | timestamp int64]
func (msg *ClientMessage) UID() []byte {
	buf := new(bytes.Buffer)
	if binary.Write(buf, binary.BigEndian, msg.HostAddr.IP.To4()) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(msg.HostAddr.Port)) != nil ||
		binary.Write(buf, binary.LittleEndian, msg.rand) != nil ||
		binary.Write(buf, binary.LittleEndian, msg.timestamp) != nil {
		fmt.Println("Error: binary.Write failed!")
	}
	return buf.Bytes()
}

// Creates a byte/message representation of the message
// Is of form [UID [16]byte | studentID int32 ]
func (msg *ClientMessage) ToBytes() []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, msg.ID)
	return append(msg.UID(), buf.Bytes()...)
}

func makeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
