package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"net"
)

import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/util"

var myRand = rand.New(rand.NewSource(util.UnixMilliTimestamp()))

const CmdPut = 0x01
const CmdGet = 0x02
const CmdRemove = 0x03

type ClientMessage struct {
	UID     [16]byte
	Command byte
	Key     [32]byte
	Value   []byte
}

func newClientMessage(addr *net.UDPAddr, command byte, key [32]byte, value []byte) *ClientMessage {
	if addr == nil {
		return nil
	}
	msg := &ClientMessage{
		UID:     newUID(addr),
		Command: command,
		Key:     key,
		Value:   value,
	}
	return msg
}

// Returns the 16 byte Unique ID
// Is of form [ip [4]byte | port int16 | rand int16 | timestamp int64]
func newUID(addr *net.UDPAddr) [16]byte {
	buf := new(bytes.Buffer)
	if binary.Write(buf, binary.BigEndian, addr.IP.To4()) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(addr.Port)) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(myRand.Int())) != nil ||
		binary.Write(buf, binary.LittleEndian, util.UnixMilliTimestamp()) != nil {
		log.E.Panic("binary.Write failed!")
	}
	return byteArray16(buf.Bytes())
}

// Returns the payload for this message
// Is of form [command [1]byte | key [32]byte | (optional) val length int16 |
//				 value [<=15,000]byte ]
func (msg *ClientMessage) Payload() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(msg.Command)
	buf.Write(msg.Key[:])
	binary.Write(buf, binary.LittleEndian, int16(len(msg.Value)))
	buf.Write(msg.Value)
	return buf.Bytes()
}

// Creates a byte/message representation of the message
// Is of form [UID [16]byte |  payload ]
func (msg *ClientMessage) Bytes() []byte {
	return append(msg.UID[:], msg.Payload()...)
}

func expectsKeyForCommand(command byte) bool {
	return command == CmdGet ||
		command == CmdPut ||
		command == CmdRemove
}

func expectsValueForCommand(command byte) bool {
	return command == CmdPut
}

// Parses a client datagram, and returns a ClientMessage representation
func parseClientMessage(dgram []byte) (*ClientMessage, error) {
	if len(dgram) < 17 {
		return nil, errors.New("Datagram header improperly formatted")
	}

	uid := dgram[:16]
	buf := bytes.NewBuffer(dgram[16:])

	command, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	clientMsg := &ClientMessage{UID: byteArray16(uid), Command: command}

	if expectsKeyForCommand(command) {
		key := make([]byte, 32)
		bytesRead, err := buf.Read(key)
		if err != nil {
			return nil, err
		} else if bytesRead != 32 {
			return nil, errors.New("Client datagram missing key")
		}

		clientMsg.Key = byteArray32(key)

		if expectsValueForCommand(command) {
			var valueLen int16
			err := binary.Read(buf, binary.LittleEndian, &valueLen)
			if err != nil {
				return nil, err
			}

			value := make([]byte, valueLen)
			bytesRead, err := buf.Read(value)
			if err != nil {
				return nil, err
			}
			if bytesRead != int(valueLen) {
				return nil, errors.New("Value length mismatch")
			}

			clientMsg.Value = value[:bytesRead]
		}
	}

	return clientMsg, nil
}
