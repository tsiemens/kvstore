package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	//	"math/rand"
	"net"
	//	"fmt"
)

import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/util"

//var myRand = rand.New(rand.NewSource(util.UnixMilliTimestamp()))

type BaseDgram struct {
	uid     [16]byte
	command byte
}

type KeyDgram struct {
	BaseDgram
	Key [32]byte
}

type KeyValueDgram struct {
	KeyDgram
	Value []byte
}

type ValueDgram struct {
	BaseDgram
	Value []byte
}

type Message interface {
	UID() [16]byte
	Command() byte
	Bytes() []byte
}

func (d *BaseDgram) UID() [16]byte {
	return d.uid
}

func (d *BaseDgram) Command() byte {
	return d.command
}

func newMessage(msgUID [16]byte, command byte) Message {
	return &BaseDgram{
		uid:     msgUID,
		command: command,
	}
}

func newKeyDgram(msgUID [16]byte, command byte, key [32]byte) *KeyDgram {
	return &KeyDgram{
		BaseDgram: BaseDgram{
			uid:     msgUID,
			command: command,
		},
		Key: key,
	}
}

func newKeyValueDgram(msgUID [16]byte, command byte,
	key [32]byte, value []byte) *KeyValueDgram {
	return &KeyValueDgram{
		KeyDgram: KeyDgram{
			BaseDgram: BaseDgram{
				uid:     msgUID,
				command: command,
			},
			Key: key,
		},
		Value: value,
	}
}

func newValueDgram(msgUID [16]byte, command byte, value []byte) *ValueDgram {
	return &ValueDgram{
		BaseDgram: BaseDgram{
			uid:     msgUID,
			command: command,
		},
		Value: value,
	}
}

// Returns the 16 byte Unique ID
// Is of form [ip [4]byte | port int16 | rand int16 | timestamp int64]
func xnewUID(addr *net.UDPAddr) [16]byte {
	buf := new(bytes.Buffer)
	if binary.Write(buf, binary.BigEndian, addr.IP.To4()) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(addr.Port)) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(myRand.Int())) != nil ||
		binary.Write(buf, binary.LittleEndian, util.UnixMilliTimestamp()) != nil {
		log.E.Panic("binary.Write failed!")
	}
	return byteArray16(buf.Bytes())
}

// Returns byte datagram representation of message
// [UID (16 bytes), command (1 byte)]
func (msg *BaseDgram) Bytes() []byte {
	buf := new(bytes.Buffer)
	buf.Write(msg.uid[:])
	buf.WriteByte(msg.command)
	return buf.Bytes()
}

// Returns byte datagram representation of message
// [base data | key (32 bytes)]
func (msg *KeyDgram) Bytes() []byte {
	return append(msg.BaseDgram.Bytes(), msg.Key[:]...)
}

func valueBytes(value []byte) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int16(len(value)))
	buf.Write(value)
	return buf.Bytes()
}

// Returns byte datagram representation of message
// Is of form [key dgram data | val length int16 |
//				 value [<=15,000]byte ]
func (msg *KeyValueDgram) Bytes() []byte {
	return append(msg.KeyDgram.Bytes(), valueBytes(msg.Value)...)
}

func (msg *ValueDgram) Bytes() []byte {
	return append(msg.BaseDgram.Bytes(), valueBytes(msg.Value)...)
}

type MessagePayloadParser func(uid [16]byte, cmd byte,
	payload []byte) (Message, error)

func ParseMessage(dgram []byte,
	parserMap map[byte]MessagePayloadParser) (Message, error) {

	if len(dgram) < 17 {
		return nil, errors.New("Datagram header improperly formatted")
	}

	uid := dgram[:16]
	buf := bytes.NewBuffer(dgram[16:])

	command, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	if parser, ok := parserMap[command]; ok {
		return parser(byteArray16(uid), command, dgram[17:])
	} else {
		return nil, errors.New("Unrecognized command " + string(command))
	}
}

func ParseBaseDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	return newMessage(uid, cmd), nil
}

func parseKey(b []byte) ([32]byte, error) {
	buf := bytes.NewBuffer(b)
	key := make([]byte, 32)
	bytesRead, err := buf.Read(key)
	if err != nil {
		return [32]byte{}, err
	} else if bytesRead != 32 {
		return [32]byte{}, errors.New("Too few bytes to parse key")
	}

	return byteArray32(key), nil
}

func ParseKeyDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	key, err := parseKey(payload)
	if err != nil {
		return nil, err
	}
	return newKeyDgram(uid, cmd, key), nil
}

func parseMultiLengthValue(b []byte) ([]byte, error) {
	buf := bytes.NewBuffer(b)
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

	return value[:bytesRead], nil
}

func ParseKeyValueDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	key, err := parseKey(payload)
	if err != nil {
		return nil, err
	}

	value, err := parseMultiLengthValue(payload[32:])
	if err != nil {
		return nil, err
	}
	return newKeyValueDgram(uid, cmd, key, value), nil
}

func ParseValueDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	value, err := parseMultiLengthValue(payload)
	if err != nil {
		return nil, err
	}
	return newValueDgram(uid, cmd, value), nil
}
