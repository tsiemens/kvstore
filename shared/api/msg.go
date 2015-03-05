package api

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/util"

// Base Command codes that the server will expect to receive
const CmdPut = 0x01
const CmdGet = 0x02
const CmdRemove = 0x03
const CmdShutdown = 0x04
const CmdStatusUpdate = 0x21
const CmdAdhocUpdate = 0x22
const CmdMembership = 0x23
const CmdMembershipExchange = 0x25
const CmdMembershipQuery = 0x27
const CmdMembershipFailure = 0x28
const CmdMembershipFailureGossip = 0x29

// Response codes that can be sent back to the client
const RespOk = 0x00
const RespInvalidKey = 0x01
const RespOutOfSpace = 0x02
const RespSysOverload = 0x03
const RespInternalError = 0x04
const RespUnknownCommand = 0x05
const RespStatusUpdateFail = 0x06
const RespStatusUpdateOK = 0x07
const RespAdhocUpdateOK = 0x08
const RespMalformedDatagram = 0x09

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

func NewBaseDgram(msgUID [16]byte, command byte) Message {
	return &BaseDgram{
		uid:     msgUID,
		command: command,
	}
}

func NewKeyDgram(msgUID [16]byte, command byte, key [32]byte) *KeyDgram {
	return &KeyDgram{
		BaseDgram: BaseDgram{
			uid:     msgUID,
			command: command,
		},
		Key: key,
	}
}

func NewKeyValueDgram(msgUID [16]byte, command byte,
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

func NewValueDgram(msgUID [16]byte, command byte, value []byte) *ValueDgram {
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
func NewMessageUID(addr *net.UDPAddr) [16]byte {
	buf := new(bytes.Buffer)
	if binary.Write(buf, binary.BigEndian, addr.IP.To4()) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(addr.Port)) != nil ||
		binary.Write(buf, binary.LittleEndian, int16(util.Rand.Int())) != nil ||
		binary.Write(buf, binary.LittleEndian, util.UnixMilliTimestamp()) != nil {
		log.E.Panic("binary.Write failed!")
	}
	return ByteArray16(buf.Bytes())
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

/* Parses message.
 * Returns the message, or an error, plus a response message for the error */
func ParseMessage(dgram []byte,
	parserMap map[byte]MessagePayloadParser) (Message, error, Message) {

	if len(dgram) < 17 {
		return nil, errors.New("Datagram header improperly formatted"), nil
	}

	uid := dgram[:16]
	command := dgram[16]

	if parser, ok := parserMap[command]; ok {
		msg, err := parser(ByteArray16(uid), command, dgram[17:])
		if err != nil {
			return nil, err,
				NewBaseDgram(ByteArray16(uid), RespMalformedDatagram)
		} else {
			return msg, nil, nil
		}
	} else {
		return nil,
			errors.New(fmt.Sprintf("Could not parse unrecognized command 0x%x", command)),
			NewBaseDgram(ByteArray16(uid), RespUnknownCommand)
	}
}

func ParseBaseDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	return NewBaseDgram(uid, cmd), nil
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

	return ByteArray32(key), nil
}

func ParseKeyDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	key, err := parseKey(payload)
	if err != nil {
		return nil, err
	}
	return NewKeyDgram(uid, cmd, key), nil
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
	return NewKeyValueDgram(uid, cmd, key, value), nil
}

func ParseValueDgram(uid [16]byte, cmd byte, payload []byte) (Message, error) {
	value, err := parseMultiLengthValue(payload)
	if err != nil {
		return nil, err
	}
	return NewValueDgram(uid, cmd, value), nil
}
