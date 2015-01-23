package api

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const RespOk = 0x00
const RespInvalidKey = 0x01
const RespOutOfSpace = 0x02
const RespSysOverload = 0x03
const RespInternalError = 0x04
const RespUnknownCommand = 0x05

type ServerMessage struct {
	UID          [16]byte // The identifier sent in the corresponding request
	ResponseCode byte
	Value        []byte
}

func newServerMessage(reqUID [16]byte, respCode byte, value []byte) *ServerMessage {
	return &ServerMessage{
		UID:          reqUID,
		ResponseCode: respCode,
		Value:        value,
	}
}

// Returns the payload for this message
// Is of form [response [1]byte | (optional) val length int16 | value [<=15,000]byte ]
func (msg *ServerMessage) Payload() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(msg.ResponseCode)
	binary.Write(buf, binary.LittleEndian, int16(len(msg.Value)))
	buf.Write(msg.Value)
	return buf.Bytes()
}

// Server message is of form [request uid [16]byte | payload ]
func (msg *ServerMessage) Bytes() []byte {
	return append(msg.UID[:], msg.Payload()...)
}

// Parses a server datagram, and returns a ServerMessage representation
func parseServerMessage(dgram []byte) (*ServerMessage, error) {
	if len(dgram) < 17 {
		return nil, errors.New("Datagram header improperly formatted")
	}

	uid := dgram[:16]
	buf := bytes.NewBuffer(dgram[16:])

	respCode, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	serverMsg := &ServerMessage{UID: byteArray16(uid), ResponseCode: respCode}

	if len(dgram) >= 19 {
		var valueLen int16
		err := binary.Read(buf, binary.LittleEndian, &valueLen)
		if err != nil {
			return nil, err
		}

		if int(valueLen) > len(dgram)-19 {
			return nil, errors.New("Server datagram invalid")
		}

		value := make([]byte, valueLen)
		bytesRead, err := buf.Read(value)
		if err != nil {
			return nil, err
		}

		serverMsg.Value = value[:bytesRead]
	}

	return serverMsg, nil
}
