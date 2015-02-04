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
const RespStatusUpdateFail = 0x06
const RespStatusUpdateOK = 0x07
const RespAdhocUpdateOK = 0x08

type ResponseMessage struct {
	UID          [16]byte // The identifier sent in the corresponding request
	ResponseCode byte
	Value        []byte
}

func newResponseMessage(reqUID [16]byte, respCode byte, value []byte) *ResponseMessage {
	if value == nil {
		value = make([]byte, 0)
	}
	return &ResponseMessage{
		UID:          reqUID,
		ResponseCode: respCode,
		Value:        value,
	}
}

// Returns the payload for this message
// Is of form [response [1]byte | (optional) val length int16 | value [<=15,000]byte ]
func (msg *ResponseMessage) Payload() []byte {
	buf := new(bytes.Buffer)
	buf.WriteByte(msg.ResponseCode)
	binary.Write(buf, binary.LittleEndian, int16(len(msg.Value)))
	buf.Write(msg.Value)
	return buf.Bytes()
}

// Response message is of form [request uid [16]byte | payload ]
func (msg *ResponseMessage) Bytes() []byte {
	return append(msg.UID[:], msg.Payload()...)
}

func (msg *ResponseMessage) Error() error {
	switch msg.ResponseCode {
	case RespOk:
		return nil
	case RespInvalidKey:
		return errors.New("Non-existent key requested")
	case RespOutOfSpace:
		return errors.New("Response out of space")
	case RespSysOverload:
		return errors.New("System overload")
	case RespInternalError:
		return errors.New("Internal KVStore failure")
	case RespUnknownCommand:
		return errors.New("Unrecognized command")
	default:
		return nil
	}
}

// Parses a response datagram, and returns a ResponseMessage representation
func parseResponseMessage(dgram []byte) (*ResponseMessage, error) {
	if len(dgram) < 17 {
		return nil, errors.New("Datagram header improperly formatted")
	}

	uid := dgram[:16]
	buf := bytes.NewBuffer(dgram[16:])

	respCode, err := buf.ReadByte()
	if err != nil {
		return nil, err
	}

	responseMsg := &ResponseMessage{UID: byteArray16(uid), ResponseCode: respCode}

	if len(dgram) >= 19 {
		var valueLen int16
		err := binary.Read(buf, binary.LittleEndian, &valueLen)
		if err != nil {
			return nil, err
		}

		if int(valueLen) > len(dgram)-19 {
			return nil, errors.New("Response datagram invalid")
		}

		value := make([]byte, valueLen)
		bytesRead, err := buf.Read(value)
		if err != nil {
			return nil, err
		}

		responseMsg.Value = value[:bytesRead]
	}

	return responseMsg, nil
}
