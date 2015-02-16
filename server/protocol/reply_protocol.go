package protocol

import "net"
import "github.com/tsiemens/kvstore/shared/api"

func ReplyToGet(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, value []byte) {
	var reply api.Message
	if value != nil {
		reply = api.NewValueDgram(requestMsg.UID(), api.RespOk, value)
	} else {
		reply = api.NewBaseDgram(requestMsg.UID(), api.RespInvalidKey)
	}
	conn.WriteTo(reply.Bytes(), recvAddr)
}

func ReplyToPut(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, success bool) {
	var respCode byte
	if success {
		respCode = api.RespOk
	} else {
		respCode = api.RespInternalError
	}
	conn.WriteTo(api.NewBaseDgram(requestMsg.UID(), respCode).Bytes(),
		recvAddr)
}

func ReplyToRemove(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, success bool) {
	var respCode byte
	if success {
		respCode = api.RespOk
	} else {
		respCode = api.RespInvalidKey
	}
	conn.WriteTo(api.NewBaseDgram(requestMsg.UID(), respCode).Bytes(),
		recvAddr)
}

func ReplyToStatusUpdateServer(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, statusResult []byte, success bool) {
	var respCode byte

	if !success {
		respCode = api.RespStatusUpdateFail
		conn.WriteTo(api.NewValueDgram(requestMsg.UID(), respCode,
			statusResult).Bytes(),
			recvAddr)
		return
	}

	switch cmd := requestMsg.Command(); cmd {
	case api.CmdStatusUpdate:
		respCode = api.RespStatusUpdateOK
	case api.CmdAdhocUpdate:
		respCode = api.RespAdhocUpdateOK
	default:
		respCode = api.RespOk
	}

	//TODO - figure out what to do with UID
	conn.WriteTo(api.NewValueDgram(requestMsg.UID(), respCode,
		statusResult).Bytes(),
		recvAddr)
}

func NotifyStatusUpdate(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message) {
}

func ReplyToUnknownCommand(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message) {
	conn.WriteTo(api.NewBaseDgram(requestMsg.UID(),
		api.RespUnknownCommand).Bytes(),
		recvAddr)
}

func Debug_ReplyWithBadUID(conn *net.UDPConn, recvAddr *net.UDPAddr) {
	conn.WriteTo(api.NewBaseDgram([16]byte{}, api.RespOk).Bytes(),
		recvAddr)
}
