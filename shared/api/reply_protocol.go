package api

import "net"
import "github.com/tsiemens/kvstore/shared/log"

type RequestMessageHandler interface {
	HandleRequestMessage(msg *RequestMessage, recvAddr *net.UDPAddr)
}

func LoopReceiver(conn *net.UDPConn, handler RequestMessageHandler) error {
	for {
		msg, recvAddr, err := recvFrom(conn)
		if err != nil {
			log.E.Println(err)
			if !err.Temporary() {
				return err
			}
		} else {
			log.D.Println("Received message from", recvAddr)
			go handler.HandleRequestMessage(msg, recvAddr)
		}
	}
}

func recvFrom(conn *net.UDPConn) (*RequestMessage, *net.UDPAddr, net.Error) {
	buff := make([]byte, MaxMessageSize)

	for {
		n, recvAddr, err := conn.ReadFromUDP(buff)
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				return nil, recvAddr, netErr
			}
			log.E.Println(err)
		} else {
			//log.D.Printf("Received [% x]\n", buff[0:60])
			requestMsg, err := parseRequestMessage(buff[0:n])
			if err != nil {
				log.E.Println(err)
			} else {
				return requestMsg, recvAddr, nil
			}
		}
	}
}

func reply(conn *net.UDPConn, recvAddr *net.UDPAddr,
	uid [16]byte, respCode byte, value []byte) {

	reply := newResponseMessage(uid, respCode, value)
	conn.WriteTo(reply.Bytes(), recvAddr)
	//log.D.Printf("Sent: [% x]\n", reply.Bytes())
}

func ReplyToGet(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg *RequestMessage, value []byte) {
	var respCode byte
	if value != nil {
		respCode = RespOk
	} else {
		respCode = RespInvalidKey
	}
	reply(conn, recvAddr, requestMsg.UID, respCode, value)
}

func ReplyToPut(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg *RequestMessage, success bool) {
	var respCode byte
	if success {
		respCode = RespOk
	} else {
		respCode = RespInternalError
	}
	reply(conn, recvAddr, requestMsg.UID, respCode, nil)
}

func ReplyToRemove(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg *RequestMessage, success bool) {
	var respCode byte
	if success {
		respCode = RespOk
	} else {
		respCode = RespInvalidKey
	}
	reply(conn, recvAddr, requestMsg.UID, respCode, nil)
}

func ReplyToStatusUpdateServer(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg *RequestMessage, statusResult []byte, success bool) {
	var respCode byte

	if !success {
		respCode = RespStatusUpdateFail
		reply(conn, recvAddr, requestMsg.UID /*this is probably wrong*/, respCode, statusResult)
		return
	}

	switch requestMsg.Command {
	case CmdStatusUpdate:
		respCode = RespStatusUpdateOK
	case CmdAdhocUpdate:
		respCode = RespAdhocUpdateOK
	default:
		respCode = RespOk
	}

	//TODO - figure out what to do with UID
	reply(conn, recvAddr, requestMsg.UID /*this is probably wrong*/, respCode, statusResult)

}

func NotifyStatusUpdate(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg *RequestMessage) {
}

func ReplyToUnknownCommand(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg *RequestMessage) {
	reply(conn, recvAddr, requestMsg.UID, RespUnknownCommand, nil)
}

func Debug_ReplyWithBadUID(conn *net.UDPConn, recvAddr *net.UDPAddr) {
	reply(conn, recvAddr, [16]byte{}, RespOk, nil)
}
