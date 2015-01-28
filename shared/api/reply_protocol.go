package api

import "net"
import "github.com/tsiemens/kvstore/shared/log"

type ClientMessageHandler interface {
	HandleClientMessage(msg *ClientMessage, recvAddr *net.UDPAddr)
}

func LoopReceiver(conn *net.UDPConn, handler ClientMessageHandler) error {
	for {
		msg, recvAddr, err := recvFromClient(conn)
		if err != nil {
			log.E.Println(err)
			if !err.Temporary() {
				return err
			}
		} else {
			go handler.HandleClientMessage(msg, recvAddr)
		}
	}
}

func recvFromClient(conn *net.UDPConn) (*ClientMessage, *net.UDPAddr, net.Error) {
	buff := make([]byte, MaxMessageSize)

	for {
		n, recvAddr, err := conn.ReadFromUDP(buff)
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				return nil, recvAddr, netErr
			}
			log.E.Println(err)
		} else {
			log.D.Printf("Received [% x]\n", buff[0:60])
			clientMsg, err := parseClientMessage(buff[0:n])
			if err != nil {
				log.E.Println(err)
			} else {
				return clientMsg, recvAddr, nil
			}
		}
	}
}

func reply(conn *net.UDPConn, clientAddr *net.UDPAddr,
	uid [16]byte, respCode byte, value []byte) {

	reply := newServerMessage(uid, respCode, value)
	conn.WriteTo(reply.Bytes(), clientAddr)
	log.D.Printf("Sent: [% x]\n", reply.Bytes())
}

func ReplyToGet(conn *net.UDPConn, clientAddr *net.UDPAddr,
	clientMsg *ClientMessage, value []byte) {
	var respCode byte
	if value != nil {
		respCode = RespOk
	} else {
		respCode = RespInvalidKey
	}
	reply(conn, clientAddr, clientMsg.UID, respCode, value)
}

func ReplyToPut(conn *net.UDPConn, clientAddr *net.UDPAddr,
	clientMsg *ClientMessage, success bool) {
	var respCode byte
	if success {
		respCode = RespOk
	} else {
		respCode = RespInternalError
	}
	reply(conn, clientAddr, clientMsg.UID, respCode, nil)
}

func ReplyToRemove(conn *net.UDPConn, clientAddr *net.UDPAddr,
	clientMsg *ClientMessage, success bool) {
	var respCode byte
	if success {
		respCode = RespOk
	} else {
		respCode = RespInvalidKey
	}
	reply(conn, clientAddr, clientMsg.UID, respCode, nil)
}

func ReplyToUnknownCommand(conn *net.UDPConn, clientAddr *net.UDPAddr,
	clientMsg *ClientMessage) {
	reply(conn, clientAddr, clientMsg.UID, RespUnknownCommand, nil)
}

func Debug_ReplyWithBadUID(conn *net.UDPConn, clientAddr *net.UDPAddr) {
	reply(conn, clientAddr, [16]byte{}, RespOk, nil)
}
