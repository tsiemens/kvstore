package api

import "net"
import "github.com/tsiemens/kvstore/shared/log"

type StatusMessageHandler interface {
	HandleStatusMessage(msg *ResponseMessage, recvAddr *net.UDPAddr)
}

func StatusReceiver(conn *net.UDPConn, handler StatusMessageHandler) error {
	for {
		msg, recvAddr, err := recvFromStatus(conn)
		if err != nil {
			log.E.Println(err)
			if !err.Temporary() {
				return err
			}
		} else {
			log.D.Println("Received message from", recvAddr)
			go handler.HandleStatusMessage(msg, recvAddr)
		}
	}
}

func recvFromStatus(conn *net.UDPConn) (*ResponseMessage, *net.UDPAddr, net.Error) {
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
			responseMsg, err := parseResponseMessage(buff[0:n])
			if err != nil {
				log.E.Println(err)
			} else {
				return responseMsg, recvAddr, nil
			}
		}
	}
}
