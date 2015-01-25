package api

import "github.com/tsiemens/kvstore/shared/log"
import "net"

type ClientMessageHandler func(msg *ClientMessage, recvAddr *net.UDPAddr)

func LoopReceiver(conn *net.UDPConn, handler ClientMessageHandler, exit chan bool) {
	for {
		msg, recvAddr, err := recvFromClient(conn)
		if err != nil {
			log.E.Println(err)
			if !err.Temporary() {
				exit <- true
				return
			}
		} else {
			go handler(msg, recvAddr)
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
