package protocol

import "net"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/api"

type MessageHandler interface {
	HandleMessage(msg api.Message, recvAddr *net.UDPAddr)
}

func LoopReceiver(conn *net.UDPConn, handler MessageHandler) error {
	for {
		msg, recvAddr, err := recvFrom(conn)
		if err != nil {
			log.E.Println(err)
			if !err.Temporary() {
				return err
			}
		} else {
			log.D.Println("Received message from", recvAddr)
			go handler.HandleMessage(msg, recvAddr)
		}
	}
}

func recvFrom(conn *net.UDPConn) (api.Message, *net.UDPAddr, net.Error) {
	buff := make([]byte, api.MaxMessageSize)

	for {
		n, recvAddr, err := conn.ReadFromUDP(buff)
		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				return nil, recvAddr, netErr
			}
			log.E.Println(err)
		} else {
			//log.D.Printf("Received [% x]\n", buff[0:60])
			requestMsg, err := api.ParseMessage(buff[0:n], api.CmdMessageParsers)
			if err != nil {
				log.E.Println(err)
			} else {
				return requestMsg, recvAddr, nil
			}
		}
	}
}
