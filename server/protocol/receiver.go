package protocol

import "net"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/api"

type MessageHandler interface {
	HandleMessage(msg api.Message, recvAddr *net.UDPAddr)
}

func LoopReceiver(conn *net.UDPConn, handler MessageHandler) error {
	for {
		msg, recvAddr, err, errMsg := recvFrom(conn)
		if err != nil {
			if !err.Temporary() {
				return err
			}
		} else if errMsg != nil {
			conn.WriteTo(errMsg.Bytes(), recvAddr)
		} else {
			log.D.Printf("Received message type %x from %v\n", msg.Command(), recvAddr)
			go handler.HandleMessage(msg, recvAddr)
		}
	}
}

/* Receives a message and parses it.
 * Returns the message, the address received from,
 * a possible error or an error message to return. */
func recvFrom(conn *net.UDPConn) (api.Message, *net.UDPAddr, net.Error, api.Message) {
	buff := make([]byte, api.MaxMessageSize)

	for {
		n, recvAddr, err := conn.ReadFromUDP(buff)
		if err != nil {
			log.E.Println(err)
			if netErr, ok := err.(net.Error); ok {
				return nil, recvAddr, netErr, nil
			}
		} else {
			//log.D.Printf("Received [% x]\n", buff[0:60])
			requestMsg, err, errMsg := api.ParseMessage(buff[0:n], api.CmdMessageParsers)
			if err != nil {
				log.E.Println(err)
				log.E.Println("From", recvAddr)
				return nil, recvAddr, nil, errMsg
			} else {
				return requestMsg, recvAddr, nil, nil
			}
		}
	}
}
