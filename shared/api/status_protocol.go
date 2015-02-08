package api

import "net"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/server/config"
import "time"

type StatusMessageHandler interface {
	HandleStatusMessage(msg *ResponseMessage, recvAddr *net.UDPAddr)
}

func StatusReceiver(conn *net.UDPConn, handler StatusMessageHandler) error {
	go periodicStatusUpdate(conn)
	for {
		msg, recvAddr, err := recvFromStatus(conn)
		if err != nil {
			log.E.Println(err)
			if !err.Temporary() {
				return err
			}
		} else {
			//log.D.Println("Received message from", recvAddr)
			go handler.HandleStatusMessage(msg, recvAddr)
		}
	}
}

func periodicStatusUpdate(conn *net.UDPConn) {
	conf := config.GetConfig()
	for {
		time.Sleep(conf.UpdateFrequency)
		key, err := NewRandKey()
		if err != nil {
			log.E.Println(err)
		}
		err = StatusUpdate(conf.GetRandAddr(), key)
		if err != nil {
			log.E.Println(err)
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
			//log.D.Printf("Received [% x]\n", buff[0:60])
			responseMsg, err := parseResponseMessage(buff[0:n])
			if err != nil {
				log.E.Println(err)
			} else {
				return responseMsg, recvAddr, nil
			}
		}
	}
}
