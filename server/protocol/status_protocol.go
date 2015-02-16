package protocol

import (
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
	"time"
)

type StatusMessageHandler interface {
	HandleStatusMessage(msg api.Message, recvAddr *net.UDPAddr)
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
	node := node.GetProcessNode()
	for {
		peer, key := node.RandomPeer()
		if peer != nil {
			err := api.StatusUpdate(conn, peer.Addr.String(), *key)
			if err != nil {
				log.E.Println(err)
			}
		}
		time.Sleep(conf.UpdateFrequency)
	}
}

func recvFromStatus(conn *net.UDPConn) (api.Message, *net.UDPAddr, net.Error) {
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
			responseMsg, err := api.ParseMessage(buff[0:n],
				MessageParsers)
			if err != nil {
				log.E.Println(err)
			} else {
				return responseMsg, recvAddr, nil
			}
		}
	}
}
