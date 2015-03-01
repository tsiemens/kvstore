package protocol

import (
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
	"time"
)

var StatusMessageParsers = map[byte]api.MessagePayloadParser{
	api.CmdStatusUpdate: api.ParseKeyValueDgram,
	api.CmdAdhocUpdate:  api.ParseKeyValueDgram,

	api.RespOk:             api.ParseValueDgram,
	api.RespSysOverload:    api.ParseBaseDgram,
	api.RespInternalError:  api.ParseBaseDgram,
	api.RespUnknownCommand: api.ParseBaseDgram,
	api.RespStatusUpdateOK: api.ParseValueDgram,
	api.RespAdhocUpdateOK:  api.ParseValueDgram,
}

type StatusMessageHandler interface {
	HandleStatusMessage(msg api.Message, recvAddr *net.UDPAddr)
	HandlePeerListUpdate(peers map[store.Key]*node.Peer)
}

func StatusReceiver(conn *net.UDPConn, handler StatusMessageHandler) error {
	go periodicStatusUpdate(conn, handler)
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

func periodicStatusUpdate(conn *net.UDPConn, handler StatusMessageHandler) {
	conf := config.GetConfig()
	for {
		randomPeer := node.RandomWellKnownPeer()
		if randomPeer != nil {
			peers, err := SendMembershipQuery(randomPeer.Addr.String())
			if err != nil {
				log.E.Println(err)
			} else {
				handler.HandlePeerListUpdate(peers)
				randKey, err := api.NewRandKey()
				if err != nil {
					log.E.Println(err)
				} else {
					err := api.StatusUpdate(conn, randomPeer.Addr.String(), randKey)
					if err != nil {
						log.E.Println(err)
					}
				}
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
			responseMsg, err, _ := api.ParseMessage(buff[0:n],
				StatusMessageParsers)
			if err != nil {
				log.E.Println(err)
			} else {
				return responseMsg, recvAddr, nil
			}
		}
	}
}
