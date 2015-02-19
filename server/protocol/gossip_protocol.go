package protocol

import (
	"fmt"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

func Gossip(conn *net.UDPConn, msg *api.KeyValueDgram) {
	conf := config.GetConfig()
	thisNode := node.GetProcessNode()
	for i := 0; i < conf.NotifyCount; i++ {
		peer, _ := thisNode.RandomPeer()
		if peer == nil {
			log.E.Println("Could not gossip; no peers")
			return
		}
		addr := peer.Addr
		requestMsg := api.NewKeyValueDgram(api.NewMessageUID(addr),
			api.CmdStatusUpdate, msg.Key, msg.Value)
		log.D.Println("Gossiping to", addr)
		_, err := conn.WriteTo(requestMsg.Bytes(), addr)
		if err != nil {
			fmt.Println(err)
		}
	}
}
