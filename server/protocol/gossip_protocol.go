package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
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
		log.D.Println("Gossiping to", addr)
		_, err := conn.WriteTo(msg.Bytes(), addr)
		if err != nil {
			log.E.Println(err)
		}
	}
}

func InitMembershipGossip(conn *net.UDPConn, peerId *store.Key, failedPeer *node.Peer) {
	peerdata := map[store.Key]*node.Peer{
		*peerId: failedPeer,
	}
	payload, err := json.Marshal(NewPeerList(peerdata))
	if err != nil {
		log.E.Println(err)
		return
	}
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
			api.CmdMembershipFailureGossip, thisNode.ID, payload)
		log.D.Println("Gossiping to", addr)
		_, err = conn.WriteTo(requestMsg.Bytes(), addr)
		if err != nil {
			log.E.Println(err)
		}
	}
}
