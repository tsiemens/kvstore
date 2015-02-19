package loop

import (
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/protocol"
	"github.com/tsiemens/kvstore/shared/log"
	"time"
)

const MembershipSendFreq = time.Millisecond * 5000

// Starts all default background periodic tasks for the server
func GoAll() {
	if node.GetProcessNode() == nil {
		log.E.Fatal("Process node has not been initialized!")
	}
	if node.GetProcessNode().Conn == nil {
		log.E.Fatal("Process node Connection has not been initialized!")
	}
	go MembershipUpdateLoop()
}

func MembershipUpdateLoop() {
	thisNode := node.GetProcessNode()
	for {
		randPeer, peerId := thisNode.RandomPeer()
		if randPeer != nil {
			err := protocol.SendMembershipMsg(thisNode.Conn, randPeer.Addr,
				thisNode.ID, thisNode.KnownPeers, false)
			if err != nil {
				log.E.Println(err)
				thisNode.SetPeerOffline(*peerId)
			}
		}
		log.D.Printf("Currently known peers: [\n%s\n]\n",
			node.PeerListString(thisNode.KnownPeers))
		time.Sleep(MembershipSendFreq)
	}
}
