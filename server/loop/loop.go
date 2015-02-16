package loop

import (
	serverapi "github.com/tsiemens/kvstore/server/api"
	"github.com/tsiemens/kvstore/server/node"
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
			err := serverapi.SendMembershipMsg(thisNode.Conn, randPeer.Addr, thisNode.ID,
				thisNode.KnownPeers)
			if err != nil {
				log.E.Println(err)
				if peerId != nil {
					randPeer.Online = false
					thisNode.KnownPeers[*peerId] = randPeer
				}
			}
		}
		log.D.Printf("Currently known peers: [\n%s\n]\n",
			node.PeerListString(thisNode.KnownPeers))
		time.Sleep(MembershipSendFreq)
	}
}
