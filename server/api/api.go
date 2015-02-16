package api

// In an attempt to avoid import cycles, all server specific UDP sending
// functions should go here.

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
	"time"
)

type PeerList struct {
	Peers map[string]node.Peer
}

func newPeerList(peers map[store.Key]*node.Peer) *PeerList {
	pl := map[string]node.Peer{}
	for key, peer := range peers {
		pl[api.KeyHex(key)] = node.Peer{Online: peer.Online,
			LastSeen: peer.LastSeen,
			Addr:     peer.Addr,
		}
	}
	return &PeerList{pl}
}

func (pl *PeerList) PointerMap() map[store.Key]*node.Peer {
	peers := map[store.Key]*node.Peer{}
	for key, peer := range pl.Peers {
		k, err := api.KeyFromHex(key)
		if err != nil {
			log.E.Println("Failed to parse key " + key)
		} else {
			peers[store.Key(k)] = &node.Peer{Online: peer.Online,
				LastSeen: peer.LastSeen,
				Addr:     peer.Addr,
			}
		}
	}
	return peers
}

type TestPeer struct {
	Online   bool
	LastSeen time.Time
	Addr     net.UDPAddr
}

func SendMembershipMsg(conn *net.UDPConn, addr *net.UDPAddr, myNodeid [32]byte,
	peers map[store.Key]*node.Peer, isReply bool) error {
	//testpeers := map[store.Key]TestPeer{}
	peerdata, err := json.Marshal(newPeerList(peers))
	if err != nil {
		return err
	}
	return api.Send(conn, addr.String(), func(addr *net.UDPAddr) api.Message {
		var code byte
		if isReply { // I really wish go had ternary operators -_-
			code = api.CmdMembership
		} else {
			code = api.CmdMembershipResponse
		}
		return api.NewKeyValueDgram(api.NewMessageUID(addr),
			code, node.GetProcessNode().ID, peerdata)
	})
}
