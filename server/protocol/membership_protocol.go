package protocol

import (
	"encoding/json"
	"errors"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
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

func SendMembershipMsg(conn *net.UDPConn, addr *net.UDPAddr, myNodeId [32]byte,
	peers map[store.Key]*node.Peer, isReply bool) error {
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
			code, myNodeId, peerdata)
	})
}

func SendMembershipQuery(url string) (map[store.Key]*node.Peer, error) {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewBaseDgram(api.NewMessageUID(addr), api.CmdMembershipQuery)
	})
	if err != nil {
		return nil, err
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return nil, cmdErr
	} else if vmsg, ok := msg.(*api.ValueDgram); ok {
		peers := &PeerList{}
		err := json.Unmarshal(vmsg.Value, peers)
		return peers.PointerMap(), err
	} else {
		return nil, errors.New("Received invalid membership qeury datagram")
	}
}
