package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"net"
	"strconv"
	"time"
)

const timeErr = time.Millisecond * 5
const TimeTillMemberDrop = time.Minute * 5

// Node represents this machine, as one in a cluster of nodes.
type Node struct {
	ID         store.Key // Not needed just yet, but it will later
	KnownPeers map[store.Key]*Peer
	Lock       util.Semaphore
	Conn       *net.UDPConn
}

type Peer struct {
	Online   bool
	LastSeen time.Time
	Addr     *net.UDPAddr
}

var node *Node

func Init(localAddr *net.UDPAddr, conn *net.UDPConn) {
	node = &Node{
		ID:         createNodeID(localAddr),
		KnownPeers: map[store.Key]*Peer{},
		Lock:       util.NewSemaphore(),
		Conn:       conn,
	}

	log.I.Println("Node initialized with ID: " + node.ID.String())
}

func GetProcessNode() *Node {
	return node
}

func createNodeID(localAddr *net.UDPAddr) store.Key {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.BigEndian, localAddr.IP)
	binary.Write(buf, binary.LittleEndian, int16(localAddr.Port))
	return store.Key(sha256.Sum256(buf.Bytes()))
}

func wellKnownPeers() []*net.UDPAddr {
	conf := config.GetConfig()
	myAddr := GetProcessNode().Conn.LocalAddr().(*net.UDPAddr)
	if conf.UseLoopback {
		p, _ := net.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(conf.DefaultLocalhostPort))
		if util.AddrsEqual(myAddr, p) {
			return []*net.UDPAddr{}
		} else {
			return []*net.UDPAddr{p}
		}
	} else {
		knownPeers := make([]*net.UDPAddr, 0, len(conf.PeerList))
		for _, peer := range conf.PeerList {
			peerAddr, _ := net.ResolveUDPAddr("udp", peer)
			if !util.AddrsEqual(myAddr, peerAddr) {
				knownPeers = append(knownPeers, peerAddr)
			}
		}
		return knownPeers
	}
}

func (node *Node) CountOnlinePeers() int {
	count := 0
	for _, peer := range node.KnownPeers {
		if peer.Online {
			count += 1
		}
	}
	return count
}

func (node *Node) RandomPeer() (*Peer, *store.Key) {
	node.Lock.Lock()
	defer node.Lock.Unlock()
	size := node.CountOnlinePeers()
	if size == 0 {
		log.D.Println("No peers online. Looking for well known peers...")
		wellKnown := wellKnownPeers()
		if len(wellKnown) == 0 { // May happen when this node is the only well known one
			return nil, nil
		}
		return &Peer{Addr: wellKnown[util.Rand.Intn(len(wellKnown))]}, nil
	}
	rand := util.Rand.Intn(size)
	i := 0
	for key, peer := range node.KnownPeers {
		if peer.Online {
			if i == rand {
				return peer, &key
			}
			i += 1
		}
	}
	log.E.Println("Failed to get random peer. Concurrency problem or bug!")
	return nil, nil
}

func (node *Node) CleanupKnownNodes() {
	now := time.Now()
	for key, peer := range node.KnownPeers {
		if peer.LastSeen.Add(TimeTillMemberDrop).Before(now) {
			log.I.Printf("Peer %s is expired\n", key.String())
			delete(node.KnownPeers, key)
		}
	}
}

func (node *Node) UpdatePeers(peers map[store.Key]*Peer, sendingPeerId store.Key, sendingAddr *net.UDPAddr) {
	node.Lock.Lock()
	defer node.Lock.Unlock()
	log.I.Println("Updating peers...")
	for key, remotePeerVal := range peers {
		if key != node.ID {
			log.I.Println("updating " + key.String())
			if _, ok := node.KnownPeers[key]; ok {
				if time.Now().Add(timeErr).Before(remotePeerVal.LastSeen) {
					node.KnownPeers[key] = remotePeerVal
				}
			} else {
				node.KnownPeers[key] = remotePeerVal
			}
		} else {
			log.I.Println("ingnoring my id")
		}
	}

	var sendingPeer *Peer
	if peer, ok := node.KnownPeers[sendingPeerId]; ok {
		sendingPeer = peer
	} else {
		sendingPeer = &Peer{}
		node.KnownPeers[sendingPeerId] = sendingPeer
	}
	sendingPeer.LastSeen = time.Now()
	sendingPeer.Online = true
	sendingPeer.Addr = sendingAddr

	node.CleanupKnownNodes()
	log.I.Println("Done.")
}

func (n *Node) SetPeerOffline(peerId store.Key) {
	if peer, ok := node.KnownPeers[peerId]; ok {
		peer.Online = false
	}
}

func PeerListString(peers map[store.Key]*Peer) string {
	s := ""
	for key, peer := range peers {
		s += fmt.Sprintf("	%s: %s, online:%v, lastseen:%s\n",
			key.String(), peer.Addr.String(), peer.Online,
			peer.LastSeen.String())
	}
	return s
}
