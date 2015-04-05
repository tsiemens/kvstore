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
	"sort"
	"strconv"
	"time"
)

const timeErr = time.Millisecond * 50
const TimeTillMemberDrop = time.Minute * 1

// Node represents this machine, as one in a cluster of nodes.
type Node struct {
	ID                  store.Key // Not needed just yet, but it will later
	KnownPeers          map[store.Key]*Peer
	NodeKeyList         []store.Key
	Lock                util.Semaphore
	Conn                *net.UDPConn
	Store               *store.Store
	sendKeyValuesToNode KeyValueMigrator
}

type Peer struct {
	Online   bool
	LastSeen time.Time
	Addr     *net.UDPAddr
}

var node *Node

func Init(localAddr *net.UDPAddr, conn *net.UDPConn, procStore *store.Store,
	sendKVs KeyValueMigrator) {
	node = &Node{
		ID:                  createNodeID(localAddr),
		KnownPeers:          map[store.Key]*Peer{},
		NodeKeyList:         []store.Key{},
		Lock:                util.NewSemaphore(),
		Conn:                conn,
		Store:               procStore,
		sendKeyValuesToNode: sendKVs,
	}
	node.UpdateSortedKeys()
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
	var myAddr *net.UDPAddr
	if GetProcessNode() != nil {
		myAddr = GetProcessNode().Conn.LocalAddr().(*net.UDPAddr)
	}
	if conf.UseLoopback {
		p, _ := net.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(conf.DefaultLocalhostPort))
		if myAddr != nil && util.AddrsEqual(myAddr, p) {
			return []*net.UDPAddr{}
		} else {
			return []*net.UDPAddr{p}
		}
	} else {
		knownPeers := make([]*net.UDPAddr, 0, len(conf.PeerList))
		for _, peer := range conf.PeerList {
			peerAddr, _ := net.ResolveUDPAddr("udp", peer)
			if myAddr == nil || !util.AddrsEqual(myAddr, peerAddr) {
				knownPeers = append(knownPeers, peerAddr)
			}
		}
		return knownPeers
	}
}

func RandomWellKnownPeer() *Peer {
	wellKnown := wellKnownPeers()
	if len(wellKnown) == 0 { // May happen when this node is the only well known one
		return nil
	}
	return &Peer{Addr: wellKnown[util.Rand.Intn(len(wellKnown))]}
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
		return RandomWellKnownPeer(), nil
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

func (node *Node) UpdateSortedKeys() {
	node.NodeKeyList = make([]store.Key, 0, len(node.KnownPeers)+1)
	for k, peer := range node.KnownPeers {
		if peer.Online {
			node.NodeKeyList = append(node.NodeKeyList, k)
		}
	}
	node.NodeKeyList = append(node.NodeKeyList, node.ID)
	sort.Sort(store.Keys(node.NodeKeyList))
}

func (node *Node) UpdatePeers(peers map[store.Key]*Peer, sendingPeerId store.Key, sendingAddr *net.UDPAddr) {
	node.Lock.Lock()
	defer node.Lock.Unlock()
	newOnlineNodes := make([]store.Key, 1)
	oldLowerBoundKey := node.GetNextLowestPeerKey()
	log.D.Println("Updating peers...")
	for key, remotePeerVal := range peers {
		if key != node.ID {
			if node.updateKnownPeer(key, remotePeerVal) {
				// Node is new or used to be offline and is now online
				newOnlineNodes = append(newOnlineNodes, key)
			}
		}
	}

	// Using loopback, it's possible for a node to send a
	// gossip membership message to itself
	if sendingPeerId == node.ID {
		return
	}

	var sendingPeer *Peer
	if peer, ok := node.KnownPeers[sendingPeerId]; ok {
		sendingPeer = peer
	} else {
		sendingPeer = &Peer{}
		node.KnownPeers[sendingPeerId] = sendingPeer
		sendingPeer.Addr = sendingAddr
		newOnlineNodes = append(newOnlineNodes, sendingPeerId)
	}
	sendingPeer.LastSeen = time.Now()
	sendingPeer.Online = true

	node.CleanupKnownNodes()
	node.UpdateSortedKeys()
	node.handleNewPeersOnline(newOnlineNodes, oldLowerBoundKey)
	log.D.Println("Done.")
}

// Updates the peer for key in KnownPeers with the newly received remotePeerVal
// Returns true if the node has just come online
func (node *Node) updateKnownPeer(key store.Key, remotePeerVal *Peer) bool {
	isNewlyOnline := false
	log.D.Println("updating " + key.String())
	if _, ok := node.KnownPeers[key]; ok {
		if time.Now().Add(timeErr).After(remotePeerVal.LastSeen) {
			peerVal := node.KnownPeers[key]
			if !peerVal.Online && remotePeerVal.Online {
				isNewlyOnline = true
			}
			peerVal.Online = remotePeerVal.Online
			if peerVal.LastSeen.Before(remotePeerVal.LastSeen) {
				peerVal.LastSeen = remotePeerVal.LastSeen
			}
		}
	} else {
		node.KnownPeers[key] = remotePeerVal
		if remotePeerVal.Online {
			isNewlyOnline = true
		}
	}
	return isNewlyOnline
}

// Handles the case when a peer was previously not known, or is now online
// If a value transfer is required, spawns new goroutines to copy it.
func (n *Node) handleNewPeersOnline(peerIds []store.Key,
	oldLowerBound store.Key) {

	storeKeys := n.Store.GetSortedKeys()
	for _, newPeerKey := range peerIds {
		if (&newPeerKey).Between(oldLowerBound, n.ID) {
			nodesKeys := n.GetAllKeysForNode(newPeerKey, storeKeys)
			// send all keys in this range
			values := make(map[store.Key]*store.StoreVal, len(nodesKeys))
			for _, key := range nodesKeys {
				val, err := n.Store.Get(key)
				if err == nil {
					values[key] = val
				}
			}
			go n.sendKeyValuesToNode(newPeerKey, values)
		}
	}
}

func (n *Node) GetAllKeysForNode(peerKey store.Key,
	sortedStoreKeys []store.Key) []store.Key {

	nextLowest := n.GetNextLowestPeerKeyFrom(peerKey)
	// probabalistic size for efficiency
	keys := make([]store.Key, int(float32(len(sortedStoreKeys))*0.6))
	for _, storeKey := range sortedStoreKeys {
		if storeKey.Between(nextLowest, peerKey) {
			keys = append(keys, storeKey)
		}
	}
	return keys
}

func (n *Node) GetNextLowestPeerKey() store.Key {
	return n.GetNextLowestPeerKeyFrom(n.ID)
}

func (n *Node) GetNextLowestPeerKeyFrom(key store.Key) store.Key {
	for i, nodekey := range n.NodeKeyList {
		if nodekey == key {
			return n.NodeKeyList[n.getPredecessorIndexOfNodeAtIndex(i)]
		}
	}
	return key
}

func (n *Node) getPredecessorIndexOfNodeAtIndex(index int) int {
	if index == 0 {
		return len(n.NodeKeyList) - 1
	} else {
		return index
	}
}

func (n *Node) GetReplicaIdsForKey(key store.Key) []store.Key {
	headKeyPtr, _ := n.GetPeerResponsibleForKey(key)
	var headKeyIndex int
	for i, nodeKey := range n.NodeKeyList {
		if nodeKey == *headKeyPtr {
			headKeyIndex = i
			break
		}
	}
	maxReplicas := config.GetConfig().MaxReplicas
	keys := make([]store.Key, 0, maxReplicas)
	keys = append(keys, *headKeyPtr)
	nextReplicaIndex := n.getPredecessorIndexOfNodeAtIndex(headKeyIndex)
	for len(keys) < maxReplicas && n.NodeKeyList[nextReplicaIndex] != *headKeyPtr {
		keys = append(keys, n.NodeKeyList[nextReplicaIndex])
		nextReplicaIndex = n.getPredecessorIndexOfNodeAtIndex(nextReplicaIndex)
	}
	return keys
}

// This is really irritating that we need this because of IMPORT CYCLES
type KeyValueMigrator func(peerKey store.Key, values map[store.Key]*store.StoreVal)

func (n *Node) SetPeerOffline(peerId store.Key) {
	if peer, ok := node.KnownPeers[peerId]; ok {
		peer.Online = false
	}
}

/* Returns the peer that should handle the given key.
 * Returns nil peer if this node is responsible.
 * A peer is responsible if it is the next higher or equal to the key
 */
func (n *Node) GetPeerResponsibleForKey(key store.Key) (*store.Key, *Peer) {
	responsibleKey := n.NodeKeyList[0]
	for _, nodekey := range n.NodeKeyList {
		if nodekey.GreaterEquals(key) {
			if nodekey == n.ID {
				responsibleKey = nodekey
				break
			}
			if node, ok := n.KnownPeers[nodekey]; ok && node.Online {
				responsibleKey = nodekey
				break
			}
		}
	}
	if responsibleKey == n.ID {
		return &responsibleKey, nil
	} else {
		return &responsibleKey, n.KnownPeers[responsibleKey]
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
