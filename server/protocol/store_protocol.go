package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/cache"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

type kvMap struct {
	M map[string][]byte
}

func NewKVMap(kvs map[store.Key][]byte) *kvMap {
	keyStringMap := map[string][]byte{}
	for key, value := range kvs {
		keyStringMap[api.KeyHex(key)] = value
	}
	return &kvMap{keyStringMap}
}

func (kvmap *kvMap) KeyValues() map[store.Key][]byte {
	keyValMap := map[store.Key][]byte{}
	for keyString, value := range kvmap.M {
		key, err := api.KeyFromHex(keyString)
		if err == nil {
			keyValMap[store.Key(key)] = value
		} else {
			log.E.Println(err)
		}
	}
	return keyValMap
}

// Sends a sendRecv message with a range of key values to a node
// Returns error if the node times out
func SendStorePushMsg(conn *net.UDPConn, addr *net.UDPAddr, values map[store.Key][]byte) error {

	valuesWrapper := NewKVMap(values)
	kvdata, err := json.Marshal(valuesWrapper)
	if err != nil {
		log.E.Panicln("Could not marshal key values")
	}

	msgGenerator := func(addr *net.UDPAddr) api.Message {
		return api.NewValueDgram(api.NewMessageUID(addr), api.CmdStorePush, kvdata)
	}

	_, err = api.SendRecv(addr.String(), msgGenerator)
	return err
}

func ParseStorePushMsgValue(data []byte) (map[store.Key][]byte, error) {
	values := &kvMap{}
	err := json.Unmarshal(data, values)
	if err != nil {
		return nil, err
	} else {
		return values.KeyValues(), nil
	}
}

// Hack to avoid import cycles
func SendKeyValuesToNode(peerKey store.Key, values map[store.Key][]byte) {
	n := node.GetProcessNode()
	if peer, ok := n.KnownPeers[peerKey]; ok {
		err := SendStorePushMsg(n.Conn, peer.Addr, values)
		if err != nil {
			peer.Online = false
			log.D.Printf("Failed to copy keys to %s\n", peerKey.String())
		} else {
			log.I.Printf("Copied portion of keys to %s\n", peerKey.String())
		}
	}
}

func ReplyToStorePush(conn *net.UDPConn, recvAddr *net.UDPAddr,
	cache *cache.Cache, requestMsg api.Message) {
	reply := api.NewValueDgram(requestMsg.UID(), api.RespOk, []byte{})
	cache.SendReply(conn, reply, recvAddr)
}
