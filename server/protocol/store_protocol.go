package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/cache"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

type kvMap struct {
	M map[store.Key][]byte
}

// Sends a sendRecv message with a range of key values to a node
// Returns error if the node times out
func SendStorePushMsg(conn *net.UDPConn, addr *net.UDPAddr, values map[store.Key][]byte) error {

	kvdata, err := json.Marshal(&kvMap{values})
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
		return values.M, nil
	}
}

func ReplyToStorePush(conn *net.UDPConn, recvAddr *net.UDPAddr,
	cache *cache.Cache, requestMsg api.Message) {
	reply := api.NewValueDgram(requestMsg.UID(), api.RespOk, []byte{})
	cache.SendReply(conn, reply, recvAddr)
}
