package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

func IntraNodeGet(url string, key *store.Key) api.Message {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdIntraGet, *key)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}

// Sends a write message to another node specified by url.
// Set timestamp to 0, to get the remote node to auto increment its timestamp
func IntraNodeWrite(url string, key *store.Key, value []byte, active bool, timestamp int) api.Message {
	storeVal := &store.StoreVal{Val: value, Active: active, Timestamp: timestamp}
	payload, jsonerr := json.Marshal(storeVal)
	if jsonerr != nil {
		log.E.Println(jsonerr)
		return nil
	}
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyValueDgram(api.NewMessageUID(addr), api.CmdIntraWrite, *key, payload)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}
