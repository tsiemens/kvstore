package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

func IntraNodeGet(url string, msg api.Message) api.Message {
	keyMsg := msg.(*api.KeyDgram)
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(msg.UID(), api.CmdIntraGet, keyMsg.Key)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}

func IntraNodePut(url string, msg api.Message, timestamp int) api.Message {
	keyValMsg := msg.(*api.KeyValueDgram)
	storeVal := &store.StoreVal{Val: keyValMsg.Value, Active: true, Timestamp: timestamp}
	payload, jsonerr := json.Marshal(storeVal)
	if jsonerr != nil {
		log.E.Println(jsonerr)
		return nil
	}
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyValueDgram(msg.UID(), api.CmdIntraPut, keyValMsg.Key, payload)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}

func IntraNodeRemove(url string, msg api.Message, timestamp int) api.Message {
	keyMsg := msg.(*api.KeyDgram)
	storeVal := &store.StoreVal{Val: nil, Active: false, Timestamp: timestamp}
	payload, jsonerr := json.Marshal(storeVal)
	if jsonerr != nil {
		log.E.Println(jsonerr)
		return nil
	}
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyValueDgram(msg.UID(), api.CmdIntraRemove, keyMsg.Key, payload)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}

func IntraNodeGetTimestamp(url string, msg api.Message) api.Message {
	var key store.Key
	if msg.Command() == api.CmdPut {
		key = msg.(*api.KeyValueDgram).Key
	} else {
		key = msg.(*api.KeyDgram).Key
	}
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdGetTimestamp, key)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}
