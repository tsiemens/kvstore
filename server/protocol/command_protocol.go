package protocol

import (
	"github.com/tsiemens/kvstore/shared/api"
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

func IntraNodePut(url string, msg api.Message) api.Message {
	keyValMsg := msg.(*api.KeyValueDgram)
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyValueDgram(msg.UID(), api.CmdIntraPut, keyValMsg.Key, keyValMsg.Value)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}

func IntraNodeRemove(url string, msg api.Message) api.Message {
	keyMsg := msg.(*api.KeyDgram)
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(msg.UID(), api.CmdIntraRemove, keyMsg.Key)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}

func IntraNodeGetTimestamp(url string, msg api.Message) api.Message {
	keyValueMsg := msg.(*api.KeyValueDgram)
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdGetTimestamp, keyValueMsg.Key)
	})
	if err != nil {
		return nil
	} else {
		return msg
	}
}
