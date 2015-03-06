package protocol

import (
	"github.com/tsiemens/kvstore/shared/api"
	"net"
)

func IntraNodeGet(url string, key [32]byte) ([]byte, byte) {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdGet, key)
	})
	if err != nil {
		return nil, api.RespTimeout
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return nil, msg.Command()
	} else if vmsg, ok := msg.(*api.ValueDgram); ok {
		return vmsg.Value, api.RespOk
	} else {
		return nil, api.RespInternalError
	}
}

func IntraNodePut(url string, key [32]byte, value []byte) byte {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyValueDgram(api.NewMessageUID(addr), api.CmdPut, key, value)
	})
	if err != nil {
		return api.RespTimeout
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return msg.Command()
	} else {
		return api.RespOk
	}
}

func IntraNodeRemove(url string, key [32]byte) byte {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdRemove, key)
	})
	if err != nil {
		return api.RespTimeout
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return msg.Command()
	} else {
		return api.RespOk
	}
}
