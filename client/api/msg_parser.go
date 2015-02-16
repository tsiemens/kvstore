package api

import "github.com/tsiemens/kvstore/shared/api"

var MessageParsers = map[byte]api.MessagePayloadParser{
	api.RespOk:               api.ParseValueDgram,
	api.RespInvalidKey:       api.ParseMessage,
	api.RespOutOfSpace:       api.ParseMessage,
	api.RespSysOverload:      api.ParseMessage,
	api.RespInternalError:    api.ParseMessage,
	api.RespUnknownCommand:   api.ParseMessage,
	api.RespStatusUpdateFail: api.ParseMessage,
	api.RespStatusUpdateOK:   api.ParseMessage,
	api.RespAdhocUpdateOK:    api.ParseMessage,
}
