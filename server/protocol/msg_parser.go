package protocol

import "github.com/tsiemens/kvstore/shared/api"

var MessageParsers = map[byte]api.MessagePayloadParser{
	api.CmdPut:          api.ParseKeyValueDgram,
	api.CmdGet:          api.ParseKeyDgram,
	api.CmdRemove:       api.ParseKeyDgram,
	api.CmdStatusUpdate: api.ParseKeyValueDgram,
	api.CmdAdhocUpdate:  api.ParseKeyValueDgram,
	api.CmdMembership:   api.ParseKeyValueDgram,
}
