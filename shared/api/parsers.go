package api

var CmdMessageParsers = map[byte]MessagePayloadParser{
	CmdPut:                ParseKeyValueDgram,
	CmdGet:                ParseKeyDgram,
	CmdRemove:             ParseKeyDgram,
	CmdStatusUpdate:       ParseKeyValueDgram,
	CmdAdhocUpdate:        ParseKeyValueDgram,
	CmdMembership:         ParseKeyValueDgram,
	CmdMembershipResponse: ParseKeyValueDgram,
	CmdMembershipQuery:    ParseBaseDgram,
}

var RespMessageParsers = map[byte]MessagePayloadParser{
	RespOk:               ParseValueDgram,
	RespInvalidKey:       ParseBaseDgram,
	RespOutOfSpace:       ParseBaseDgram,
	RespSysOverload:      ParseBaseDgram,
	RespInternalError:    ParseBaseDgram,
	RespUnknownCommand:   ParseBaseDgram,
	RespStatusUpdateFail: ParseBaseDgram,
	RespStatusUpdateOK:   ParseKeyValueDgram,
	RespAdhocUpdateOK:    ParseKeyValueDgram,
}
