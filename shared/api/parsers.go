package api

var CmdMessageParsers = map[byte]MessagePayloadParser{
	CmdPut:                     ParseKeyValueDgram,
	CmdGet:                     ParseKeyDgram,
	CmdRemove:                  ParseKeyDgram,
	CmdIntraPut:                ParseKeyValueDgram,
	CmdIntraGet:                ParseKeyDgram,
	CmdIntraRemove:             ParseKeyDgram,
	CmdShutdown:                ParseKeyDgram,
	CmdStatusUpdate:            ParseKeyValueDgram,
	CmdAdhocUpdate:             ParseKeyValueDgram,
	CmdMembership:              ParseKeyValueDgram,
	CmdMembershipExchange:      ParseKeyValueDgram,
	CmdMembershipQuery:         ParseBaseDgram,
	CmdMembershipFailure:       ParseKeyValueDgram,
	CmdMembershipFailureGossip: ParseKeyValueDgram,
	CmdStorePush:               ParseValueDgram,
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
	RespInvalidNode:      ParseKeyValueDgram,
	RespTimeout:          ParseBaseDgram,
}
