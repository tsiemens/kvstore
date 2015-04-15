package api

var CmdMessageParsers = map[byte]MessagePayloadParser{
	CmdPut:                     ParseKeyValueDgram,
	CmdGet:                     ParseKeyDgram,
	CmdRemove:                  ParseKeyDgram,
	CmdIntraWrite:              ParseKeyValueDgram,
	CmdIntraGet:                ParseKeyDgram,
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
	RespOk:                  ParseValueDgram,
	RespInvalidKey:          ParseBaseDgram,
	RespOutOfSpace:          ParseBaseDgram,
	RespSysOverload:         ParseBaseDgram,
	RespInternalError:       ParseBaseDgram,
	RespClientInternalError: ParseBaseDgram,
	RespUnknownCommand:      ParseBaseDgram,
	RespStatusUpdateFail:    ParseBaseDgram,
	RespStatusUpdateOK:      ParseKeyValueDgram,
	RespAdhocUpdateOK:       ParseKeyValueDgram,
	RespInvalidNode:         ParseKeyValueDgram,
	RespTimeout:             ParseBaseDgram,
	RespOkTimestamp:         ParseValueDgram,
}
