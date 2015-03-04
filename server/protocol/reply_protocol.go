package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"net"
	"time"
)

func ReplyToGet(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, value []byte) {
	var reply api.Message
	if value != nil {
		reply = api.NewValueDgram(requestMsg.UID(), api.RespOk, value)
	} else {
		reply = api.NewBaseDgram(requestMsg.UID(), api.RespInvalidKey)
	}
	conn.WriteTo(reply.Bytes(), recvAddr)
}

func ReplyToPut(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, success bool) {
	var reply api.Message
	if success {
		reply = api.NewValueDgram(requestMsg.UID(), api.RespOk, make([]byte, 0, 0))
	} else {
		reply = api.NewBaseDgram(requestMsg.UID(), api.RespInvalidKey)
	}
	conn.WriteTo(reply.Bytes(), recvAddr)
}

func ReplyToRemove(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, success bool) {
	var reply api.Message
	if success {
		reply = api.NewValueDgram(requestMsg.UID(), api.RespOk, make([]byte, 0, 0))
	} else {
		reply = api.NewBaseDgram(requestMsg.UID(), api.RespInvalidKey)
	}
	conn.WriteTo(reply.Bytes(), recvAddr)
}

func ReplyToStatusUpdateServer(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, statusResult []byte, success bool) {
	var respCode byte

	if !success {
		respCode = api.RespStatusUpdateFail
		conn.WriteTo(api.NewValueDgram(requestMsg.UID(), respCode,
			statusResult).Bytes(),
			recvAddr)
		return
	}

	switch cmd := requestMsg.Command(); cmd {
	case api.CmdStatusUpdate:
		respCode = api.RespStatusUpdateOK
	case api.CmdAdhocUpdate:
		respCode = api.RespAdhocUpdateOK
	default:
		respCode = api.RespOk
	}

	//TODO - figure out what to do with UID
	conn.WriteTo(api.NewValueDgram(requestMsg.UID(), respCode,
		statusResult).Bytes(),
		recvAddr)
}

func ReplyToMembershipQuery(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message, myNodeId [32]byte,
	peers map[store.Key]*node.Peer) error {

	peerList := NewPeerList(peers)
	// Append this node to list
	peerList.Peers[api.KeyHex(store.Key(myNodeId))] = node.Peer{
		Online:   true,
		Addr:     conn.LocalAddr().(*net.UDPAddr),
		LastSeen: time.Now(),
	}

	peerdata, err := json.Marshal(peerList)
	if err != nil {
		return err
	}
	conn.WriteTo(
		api.NewValueDgram(requestMsg.UID(), api.RespOk, peerdata).Bytes(),
		recvAddr)
	return nil
}

func NotifyStatusUpdate(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message) {
}

func ReplyToUnknownCommand(conn *net.UDPConn, recvAddr *net.UDPAddr,
	requestMsg api.Message) {
	conn.WriteTo(api.NewBaseDgram(requestMsg.UID(),
		api.RespUnknownCommand).Bytes(),
		recvAddr)
}

func Debug_ReplyWithBadUID(conn *net.UDPConn, recvAddr *net.UDPAddr) {
	conn.WriteTo(api.NewBaseDgram([16]byte{}, api.RespOk).Bytes(),
		recvAddr)
}
