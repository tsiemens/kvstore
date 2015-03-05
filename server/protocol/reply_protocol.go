package protocol

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/cache"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"net"
	"time"
)

func ReplyToGet(conn *net.UDPConn, recvAddr *net.UDPAddr, replyMsg api.Message) {
	conn.WriteTo(replyMsg.Bytes(), recvAddr)
}

func ReplyToPut(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache,
	requestMsg api.Message, success bool) {
	var reply api.Message
	if success {
		reply = api.NewValueDgram(requestMsg.UID(), api.RespOk, make([]byte, 0, 0))
	} else {
		reply = api.NewBaseDgram(requestMsg.UID(), api.RespInvalidKey)
	}
	cache.SendReply(conn, reply, recvAddr)
}

func ReplyToRemove(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache,
	requestMsg api.Message, success bool) {
	var reply api.Message
	if success {
		reply = api.NewValueDgram(requestMsg.UID(), api.RespOk, make([]byte, 0, 0))
	} else {
		reply = api.NewBaseDgram(requestMsg.UID(), api.RespInvalidKey)
	}
	cache.SendReply(conn, reply, recvAddr)
}

func ReplyToStatusUpdateServer(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache,
	requestMsg api.Message, statusResult []byte, success bool) {
	var respCode byte

	if !success {
		respCode = api.RespStatusUpdateFail
		cache.SendReply(conn, api.NewValueDgram(requestMsg.UID(), respCode,
			statusResult),
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

	cache.SendReply(conn, api.NewValueDgram(requestMsg.UID(), respCode,
		statusResult),
		recvAddr)
}

func ReplyToMembershipQuery(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache,
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
	cache.SendReply(
		conn,
		api.NewValueDgram(requestMsg.UID(), api.RespOk, peerdata),
		recvAddr)
	return nil
}

func NotifyStatusUpdate(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache,
	requestMsg api.Message) {
}

func ReplyToUnknownCommand(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache,
	requestMsg api.Message) {
	cache.SendReply(conn, api.NewBaseDgram(requestMsg.UID(),
		api.RespUnknownCommand),
		recvAddr)
}

func Debug_ReplyWithBadUID(conn *net.UDPConn, recvAddr *net.UDPAddr, cache *cache.Cache) {
	cache.SendReply(conn, api.NewBaseDgram([16]byte{}, api.RespOk),
		recvAddr)
}
