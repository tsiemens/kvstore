package handler

import "net"
import "math/rand"

import "github.com/tsiemens/kvstore/shared/util"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/server/store"
import "github.com/tsiemens/kvstore/server/config"

type MessageHandler struct {
	store             *store.Store
	conn              *net.UDPConn
	PacketLossPercent int
	random            *rand.Rand
	statusKey         store.Key
	shouldGossip      bool
}

func New(store *store.Store, conn *net.UDPConn, lossPercent int) *MessageHandler {
	random := rand.New(rand.NewSource(util.UnixMilliTimestamp()))
	return &MessageHandler{
		store:             store,
		conn:              conn,
		PacketLossPercent: lossPercent % 101,
		random:            random,
		shouldGossip:      true,
	}
}

func (handler *MessageHandler) isPacketLost() bool {
	return handler.random.Int()%100 < handler.PacketLossPercent
}

func (handler *MessageHandler) HandleRequestMessage(msg *api.RequestMessage, recvAddr *net.UDPAddr) {
	if handler.PacketLossPercent != 0 && handler.isPacketLost() {
		log.D.Println("Opps! Packet dropped!")
		return
	}
	log.D.Println("Handling!")
	switch msg.Command {
	case api.CmdGet:
		handler.HandleGet(msg, recvAddr)
	case api.CmdPut:
		handler.HandlePut(msg, recvAddr)
	case api.CmdRemove:
		handler.HandleRemove(msg, recvAddr)
	case api.CmdStatusUpdate:
		handler.HandleStatusUpdate(msg, recvAddr)
	default:
		log.D.Println("Received unknown command " + string(msg.Command))
		api.ReplyToUnknownCommand(handler.conn, recvAddr, msg)
	}
}

func (handler *MessageHandler) HandleGet(msg *api.RequestMessage, recvAddr *net.UDPAddr) {
	val, err := handler.store.Get(store.Key(msg.Key))
	if err != nil {
		log.E.Println(err)
	}
	api.ReplyToGet(handler.conn, recvAddr, msg, val)
}

func (handler *MessageHandler) HandlePut(msg *api.RequestMessage, recvAddr *net.UDPAddr) {
	err := handler.store.Put(store.Key(msg.Key), msg.Value)
	success := true
	if err != nil {
		success = false
		log.E.Println(err)
	}
	api.ReplyToPut(handler.conn, recvAddr, msg, success)
}

func (handler *MessageHandler) HandleRemove(msg *api.RequestMessage, recvAddr *net.UDPAddr) {
	err := handler.store.Remove(store.Key(msg.Key))
	success := true
	if err != nil {
		success = false
		log.E.Println(err)
	}
	api.ReplyToRemove(handler.conn, recvAddr, msg, success)
}

func (handler *MessageHandler) HandleStatusUpdate(msg *api.RequestMessage, recvAddr *net.UDPAddr) {
	log.D.Println("Status Update handle called")
	conf := config.GetConfig()
	if handler.statusKey.Equals(msg.Key) {
		// status already reached node
		log.D.Println("Status already received")
		if handler.random.Intn(conf.K) == conf.K-1 {
			handler.shouldGossip = false
			return
		}
	} else {
		handler.shouldGossip = true
		handler.statusKey = msg.Key
		status, success := handler.ExecuteStatusUpdate(msg)
		api.ReplyToStatusUpdateServer(handler.conn, conf.StatusServerAddr, msg, status, success)
	}

	if handler.shouldGossip {
		api.Gossip(handler.conn, msg)
	}
}

func (handler *MessageHandler) ExecuteStatusUpdate(msg *api.RequestMessage) ([]byte, bool) {
	//TODO - implement
	// placeholder
	status := make([]byte, 4)
	status = []byte("hell")
	return status, true

}
