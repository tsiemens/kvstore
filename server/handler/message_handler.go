package handler

import "net"
import "math/rand"

import "github.com/tsiemens/kvstore/shared/util"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/server/store"

type MessageHandler struct {
	store             *store.Store
	conn              *net.UDPConn
	PacketLossPercent int
	random            *rand.Rand
}

func New(store *store.Store, conn *net.UDPConn, lossPercent int) *MessageHandler {
	random := rand.New(rand.NewSource(util.UnixMilliTimestamp()))
	return &MessageHandler{
		store:             store,
		conn:              conn,
		PacketLossPercent: lossPercent % 101,
		random:            random,
	}
}

func (handler *MessageHandler) Start() (exit chan bool) {
	exit = make(chan bool)
	go api.LoopReceiver(handler.conn, handler, exit)
	return exit
}

func (handler *MessageHandler) isPacketLost() bool {
	return handler.random.Int()%100 < handler.PacketLossPercent
}

func (handler *MessageHandler) HandleClientMessage(msg *api.ClientMessage, recvAddr *net.UDPAddr) {
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
	default:
		log.D.Println("Received unknown command " + string(msg.Command))
		api.ReplyToUnknownCommand(handler.conn, recvAddr, msg)
	}
}

func (handler *MessageHandler) HandleGet(msg *api.ClientMessage, recvAddr *net.UDPAddr) {
	val, err := handler.store.Get(store.Key(msg.Key))
	if err != nil {
		log.E.Println(err)
	}
	api.ReplyToGet(handler.conn, recvAddr, msg, val)
}

func (handler *MessageHandler) HandlePut(msg *api.ClientMessage, recvAddr *net.UDPAddr) {
	err := handler.store.Put(store.Key(msg.Key), msg.Value)
	success := true
	if err != nil {
		success = false
		log.E.Println(err)
	}
	api.ReplyToPut(handler.conn, recvAddr, msg, success)
}

func (handler *MessageHandler) HandleRemove(msg *api.ClientMessage, recvAddr *net.UDPAddr) {
	err := handler.store.Remove(store.Key(msg.Key))
	success := true
	if err != nil {
		success = false
		log.E.Println(err)
	}
	api.ReplyToRemove(handler.conn, recvAddr, msg, success)
}
