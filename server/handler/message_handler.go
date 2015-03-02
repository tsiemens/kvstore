package handler

import (
	"fmt"
	"github.com/tsiemens/kvstore/server/cache"
	"github.com/tsiemens/kvstore/server/protocol"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"net"
)

type CmdHandler func(mh *MessageHandler, msg api.Message,
	recvAddr *net.UDPAddr)

// Implements protocol.MessageHandler (to avoid import loops)
type MessageHandler struct {
	Conn              *net.UDPConn
	cmdHandlers       map[byte]CmdHandler
	PacketLossPercent int
	statusKey         store.Key
	shouldGossip      bool
	Cache             *cache.Cache
}

func NewDefaultCmdHandlerSet() map[byte]CmdHandler {
	return map[byte]CmdHandler{
		api.CmdPut:                HandlePut,
		api.CmdGet:                HandleGet,
		api.CmdRemove:             HandleRemove,
		api.CmdStatusUpdate:       HandleStatusUpdate,
		api.CmdAdhocUpdate:        HandleAdhocUpdate,
		api.CmdMembership:         HandleMembershipMsg,
		api.CmdMembershipResponse: HandleMembershipResponse,
		api.CmdMembershipQuery:    HandleMembershipQuery,
		api.CmdShutdown:           HandleShutdown,
	}
}

func NewDefaultMessageHandler(conn *net.UDPConn, lossPercent int) *MessageHandler {
	return NewMessageHandler(conn,
		NewDefaultCmdHandlerSet(), lossPercent)
}

func NewMessageHandler(conn *net.UDPConn,
	cmdHandlers map[byte]CmdHandler, lossPercent int) *MessageHandler {
	return &MessageHandler{
		Conn:              conn,
		cmdHandlers:       cmdHandlers,
		PacketLossPercent: lossPercent % 101,
		shouldGossip:      true,
		Cache:             cache.New(),
	}
}

func (handler *MessageHandler) isPacketLost() bool {
	return util.Rand.Int()%100 < handler.PacketLossPercent
}

func (handler *MessageHandler) HandleMessage(msg api.Message, recvAddr *net.UDPAddr) {
	if handler.PacketLossPercent != 0 && handler.isPacketLost() {
		log.D.Println("Opps! Packet dropped!")
		return
	}

	if wasCached, cachedReply := handler.Cache.StoreAndGetReply(msg); wasCached {
		log.D.Println("Cached message received")
		if cachedReply != nil {
			log.D.Println("Replying with cached reply")
			handler.Conn.WriteTo(cachedReply.Bytes(), recvAddr)
		}
		return
	}

	log.D.Println("Handling!")
	if cmdHandler, ok := handler.cmdHandlers[msg.Command()]; ok {
		cmdHandler(handler, msg, recvAddr)
	} else {
		log.D.Println(fmt.Sprintf("No handler for command 0x%x", msg.Command()))
		protocol.ReplyToUnknownCommand(handler.Conn, recvAddr, handler.Cache, msg)
	}
}
