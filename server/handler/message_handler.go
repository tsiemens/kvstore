package handler

import (
	"fmt"
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
	store             *store.Store
	Conn              *net.UDPConn
	cmdHandlers       map[byte]CmdHandler
	PacketLossPercent int
	statusKey         store.Key
	shouldGossip      bool
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
	}
}

func NewDefaultMessageHandler(store *store.Store, conn *net.UDPConn, lossPercent int) *MessageHandler {
	return NewMessageHandler(store, conn,
		NewDefaultCmdHandlerSet(), lossPercent)
}

func NewMessageHandler(store *store.Store, conn *net.UDPConn,
	cmdHandlers map[byte]CmdHandler, lossPercent int) *MessageHandler {
	return &MessageHandler{
		store:             store,
		Conn:              conn,
		cmdHandlers:       cmdHandlers,
		PacketLossPercent: lossPercent % 101,
		shouldGossip:      true,
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
	log.D.Println("Handling!")
	if cmdHandler, ok := handler.cmdHandlers[msg.Command()]; ok {
		cmdHandler(handler, msg, recvAddr)
	} else {
		log.D.Println(fmt.Sprintf("No handler for command 0x%x", msg.Command()))
		protocol.ReplyToUnknownCommand(handler.Conn, recvAddr, msg)
	}
}
