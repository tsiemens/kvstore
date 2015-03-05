package handler

import (
	"fmt"
	"github.com/tsiemens/kvstore/server/cache"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/protocol"
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
	GossipKeyMap      map[string]bool
	Cache             *cache.Cache
}

func NewDefaultCmdHandlerSet() map[byte]CmdHandler {
	return map[byte]CmdHandler{
		api.CmdPut:                     HandlePut,
		api.CmdGet:                     HandleGet,
		api.CmdRemove:                  HandleRemove,
		api.CmdShutdown:                HandleShutdown,
		api.CmdIntraPut:                HandlePut,
		api.CmdIntraGet:                HandleGet,
		api.CmdIntraRemove:             HandleRemove,
		api.CmdStatusUpdate:            HandleStatusUpdate,
		api.CmdAdhocUpdate:             HandleAdhocUpdate,
		api.CmdMembership:              HandleMembershipMsg,
		api.CmdMembershipExchange:      HandleMembershipMsgExchange,
		api.CmdMembershipQuery:         HandleMembershipQuery,
		api.CmdMembershipFailure:       HandleMembershipMsg,
		api.CmdMembershipFailureGossip: HandleMembershipFailureGossip,
		api.CmdStorePush:               HandleStorePush,
	}
}

func (handler *MessageHandler) IsNewMessage(key [16]byte) bool {
	if _, ok := handler.GossipKeyMap[string(key[:])]; ok {
		return false
	}
	return true
}

func (handler *MessageHandler) ShouldGossip(key [16]byte) bool {
	conf := config.GetConfig()
	if shouldGossip, ok := handler.GossipKeyMap[string(key[:])]; ok {
		if shouldGossip {
			if util.Rand.Intn(conf.K) == conf.K-1 {
				handler.GossipKeyMap[string(key[:])] = false
				return false
			} else {
				return true
			}
		} else {
			return false
		}
	} else {
		handler.GossipKeyMap[string(key[:])] = true
		return true
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
		GossipKeyMap:      make(map[string]bool, 0),
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
