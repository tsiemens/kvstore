package handler

import "net"
import "math/rand"

import "github.com/tsiemens/kvstore/shared/util"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/exec"
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

func NewMessageHandler(store *store.Store, conn *net.UDPConn, lossPercent int) *MessageHandler {
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
	case api.CmdAdhocUpdate:
		handler.HandleAdhocUpdate(msg, recvAddr)
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
		dataDelimiter := "\t\n\t\n"
		// TODO handle failures properly
		// Commented out all the identifiers because it was easier to create the html
		// that way
		success, deploymentSpace := exec.GetDeploymentDiskSpace()
		//deploymentSpace = "Application Size:\n" + deploymentSpace 
		success, diskSpace := exec.GetDiskSpace()
		//diskSpace = "Disk space:\n" + diskSpace
		success, uptime := exec.Uptime()
		//uptime = "Uptime:\n" + uptime
		success, currentload := exec.CurrentLoad()
		//currentload = "Current load:\n" + currentload
		api.ReplyToStatusUpdateServer(handler.conn, conf.StatusServerAddr, msg, []byte(deploymentSpace+dataDelimiter+diskSpace+dataDelimiter+uptime+dataDelimiter+currentload), success)
	}

	if handler.shouldGossip {
		api.Gossip(handler.conn, msg)
	}
}

func (handler *MessageHandler) HandleAdhocUpdate(msg *api.RequestMessage, recvAddr *net.UDPAddr) {
	log.D.Println("Adhoc Update handle called")
	conf := config.GetConfig()
	log.I.Println(msg.Key)
	log.I.Println(handler.statusKey)
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
		success, status := exec.RunCommand(string(msg.Value))
		api.ReplyToStatusUpdateServer(handler.conn, conf.StatusServerAddr, msg, []byte(status), success)
	}

	if handler.shouldGossip {
		api.Gossip(handler.conn, msg)
	}
}
