package handler

import (
	"net"

	"encoding/json"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/protocol"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/exec"
	"github.com/tsiemens/kvstore/shared/log"
)

func HandleGet(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	val, err := node.GetProcessNode().Store.Get(store.Key(keyMsg.Key))
	if err != nil {
		log.E.Println(err)
	}
	protocol.ReplyToGet(handler.Conn, recvAddr, handler.Cache, msg, val)
}

func HandlePut(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyValMsg := msg.(*api.KeyValueDgram)
	err := node.GetProcessNode().Store.Put(
		store.Key(keyValMsg.Key),
		keyValMsg.Value)
	success := true
	if err != nil {
		success = false
		log.E.Println(err)
	}
	protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, msg, success)
}

func HandleRemove(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	err := node.GetProcessNode().Store.Remove(store.Key(keyMsg.Key))
	success := true
	if err != nil {
		success = false
		log.E.Println(err)
	}
	protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, msg, success)
}

func HandleStatusUpdate(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	conf := config.GetConfig()
	log.D.Println("Status Update handle called")
	keyValMsg := msg.(*api.KeyValueDgram)
	if handler.IsNewMessage(keyValMsg.Key) {
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
		protocol.ReplyToStatusUpdateServer(handler.Conn, conf.StatusServerAddr, handler.Cache, msg, []byte(deploymentSpace+dataDelimiter+diskSpace+dataDelimiter+uptime+dataDelimiter+currentload), success)
	}

	if handler.ShouldGossip(keyValMsg.Key) {
		protocol.Gossip(handler.Conn, keyValMsg)
	}
}

func HandleAdhocUpdate(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	conf := config.GetConfig()
	keyValMsg := msg.(*api.KeyValueDgram)
	if handler.IsNewMessage(keyValMsg.Key) {
		success, status := exec.RunCommand(string(keyValMsg.Value))
		protocol.ReplyToStatusUpdateServer(handler.Conn, conf.StatusServerAddr, handler.Cache, msg, []byte(status), success)
	}

	if handler.ShouldGossip(keyValMsg.Key) {
		protocol.Gossip(handler.Conn, keyValMsg)
	}
}

func HandleMembershipMsgExchange(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleMembership(&msg, recvAddr)
	thisNode := node.GetProcessNode()
	protocol.SendMembershipMsg(handler.Conn, recvAddr,
		thisNode.ID, thisNode.KnownPeers, api.CmdMembership)
}

func HandleMembershipMsg(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleMembership(&msg, recvAddr)
}

func HandleMembershipFailureGossip(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleMembership(&msg, recvAddr)
	if handler.ShouldGossip(msg.(*api.KeyValueDgram).Key) {
		protocol.Gossip(handler.Conn, msg.(*api.KeyValueDgram))
	}
}

func HandleMembershipQuery(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	err := protocol.ReplyToMembershipQuery(handler.Conn, recvAddr, handler.Cache,
		msg, node.GetProcessNode().ID, node.GetProcessNode().KnownPeers)
	if err != nil {
		log.E.Println(err)
	}
}

func handleMembership(msg *api.Message, recvAddr *net.UDPAddr) {
	if keyValMsg, ok := (*msg).(*api.KeyValueDgram); ok {
		nodeId := keyValMsg.Key
		peers := &protocol.PeerList{}
		err := json.Unmarshal(keyValMsg.Value, peers)
		if err != nil {
			log.E.Println(err)
		} else {
			thisNode := node.GetProcessNode()
			thisNode.UpdatePeers(peers.PointerMap(), nodeId, recvAddr)
			//log.D.Printf("Currently known peers: [\n%s\n]\n",
			//	node.PeerListString(thisNode.KnownPeers))
		}
	} else {
		log.E.Println("Received invalid membership datagram")
	}
}

func HandleShutdown(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	log.I.Fatal("Shutdown Command recieved, aborting program")
}
