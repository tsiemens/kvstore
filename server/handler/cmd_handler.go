package handler

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/protocol"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/exec"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

func HandleGet(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	ownerId, owner := node.GetProcessNode().GetPeerResponsibleForKey(keyMsg.Key)
	var replyMsg api.Message
	log.I.Printf("OwnerId is %s\n", ownerId.String())
	log.I.Printf("My Id is %s\n", node.GetProcessNode().ID.String())

	if *ownerId == node.GetProcessNode().ID {
		log.D.Printf("Getting value with key %v\n", keyMsg.Key)
		value, err := node.GetProcessNode().Store.Get(store.Key(keyMsg.Key))
		if err != nil {
			log.E.Println(err)
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, value)
		}
	} else {
		value, respCode := protocol.IntraNodeGet(owner.Addr.String(), keyMsg.Key)
		if respCode == api.RespOk {
			replyMsg = api.NewValueDgram(msg.UID(), respCode, value)
		} else {
			replyMsg = api.NewBaseDgram(msg.UID(), respCode)
		}
		if respCode == api.RespTimeout {
			log.D.Println("Initiating gossip failure")
			protocol.InitMembershipGossip(handler.Conn, *ownerId, owner)
		}
	}

	protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
}

func HandlePut(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyValMsg := msg.(*api.KeyValueDgram)
	ownerId, owner := node.GetProcessNode().GetPeerResponsibleForKey(keyValMsg.Key)
	log.I.Printf("OwnerId is %s\n", ownerId.String())
	log.I.Printf("My Id is %s\n", node.GetProcessNode().ID.String())

	var replyMsg api.Message

	if *ownerId == node.GetProcessNode().ID {
		log.D.Printf("Storing value '%s' with key %v\n", keyValMsg.Value, keyValMsg.Key)
		err := node.GetProcessNode().Store.Put(
			store.Key(keyValMsg.Key),
			keyValMsg.Value)
		if err != nil {
			log.E.Println(err)
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		}
	} else {
		respCode := protocol.IntraNodePut(owner.Addr.String(), keyValMsg.Key, keyValMsg.Value)
		replyMsg = api.NewValueDgram(msg.UID(), respCode, make([]byte, 0, 0))
	}
	protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)
}

func HandleRemove(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	ownerId, owner := node.GetProcessNode().GetPeerResponsibleForKey(keyMsg.Key)
	var replyMsg api.Message
	log.I.Printf("OwnerId is %s\n", ownerId.String())
	log.I.Printf("My Id is %s\n", node.GetProcessNode().ID.String())

	if *ownerId == node.GetProcessNode().ID {
		log.D.Printf("Deleting value with key %v\n", keyMsg.Key)
		err := node.GetProcessNode().Store.Remove(store.Key(keyMsg.Key))
		if err != nil {
			log.E.Println(err)
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		}
	} else {
		respCode := protocol.IntraNodeRemove(owner.Addr.String(), keyMsg.Key)
		replyMsg = api.NewValueDgram(msg.UID(), respCode, make([]byte, 0, 0))
	}

	protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, replyMsg)
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
