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
	thisNode := node.GetProcessNode()
	ownerId, owner := thisNode.GetPeerResponsibleForKey(keyMsg.Key)
	log.I.Printf("OwnerId is %s\n", ownerId.String())
	log.I.Printf("My Id is %s\n", thisNode.ID.String())

	// If this node is responsible for key, return value
	if *ownerId == thisNode.ID {
		log.D.Printf("Getting value with key %v\n", keyMsg.Key)
		value, err := thisNode.Store.Get(store.Key(keyMsg.Key))
		var replyMsg api.Message
		if err != nil {
			log.E.Println(err)
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, value)
		}
		protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
		return

	}

	// If this node is not responsible for the key but is expected to be by the sending node,
	// return a membership msg
	if keyMsg.Command() == api.CmdIntraGet {
		protocol.ReplyMembershipMsg(handler.Conn, recvAddr, thisNode.ID,
			thisNode.KnownPeers, api.RespInvalidNode, msg.UID())
		return
	}

	// If this node it not responsible for the key but received a message from the client,
	// relay the command to the correct node
	if keyMsg.Command() == api.CmdGet {
		replyMsg := protocol.IntraNodeGet(owner.Addr.String(), keyMsg)
		if replyMsg != nil {
			if replyMsg.Command() == api.RespInvalidNode {
				handleMembership(replyMsg, owner.Addr)
			} else {
				protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
				log.D.Println("Replying to get")
			}
		} else { // Timeout occured
			thisNode.SetPeerOffline(*ownerId)
			newOwnerId, newOwner := thisNode.GetPeerResponsibleForKey(keyMsg.Key)
			if *newOwnerId != thisNode.ID {
				protocol.SendMembershipMsg(handler.Conn, newOwner.Addr, thisNode.ID,
					map[store.Key]*node.Peer{*ownerId: owner}, api.CmdMembershipFailure)
			}
			protocol.InitMembershipGossip(handler.Conn, ownerId, owner)
			// A5 TODO: here you would query the backup nodes
		}
	}

	//protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
}

func HandlePut(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	thisNode := node.GetProcessNode()
	keyValMsg := msg.(*api.KeyValueDgram)
	ownerId, owner := thisNode.GetPeerResponsibleForKey(keyValMsg.Key)
	log.I.Printf("OwnerId is %s\n", ownerId.String())
	log.I.Printf("My Id is %s\n", thisNode.ID.String())

	// If this node is responsible for key, return value
	if *ownerId == thisNode.ID {
		log.D.Printf("Storing value '%s' with key %v\n", keyValMsg.Value, keyValMsg.Key)
		var replyMsg api.Message
		err := thisNode.Store.Put(
			store.Key(keyValMsg.Key),
			keyValMsg.Value)
		if err != nil {
			log.E.Println(err)
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		}
		protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)
		return
	}

	// If this node is not responsible for the key but is expected to be by the sending node,
	// return a membership msg
	if keyValMsg.Command() == api.CmdIntraPut {
		protocol.ReplyMembershipMsg(handler.Conn, recvAddr, thisNode.ID,
			thisNode.KnownPeers, api.RespInvalidNode, msg.UID())
		return
	}

	// If this node it not responsible for the key but received a message from the client,
	// relay the command to the correct node
	if keyValMsg.Command() == api.CmdPut {
		log.D.Println(owner)

		replyMsg := protocol.IntraNodePut(owner.Addr.String(), keyValMsg)
		if replyMsg != nil {
			if replyMsg.Command() == api.RespInvalidNode {
				handleMembership(replyMsg, owner.Addr)
			} else {
				protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)
			}
		} else {
			thisNode.SetPeerOffline(*ownerId)
			newOwnerId, newOwner := thisNode.GetPeerResponsibleForKey(keyValMsg.Key)
			if *newOwnerId != thisNode.ID {
				protocol.ReplyMembershipMsg(handler.Conn, newOwner.Addr, thisNode.ID,
					map[store.Key]*node.Peer{*ownerId: owner}, api.CmdMembershipFailure, keyValMsg.UID())
			}
			protocol.InitMembershipGossip(handler.Conn, ownerId, owner)
			HandlePut(handler, msg, recvAddr)
			return
		}
	}
}

func HandleRemove(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	thisNode := node.GetProcessNode()
	keyMsg := msg.(*api.KeyDgram)
	ownerId, owner := thisNode.GetPeerResponsibleForKey(keyMsg.Key)
	log.I.Printf("OwnerId is %s\n", ownerId.String())
	log.I.Printf("My Id is %s\n", thisNode.ID.String())

	// If this node is responsible for key, return value
	if *ownerId == thisNode.ID {
		log.D.Printf("Deleting value with key %v\n", keyMsg.Key)
		var replyMsg api.Message
		err := thisNode.Store.Remove(store.Key(keyMsg.Key))
		if err != nil {
			log.E.Println(err)
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		}
		protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, replyMsg)
		return
	}

	// If this node is not responsible for the key but is expected to be by the sending node,
	// return a membership msg
	if keyMsg.Command() == api.CmdIntraRemove {
		protocol.ReplyMembershipMsg(handler.Conn, recvAddr, thisNode.ID,
			thisNode.KnownPeers, api.RespInvalidNode, msg.UID())
		return
	}

	// If this node it not responsible for the key but received a message from the client,
	// relay the command to the correct node
	if keyMsg.Command() == api.CmdRemove {
		replyMsg := protocol.IntraNodeRemove(owner.Addr.String(), keyMsg)
		if replyMsg != nil {
			if replyMsg.Command() == api.RespInvalidNode {
				handleMembership(replyMsg, owner.Addr)
			} else {
				protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, replyMsg)
			}
		} else {
			thisNode.SetPeerOffline(*ownerId)
			newOwnerId, newOwner := thisNode.GetPeerResponsibleForKey(keyMsg.Key)
			if *newOwnerId != thisNode.ID {
				protocol.SendMembershipMsg(handler.Conn, newOwner.Addr, thisNode.ID,
					map[store.Key]*node.Peer{*ownerId: owner}, api.CmdMembershipFailure)
			}
			protocol.InitMembershipGossip(handler.Conn, ownerId, owner)
			// A5 TODO: here you would query the backup nodes
		}
	}

}

func HandleStatusUpdate(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	conf := config.GetConfig()
	log.D.Println("Status Update handle called")
	keyValMsg := msg.(*api.KeyValueDgram)
	if handler.IsNewMessage(keyValMsg.UID()) {
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

	if handler.ShouldGossip(keyValMsg.UID()) {
		protocol.Gossip(handler.Conn, keyValMsg)
	}
}

func HandleAdhocUpdate(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	conf := config.GetConfig()
	keyValMsg := msg.(*api.KeyValueDgram)
	if handler.IsNewMessage(keyValMsg.UID()) {
		success, status := exec.RunCommand(string(keyValMsg.Value))
		protocol.ReplyToStatusUpdateServer(handler.Conn, conf.StatusServerAddr, handler.Cache, msg, []byte(status), success)
	}

	if handler.ShouldGossip(keyValMsg.UID()) {
		protocol.Gossip(handler.Conn, keyValMsg)
	}
}

func HandleMembershipMsgExchange(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleMembership(msg, recvAddr)
	thisNode := node.GetProcessNode()
	protocol.SendMembershipMsg(handler.Conn, recvAddr,
		thisNode.ID, thisNode.KnownPeers, api.CmdMembership)
}

func HandleMembershipMsg(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleMembership(msg, recvAddr)
}

func HandleMembershipFailureGossip(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleMembership(msg, recvAddr)
	if handler.ShouldGossip(msg.(*api.KeyValueDgram).UID()) {
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

func handleMembership(msg api.Message, recvAddr *net.UDPAddr) {
	if keyValMsg, ok := msg.(*api.KeyValueDgram); ok {
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
