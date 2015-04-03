package handler

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/protocol"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/exec"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

func convertClientKey(clientKey [32]byte) [32]byte {
	return sha256.Sum256(clientKey[:])
}

func keyString(key [32]byte) string {
	k := store.Key(key)
	return (&k).String()
}

func printKeyHandleMsg(key [32]byte, owner *store.Key, thisNode *node.Node) {
	log.I.Printf("Handling Key %s\n\tOwnerId is %s\n\tMy Id is %s\n",
		keyString(key), owner.String(), thisNode.ID.String())
}

func printReplicaKeyHandleMsg(key [32]byte, replicas []store.Key, thisNode *node.Node) {
	s := fmt.Sprintf("Handling Key %s\n\tMy Id is %s\n\tReplicas:\n",
		keyString(key), thisNode.ID.String())
	for _, k := range replicas {
		s += fmt.Sprintf("\t%s\n", k.String())
	}
	log.I.Printf(s)
}

type replicaGetData struct {
	Val *store.StoreVal
	Err error
}

func minSuccessfulOps(attempts int) int {
	return int((float32(attempts) / 2) + 1)
}

func HandleGet(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	if keyMsg.Command() == api.CmdGet {
		keyMsg.Key = convertClientKey(keyMsg.Key)
	}
	thisNode := node.GetProcessNode()
	replicaIds := thisNode.GetReplicaIdsForKey(keyMsg.Key)

	printReplicaKeyHandleMsg(keyMsg.Key, replicaIds, thisNode)

	respChan := make(chan *replicaGetData, node.MaxReplicas)
	// Send get to all replicas

	receivedCount := 0
	for _, replica := range replicaIds {
		if replica == thisNode.ID {
			go channeledLocalGet(respChan, keyMsg.Key)
		} else {
			go channeledRemoteGet(respChan, handler, replica, keyMsg)
		}
	}

	receivedStoreVals := make([]*store.StoreVal, 0, len(replicaIds))
	for receivedCount < len(replicaIds) {
		getData := <-respChan
		if getData.Err != nil {
			log.I.Printf("Failed get: %s", getData.Err)
		} else {
			receivedStoreVals = append(receivedStoreVals, getData.Val)
		}
		receivedCount++
	}

	// Get the most up to date value returned
	if len(receivedStoreVals) >= minSuccessfulOps(len(replicaIds)) {
		var mostUpToDate *store.StoreVal
		for _, storeVal := range receivedStoreVals {
			if mostUpToDate == nil {
				mostUpToDate = storeVal
			} else if storeVal.Timestamp > mostUpToDate.Timestamp {
				mostUpToDate = storeVal
			}
		}

		var replyMsg api.Message
		if !mostUpToDate.Active {
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, mostUpToDate.Val)
		}
		protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
	}
	// Otherwise, we didn't get enough data to make a decision.
	// Force timeout
}

func channeledLocalGet(channel chan *replicaGetData, key store.Key) {
	value, err := node.GetProcessNode().Store.Get(key)
	channel <- &replicaGetData{Val: value, Err: err}
}

func channeledRemoteGet(channel chan *replicaGetData, handler *MessageHandler,
	remotePeerKey store.Key, keyMsg *api.KeyDgram) {

	thisNode := node.GetProcessNode()
	peer := thisNode.KnownPeers[remotePeerKey]
	var storeVal *store.StoreVal
	var retErr error
	replyMsg := protocol.IntraNodeGet(peer.Addr.String(), keyMsg)
	if replyMsg != nil {
		if replyMsg.Command() == api.RespOk {
			valMsg := replyMsg.(*api.ValueDgram)
			retErr = json.Unmarshal(valMsg.Value, &storeVal)
		} else if replyMsg.Command() == api.RespInvalidKey {
			// Simulate an absent key with no priority
			// This way, it is a valid response, to differentiate between
			// a legit error
			storeVal = &store.StoreVal{Active: false, Timestamp: 0}
			retErr = nil
		} else {
			retErr = errors.New(fmt.Sprintf("Error %d on node %s",
				replyMsg.Command(), remotePeerKey.String()))
		}
	} else { // Timeout occured
		thisNode.SetPeerOffline(remotePeerKey)
		protocol.InitMembershipGossip(handler.Conn, &remotePeerKey, peer)
		retErr = errors.New(fmt.Sprintf("Timeout on node %s",
			remotePeerKey.String()))
	}
	channel <- &replicaGetData{Val: storeVal, Err: retErr}
}

func HandleIntraGet(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	thisNode := node.GetProcessNode()

	// for now, very simple. dont check if we are a legit backup
	// just return whatever we've got
	log.I.Printf("Getting value with key %v\n", keyMsg.Key)
	value, err := thisNode.Store.Get(store.Key(keyMsg.Key))
	var replyMsg api.Message
	if err != nil {
		log.E.Println(err)
		replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
	} else {
		valuedata, err := json.Marshal(value)
		if err != nil {
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInternalError)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, valuedata)
		}
	}
	protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
}

func HandlePut(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	thisNode := node.GetProcessNode()
	keyValMsg := msg.(*api.KeyValueDgram)
	if keyValMsg.Command() == api.CmdPut {
		keyValMsg.Key = convertClientKey(keyValMsg.Key)
	}
	ownerId, owner := thisNode.GetPeerResponsibleForKey(keyValMsg.Key)
	printKeyHandleMsg(keyValMsg.Key, ownerId, thisNode)

	// If this node is responsible for key, return value
	if *ownerId == thisNode.ID {
		log.D.Printf("Storing value '%s' with key %v\n", keyValMsg.Value, keyValMsg.Key)
		var replyMsg api.Message
		err := thisNode.Store.Put(
			store.Key(keyValMsg.Key),
			keyValMsg.Value,
			1) // TODO THIS TIMESTAMP IS TEMPORARY
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
	if keyMsg.Command() == api.CmdRemove {
		keyMsg.Key = convertClientKey(keyMsg.Key)
	}
	ownerId, owner := thisNode.GetPeerResponsibleForKey(keyMsg.Key)
	printKeyHandleMsg(keyMsg.Key, ownerId, thisNode)

	// If this node is responsible for key, return value
	if *ownerId == thisNode.ID {
		log.D.Printf("Deleting value with key %v\n", keyMsg.Key)
		var replyMsg api.Message
		err := thisNode.Store.Remove(store.Key(keyMsg.Key), 1) // TODO timestamp is temporary!
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

func HandleStorePush(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	valueMsg := msg.(*api.ValueDgram)
	keyVals, err := protocol.ParseStorePushMsgValue(valueMsg.Value)
	if err != nil {
		log.E.Println("Failed to parse incoming store push data")
		return
	}

	// for now, receiving this message will just cause this node to store all the contents regardless of key range. Key overflow is not an issue now
	nodeStore := node.GetProcessNode().Store
	for key, val := range keyVals {
		nodeStore.PutDirect(key, val)
	}
	protocol.ReplyToStorePush(handler.Conn, recvAddr, handler.Cache, msg)
}
