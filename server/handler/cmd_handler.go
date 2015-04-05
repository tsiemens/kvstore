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

type replicaData struct {
	Val *store.StoreVal
	Err error
}

type replicaVersionData struct {
	Version int
	Err     error
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

	respChan := make(chan *replicaData, config.GetConfig().MaxReplicas)
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

func channeledLocalGet(channel chan *replicaData, key store.Key) {
	value, err := node.GetProcessNode().Store.Get(key)
	if err != nil {
		log.E.Println(err)
	}
	channel <- &replicaData{Val: value, Err: err}
}

func channeledRemoteGet(channel chan *replicaData, handler *MessageHandler,
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
	channel <- &replicaData{Val: storeVal, Err: retErr}
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
	replicaIds := thisNode.GetReplicaIdsForKey(keyValMsg.Key)

	printReplicaKeyHandleMsg(keyValMsg.Key, replicaIds, thisNode)

	respVersionChan := make(chan *replicaVersionData, config.GetConfig().MaxReplicas)

	receivedCount := 0

	log.D.Println("Waiting for write majority...")
	for _, replica := range replicaIds {
		if replica == thisNode.ID {
			go channeledLocalVersion(respVersionChan, keyValMsg.Key)
		} else {
			go channeledRemoteVersion(respVersionChan, handler, replica, keyValMsg)
		}
	}

	var latestVersion int
	receivedVersionVals := make([]int, 0, len(replicaIds))
	for receivedCount < len(replicaIds) {
		getVersion := <-respVersionChan
		if getVersion.Err != nil {
			log.I.Printf("Failed version get: %s\n", getVersion.Err)
		} else {
			receivedVersionVals = append(receivedVersionVals, getVersion.Version)
		}
		receivedCount++
	}

	if len(receivedVersionVals) >= minSuccessfulOps(len(replicaIds)) {
		log.D.Println("Received write majority")
		latestVersion = receivedVersionVals[0]
		for _, version := range receivedVersionVals {
			if version > latestVersion {
				latestVersion = version
			}
		}
	} else {
		//TODO release nodes and timeout
		return
	}

	respChan := make(chan *replicaVersionData, config.GetConfig().MaxReplicas)
	receivedCount = 0

	log.D.Println("Writing to backup nodes...")
	for _, replica := range replicaIds {
		if replica == thisNode.ID {
			go channeledLocalPut(respChan, keyValMsg.Key, keyValMsg.Value, latestVersion)
		} else {
			go channeledRemotePut(respChan, handler, replica, keyValMsg, latestVersion)
		}
	}
	receivedStoreVals := make([]*replicaVersionData, 0, len(replicaIds))
	for receivedCount < len(replicaIds) {
		versionData := <-respChan
		if versionData.Err != nil {
			log.I.Printf("Failed put: %s\n", versionData.Err)
		} else {
			receivedStoreVals = append(receivedStoreVals, versionData)
		}
		receivedCount++
	}

	var replyMsg api.Message
	if len(receivedStoreVals) >= minSuccessfulOps(len(replicaIds)) {
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)

	}

}

func channeledLocalVersion(channel chan *replicaVersionData, key store.Key) {
	value, err := node.GetProcessNode().Store.Get(key)
	if err != nil {
		log.D.Println(err)
	}
	if value != nil {
		channel <- &replicaVersionData{Version: value.Timestamp, Err: nil}
	} else {
		channel <- &replicaVersionData{Version: 0, Err: nil}
	}
}

func channeledRemoteVersion(channel chan *replicaVersionData, handler *MessageHandler,
	remotePeerKey store.Key, keyValueMsg *api.KeyValueDgram) {

	thisNode := node.GetProcessNode()
	peer := thisNode.KnownPeers[remotePeerKey]
	var versionVal *replicaVersionData
	var retErr error
	replyMsg := protocol.IntraNodeGetTimestamp(peer.Addr.String(), keyValueMsg)
	if replyMsg != nil {
		if replyMsg.Command() == api.RespOkTimestamp {
			valMsg := replyMsg.(*api.ValueDgram)
			retErr = json.Unmarshal(valMsg.Value, &versionVal)
		} else if replyMsg.Command() == api.RespInvalidKey {
			// TODO ???
		} else {
			retErr = errors.New(fmt.Sprintf("Error %d on node %s\n",
				replyMsg.Command(), remotePeerKey.String()))
		}
	} else { // timeout occured
		thisNode.SetPeerOffline(remotePeerKey)
		protocol.InitMembershipGossip(handler.Conn, &remotePeerKey, peer)
		retErr = errors.New(fmt.Sprintf("Timeout on node %s\n",
			remotePeerKey.String()))
	}
	channel <- &replicaVersionData{Version: versionVal.Version, Err: retErr}

}

func channeledLocalPut(channel chan *replicaVersionData, key store.Key, value []byte, timestamp int) {
	log.I.Printf("Putting value with key %v\n", key)
	err := node.GetProcessNode().Store.Put(key, value, timestamp+1) // TODO increment here?
	channel <- &replicaVersionData{Version: timestamp + 1, Err: err}
}

//  merged with channeledRemoteGet into one function?
func channeledRemotePut(channel chan *replicaVersionData, handler *MessageHandler,
	remotePeerKey store.Key, keyValueMsg *api.KeyValueDgram, timestamp int) {

	thisNode := node.GetProcessNode()
	peer := thisNode.KnownPeers[remotePeerKey]
	var storeVal *store.StoreVal
	var retErr error
	replyMsg := protocol.IntraNodePut(peer.Addr.String(), keyValueMsg)
	if replyMsg != nil {
		if replyMsg.Command() == api.RespOk {
			valMsg := replyMsg.(*api.ValueDgram)
			retErr = json.Unmarshal(valMsg.Value, &storeVal)
		} else if replyMsg.Command() == api.RespInvalidKey {
			// TODO ???
		} else {
			retErr = errors.New(fmt.Sprintf("Error %d on node %s\n",
				replyMsg.Command(), remotePeerKey.String()))
		}
	} else { // timeout occured
		thisNode.SetPeerOffline(remotePeerKey)
		protocol.InitMembershipGossip(handler.Conn, &remotePeerKey, peer)
		retErr = errors.New(fmt.Sprintf("Timeout on node %s\n",
			remotePeerKey.String()))
	}
	channel <- &replicaVersionData{Version: storeVal.Timestamp, Err: retErr}
}

func HandleIntraPut(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyValueMsg := msg.(*api.KeyValueDgram)
	thisNode := node.GetProcessNode()

	// Need to implement timestamp messages and include here
	log.I.Printf("Putting value with key %v\n", keyValueMsg.Key)
	err := thisNode.Store.Put(keyValueMsg.Key, keyValueMsg.Value, 2 /* timestamp */)
	storeVal, err := thisNode.Store.Get(keyValueMsg.Key)
	var replyMsg api.Message
	if err != nil {
		log.E.Println(err)
		replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
	} else {
		valuedata, jsonerr := json.Marshal(storeVal)
		if jsonerr != nil {
			log.E.Println(err)
		}
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, valuedata)
	}
	protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)

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

func HandleGetTimestamp(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	thisNode := node.GetProcessNode()
	storeVal, err := thisNode.Store.Get(keyMsg.Key)
	var replyMsg api.Message
	// If timestamp doesn't exist, return 0
	if err != nil {
		log.D.Println(err)
		valuedata, jsonerr := json.Marshal(&replicaVersionData{Version: 0, Err: nil})
		if jsonerr != nil {
			log.E.Println(jsonerr)
		}
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOkTimestamp, valuedata)
	} else {
		valuedata, jsonerr := json.Marshal(&replicaVersionData{Version: storeVal.Timestamp, Err: nil})
		if jsonerr != nil {
			log.E.Println(jsonerr)
		}
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOkTimestamp, valuedata)
	}
	protocol.ReplyToGetTimestamp(handler.Conn, recvAddr, replyMsg)

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
