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

func printReplicaKeyHandleMsg(key [32]byte, thisNode *node.Node) {
	replicas := thisNode.GetReplicaIdsForKey(key)
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
	printReplicaKeyHandleMsg(keyMsg.Key, node.GetProcessNode())
	storeval := execQuorum(api.CmdGet, keyMsg, handler, -1 /*timestamp not used*/)

	if storeval != nil {
		var replyMsg api.Message
		if !storeval.Active {
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, storeval.Val)
		}
		protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
	}
	// Otherwise, we didn't get enough data to make a decision.
	// Force timeout
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
	keyValMsg := msg.(*api.KeyValueDgram)
	if keyValMsg.Command() == api.CmdPut {
		keyValMsg.Key = convertClientKey(keyValMsg.Key)
	}

	printReplicaKeyHandleMsg(keyValMsg.Key, node.GetProcessNode())

	mostUpToDate := execQuorum(api.CmdGetTimestamp, msg, handler, -1 /*timestamp not used */)
	if mostUpToDate == nil {
		// timeout
		return
	}

	mostUpToDate = execQuorum(api.CmdPut, msg, handler, mostUpToDate.Timestamp)
	var replyMsg api.Message
	if mostUpToDate != nil {
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)
	}

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
	keyMsg := msg.(*api.KeyDgram)
	if keyMsg.Command() == api.CmdRemove {
		keyMsg.Key = convertClientKey(keyMsg.Key)
	}
	printReplicaKeyHandleMsg(keyMsg.Key, node.GetProcessNode())
	mostUpToDate := execQuorum(api.CmdGetTimestamp, keyMsg, handler, -1 /*timestamp not used */)
	if !mostUpToDate.Active {
		replyMsg := api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, replyMsg)
		return
	}

	mostUpToDate = execQuorum(api.CmdRemove, keyMsg, handler, mostUpToDate.Timestamp)
	if mostUpToDate != nil {
		replyMsg := api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
		protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, replyMsg)
	}
}

func HandleIntraRemove(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	thisNode := node.GetProcessNode()
	// Need to implement timestamp messages and include here
	log.I.Printf("Removing value with key %v\n", keyMsg.Key)
	err := thisNode.Store.Remove(keyMsg.Key, 2 /*timestamp*/)
	var replyMsg api.Message
	if err != nil {
		replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		log.D.Println(err)
	} else {
		// we return true to inidicate the value has been deleted
		storeVal := &store.StoreVal{Val: make([]byte, 0), Active: true, Timestamp: 2}
		valuedata, jsonerr := json.Marshal(storeVal)
		if jsonerr != nil {
			log.E.Println(err)
		}
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, valuedata)
	}
	protocol.ReplyToRemove(handler.Conn, recvAddr, handler.Cache, replyMsg)

}

func execQuorum(cmd byte, msg api.Message, handler *MessageHandler, timestamp int) *store.StoreVal {
	var key store.Key
	if msg.Command() == api.CmdPut {
		key = msg.(*api.KeyValueDgram).Key
	} else {
		key = msg.(*api.KeyDgram).Key
	}
	thisNode := node.GetProcessNode()
	replicaIds := thisNode.GetReplicaIdsForKey(key)
	respChan := make(chan *replicaData, config.GetConfig().MaxReplicas)
	receivedCount := 0
	for _, replica := range replicaIds {
		if replica == thisNode.ID {
			go channeledLocalCommand(respChan, cmd, msg, timestamp)
		} else {
			go channeledRemoteCommand(respChan, cmd, handler, replica, msg)
		}
	}

	receivedStoreVals := make([]*store.StoreVal, 0, len(replicaIds))
	for receivedCount < len(replicaIds) {
		data := <-respChan
		if data.Err != nil {
			log.I.Printf("Failed get from %s: %s", keyString(replicaIds[receivedCount]), data.Err)
		} else {
			log.I.Println("Receive successful")
			receivedStoreVals = append(receivedStoreVals, data.Val)
		}
		receivedCount++
	}

	// Get the highest timestamp value
	if len(receivedStoreVals) >= minSuccessfulOps(len(replicaIds)) {
		var mostUpToDate *store.StoreVal
		for _, storeVal := range receivedStoreVals {
			if mostUpToDate == nil {
				mostUpToDate = storeVal
			} else if storeVal.Timestamp > mostUpToDate.Timestamp {
				mostUpToDate = storeVal
			}
		}
		return mostUpToDate
	} else {
		return nil
	}
	// Otherwise, we didn't get enough data to make a decision.
	// Force timeout

}

func channeledLocalCommand(channel chan *replicaData, cmd byte, msg api.Message, timestamp int) {
	var key store.Key
	if msg.Command() == api.CmdPut {
		key = msg.(*api.KeyValueDgram).Key
	} else {
		key = msg.(*api.KeyDgram).Key
	}
	switch cmd {
	case api.CmdGet:
		log.I.Printf("Getting value with key %v\n", key)
		value, err := node.GetProcessNode().Store.Get(key)
		if err != nil {
			log.E.Println(err)
		}
		channel <- &replicaData{Val: value, Err: err}
	case api.CmdPut:
		log.I.Printf("Putting value with key %v\n", key)
		err := node.GetProcessNode().Store.Put(key, msg.(*api.KeyValueDgram).Value, timestamp+1) // TODO increment here?
		value, _ := node.GetProcessNode().Store.Get(key)
		channel <- &replicaData{Val: value, Err: err}
	case api.CmdRemove:
		log.I.Printf("Removing value with key %v\n", key)
		err := node.GetProcessNode().Store.Remove(key, timestamp+1) // TODO increment here?
		if err != nil {
			channel <- &replicaData{Val: nil, Err: err}
		} else {
			value, _ := node.GetProcessNode().Store.Get(key)
			channel <- &replicaData{Val: &store.StoreVal{Val: value.Val, Active: true, Timestamp: value.Timestamp}, Err: err}
		}
	case api.CmdGetTimestamp:
		log.I.Printf("Getting timestamp for key\n")
		value, _ := node.GetProcessNode().Store.Get(key)
		if value != nil {
			channel <- &replicaData{Val: &store.StoreVal{Val: value.Val, Active: value.Active, Timestamp: value.Timestamp}, Err: nil}
		} else {
			if cmd == api.CmdPut {
				log.D.Println("Record not found. Initiating timestamp")
				channel <- &replicaData{Val: &store.StoreVal{Val: make([]byte, 0, 0), Active: true, Timestamp: 0}, Err: nil}
			} else {

				channel <- &replicaData{Val: &store.StoreVal{Val: make([]byte, 0, 0), Active: false, Timestamp: 0}, Err: nil}
			}
		}
	default:
		channel <- &replicaData{Val: nil, Err: errors.New(fmt.Sprintf("Unknown command received\n"))}
	}

}

func channeledRemoteCommand(channel chan *replicaData, cmd byte, handler *MessageHandler,
	remotePeerKey store.Key, msg api.Message) {
	thisNode := node.GetProcessNode()
	peer := thisNode.KnownPeers[remotePeerKey]
	var storeVal *store.StoreVal
	var replyMsg api.Message
	switch cmd {
	case api.CmdGet:
		replyMsg = protocol.IntraNodeGet(peer.Addr.String(), msg)
	case api.CmdPut:
		replyMsg = protocol.IntraNodePut(peer.Addr.String(), msg)
	case api.CmdRemove:
		replyMsg = protocol.IntraNodeRemove(peer.Addr.String(), msg)
	case api.CmdGetTimestamp:
		replyMsg = protocol.IntraNodeGetTimestamp(peer.Addr.String(), msg)
	default:
		replyMsg = nil
	}
	var retErr error
	if replyMsg != nil {
		if replyMsg.Command() == api.RespOk || replyMsg.Command() == api.RespOkTimestamp {
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

func HandleGetTimestamp(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	thisNode := node.GetProcessNode()
	storeVal, err := thisNode.Store.Get(keyMsg.Key)
	var replyMsg api.Message
	// If timestamp doesn't exist, return 0
	if err != nil {
		log.D.Println("Key not found. Initiating timestamp")
		valuedata, jsonerr := json.Marshal(&store.StoreVal{Val: make([]byte, 0), Active: true, Timestamp: 0})
		if jsonerr != nil {
			log.E.Println(jsonerr)
		}
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOkTimestamp, valuedata)
	} else {
		valuedata, jsonerr := json.Marshal(&store.StoreVal{Val: storeVal.Val, Active: storeVal.Active, Timestamp: storeVal.Timestamp})
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
