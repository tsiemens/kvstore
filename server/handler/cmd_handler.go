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

func HandleGet(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyMsg := msg.(*api.KeyDgram)
	keyMsg.Key = convertClientKey(keyMsg.Key)

	thisNode := node.GetProcessNode()
	printReplicaKeyHandleMsg(keyMsg.Key, thisNode)

	storeKey := store.Key(keyMsg.Key)
	storeval, err := getValueFromResponsibleNode(handler, thisNode.GetReplicaIdsForKey(storeKey),
		&storeKey)
	if err != nil {
		log.E.Println(err)
		storeval = nil
	}

	var replyMsg api.Message
	if storeval == nil {
		replyMsg = api.NewBaseDgram(msg.UID(), api.RespInternalError)
	} else if !storeval.Active {
		replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
	} else {
		replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, storeval.Val)
	}
	protocol.ReplyToGet(handler.Conn, recvAddr, replyMsg)
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
	handleWrite(handler, msg, recvAddr, true)
}

func HandleRemove(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	handleWrite(handler, msg, recvAddr, false)
}

func handleWrite(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr, active bool) {
	var key [32]byte
	var value []byte = make([]byte, 0)

	switch msg.Command() {
	case api.CmdPut:
		keyValMsg := msg.(*api.KeyValueDgram)
		key = convertClientKey(keyValMsg.Key)
		value = keyValMsg.Value
	case api.CmdRemove:
		keyMsg := msg.(*api.KeyDgram)
		key = convertClientKey(keyMsg.Key)
	default:
		replyMsg := api.NewBaseDgram(msg.UID(), api.RespInternalError)
		protocol.ReplyCached(handler.Conn, recvAddr, handler.Cache, replyMsg)
	}

	printReplicaKeyHandleMsg(key, node.GetProcessNode())

	storeKey := store.Key(key)
	respCode := writeValueToResponsibleNode(handler, node.GetProcessNode().GetReplicaIdsForKey(storeKey),
		&storeKey, value, active)

	// using value dgram, because our client's parser requires ok responses to be in value fmt
	replyMsg := api.NewValueDgram(msg.UID(), respCode, make([]byte, 0))
	protocol.ReplyCached(handler.Conn, recvAddr, handler.Cache, replyMsg)
}

// Gets the value from the most responsible node (in order).
// Will return a store value even if the key does not exist. It will be inactive
// error is non nil if another error occurs
func getValueFromResponsibleNode(handler *MessageHandler, replicaIds []store.Key,
	key *store.Key) (*store.StoreVal, error) {

	thisNode := node.GetProcessNode()
	var storeVal *store.StoreVal
	var retErr error

	for _, peerId := range replicaIds {
		if thisNode.ID == peerId {
			sv, err := thisNode.Store.Get(*key)
			storeVal = sv
			if err != nil {
				// A dummy store value, indicating we don't have it
				storeVal = &store.StoreVal{Val: make([]byte, 0), Active: false, Timestamp: 0}
			}
			retErr = nil
			break
		} else {
			peer := thisNode.KnownPeers[peerId]
			replyMsg := protocol.IntraNodeGet(peer.Addr.String(), key)
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
						replyMsg.Command(), peerId.String()))
				}
				break
			} else { // Timeout occured
				thisNode.SetPeerOffline(peerId)
				protocol.InitMembershipGossip(handler.Conn, &peerId, peer)
				retErr = errors.New(fmt.Sprintf("Timeout on node %s",
					peerId.String()))
			}
		}
	}
	return storeVal, retErr
}

// Writes the value to the responsible node. Returns the appropriate response code for the client
func writeValueToResponsibleNode(handler *MessageHandler, replicaIds []store.Key,
	key *store.Key, val []byte, active bool) byte {

	thisNode := node.GetProcessNode()

	for _, peerId := range replicaIds {
		if thisNode.ID == peerId {
			timestamp, err := thisNode.Store.WriteInc(*key, val, active)
			if err != nil {
				return api.RespInvalidKey
			} else {
				// Lazily write to the other replicas
				storeVal := &store.StoreVal{Val: val, Active: active, Timestamp: timestamp}
				go writeToMyReplicas(handler, *key, storeVal)
				return api.RespOk
			}
		} else {
			peer := thisNode.KnownPeers[peerId]
			replyMsg := protocol.IntraNodeWrite(peer.Addr.String(), key, val, active, 0)
			if replyMsg != nil {
				if replyMsg.Command() == api.RespOk {
					return api.RespOk
				} else if replyMsg.Command() == api.RespInvalidKey {
					return api.RespInvalidKey
				} else {
					log.E.Printf("Error %d on node %s",
						replyMsg.Command(), peerId.String())
					return api.RespInternalError
				}
			} else { // Timeout occured
				thisNode.SetPeerOffline(peerId)
				protocol.InitMembershipGossip(handler.Conn, &peerId, peer)
				log.I.Printf("Timeout on node %s", peerId.String())
			}
		}
	}
	return api.RespInternalError
}

func HandleIntraWrite(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	keyValueMsg := msg.(*api.KeyValueDgram)
	thisNode := node.GetProcessNode()

	var storeVal *store.StoreVal

	err := json.Unmarshal(keyValueMsg.Value, &storeVal)
	if err != nil {
		log.E.Println(err)
		replyMsg := api.NewBaseDgram(msg.UID(), api.RespInternalError)
		protocol.ReplyToPut(handler.Conn, recvAddr, handler.Cache, replyMsg)
		return
	}

	if storeVal.Active {
		log.I.Printf("Putting value with key %v\n", keyValueMsg.Key)
	} else {
		log.I.Printf("Removing value with key %v\n", keyValueMsg.Key)
	}

	if storeVal.Timestamp == 0 {
		// This is the primary node, so we are incrementing the timestamp
		timestamp, err := thisNode.Store.WriteInc(keyValueMsg.Key, storeVal.Val, storeVal.Active)
		var replyMsg api.Message
		if err != nil {
			replyMsg = api.NewBaseDgram(msg.UID(), api.RespInvalidKey)
		} else {
			// Need to use value dgram due to paring logic with ok
			replyMsg = api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0))
			storeVal.Timestamp = timestamp
			go writeToMyReplicas(handler, keyValueMsg.Key, storeVal)
		}
		protocol.ReplyCached(handler.Conn, recvAddr, handler.Cache, replyMsg)
	} else {
		// This is just a replica node. Should blindly replicate the data
		thisNode.Store.WriteIfNewer(keyValueMsg.Key, storeVal.Val, storeVal.Active,
			storeVal.Timestamp)
		// Need to use value dgram due to paring logic with ok
		replyMsg := api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0))
		protocol.ReplyCached(handler.Conn, recvAddr, handler.Cache, replyMsg)
	}
}

func writeToMyReplicas(handler *MessageHandler, key store.Key, storeVal *store.StoreVal) {
	thisNode := node.GetProcessNode()
	replicaIds := thisNode.GetAllSuccessors(key)

	neededReplicas := config.GetConfig().MaxReplicas - 1
	replicas := 0
	for _, peerId := range replicaIds {
		if thisNode.ID != peerId {
			peer := thisNode.KnownPeers[peerId]
			replyMsg := protocol.IntraNodeWrite(peer.Addr.String(), &key, storeVal.Val,
				storeVal.Active, storeVal.Timestamp)
			if replyMsg != nil {
				if replyMsg.Command() != api.RespOk {
					log.E.Printf("Error replicating %d on node %s",
						replyMsg.Command(), peerId.String())
				}

				// In case we get some timeouts, which may not come back, we need
				// to have data replicated on the other machines.
				// In the event of catastrophic failure, we still need to
				// replicate the data, even if all the old replicas died.
				replicas++
				if replicas >= neededReplicas {
					break
				}
			} else { // Timeout occured
				thisNode.SetPeerOffline(peerId)
				protocol.InitMembershipGossip(handler.Conn, &peerId, peer)
				log.I.Printf("Timeout on node %s", peerId.String())
			}
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
	replyMsg := api.NewValueDgram(msg.UID(), api.RespOk, make([]byte, 0, 0))
	protocol.ReplyCached(handler.Conn, recvAddr, handler.Cache, replyMsg)
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
		nodeStore.WriteIfNewer(key, val.Val, val.Active, val.Timestamp)
	}
	protocol.ReplyToStorePush(handler.Conn, recvAddr, handler.Cache, msg)
}

// Do nothing. Need to avoid unknown command cyles.
func HandleUnknownCommand(handler *MessageHandler, msg api.Message, recvAddr *net.UDPAddr) {
	log.D.Println("Doing nothing")
	return
}
