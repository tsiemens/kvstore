package handler

import "net"

import "github.com/tsiemens/kvstore/shared/util"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"

func StartHandler(loopback bool) (chan bool, *net.UDPConn, *net.UDPAddr, error) {
	con, localAddr, err := util.CreateUDPSocket(loopback)
	if err != nil {
		return nil, nil, nil, err
	}

	exit := make(chan bool)
	go api.LoopReceiver(con, api.ClientMessageHandler(handleMessage), exit)
	return exit, con, localAddr, nil
}

func handleMessage(msg *api.ClientMessage, recvAddr *net.UDPAddr) {
	log.D.Println("handling a msg!")
}
