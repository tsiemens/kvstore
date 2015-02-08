package api

import (
	"fmt"
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
)

func Gossip(conn *net.UDPConn, msg *RequestMessage) {
	conf := config.GetConfig()
	for i := 0; i < conf.NotifyCount; i++ {
		url := conf.GetRandAddr()
		addr, err := net.ResolveUDPAddr("udp", url)
		if err != nil {
			log.E.Println("Error resolving host:", err)
			continue
		}
		requestMsg := newRequestMessage(addr, CmdStatusUpdate, msg.Key, msg.Value)
		log.D.Println("Gossiping to", addr)
		_, err = conn.WriteTo(requestMsg.Bytes(), addr)
		if err != nil {
			fmt.Println(err)
		}
	}
}
