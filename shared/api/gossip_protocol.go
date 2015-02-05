package api

import (
	"fmt"
	"github.com/tsiemens/kvstore/server/config"
	"net"
)

func Gossip(conn *net.UDPConn, msg *RequestMessage) {
	conf := config.GetConfig()
	for i := 0; i < conf.NotifyCount; i++ {
		url := conf.GetRandAddr()
		addr, err := net.ResolveUDPAddr("udp", url)
		requestMsg := newRequestMessage(addr, CmdStatusUpdate, msg.Key, msg.Value)
		_, err = conn.WriteTo(requestMsg.Bytes(), addr)
		if err != nil {
			fmt.Println(err)
		}
	}
}
