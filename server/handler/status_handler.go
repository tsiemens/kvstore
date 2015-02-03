package handler

import (
	"fmt"
	"github.com/tsiemens/kvstore/shared/api"
	"net"
)

type StatusHandler struct {
}

func NewStatusHandler() *StatusHandler {
	return &StatusHandler{}
}

func (handler *StatusHandler) HandleStatusMessage(msg *api.ResponseMessage, recvAddr *net.UDPAddr) {
	fmt.Println("Status Received")

}
