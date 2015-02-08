package handler

import (
	"fmt"
	"github.com/tsiemens/kvstore/shared/api"
	"net"
	"strings"
	"time"
	"crypto/rand"
)

type StatusHandler struct {
	ApplicationSpace	string
	DiskSpace			string
	Uptime				string
	CurrentLoad			string
}

var statusList []StatusHandler

func NewStatusHandler() *StatusHandler {
	return &StatusHandler{}
}

func (handler *StatusHandler) HandleStatusMessage(msg *api.ResponseMessage, recvAddr *net.UDPAddr) {
	if statusList == nil {
		statusList = make([]StatusHandler, 0)
	}
	data := strings.Split(string(msg.Value), "\t\n\t\n")
	newStatus := StatusHandler{strings.TrimSpace(data[0]), strings.TrimSpace(data[1]), strings.TrimSpace(data[2]), strings.TrimSpace(data[3])}
	statusList = append(statusList, newStatus)
	for _, status := range statusList {
		fmt.Println(status)
	}
}

func RequestStatusUpdate() ([]StatusHandler, error) {
	statusList = nil 
	buff := make([]byte, 32)
	
	 _, err := rand.Read(buff) 
	 if err != nil {
	 	return nil, err
	 }

	 key, err := api.NewKey(buff)
	 if(err != nil) {
	 	return nil, err
	 }

	 //Hardcoded the value of the server to fetch and update from for testing purposes
	 //It will need to be retrieved from a list of available servers I would assume
	 err = api.StatusUpdate("localhost:64000", key)
	 if(err != nil) {
	 	return nil, err
	 }

	 time.Sleep(10 * time.Second)

	 return statusList, nil
}
