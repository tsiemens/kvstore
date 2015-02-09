package handler

import (
	"github.com/tsiemens/kvstore/shared/api"
	"net"
	"strings"
	"time"
)

const (
	UP      = 0
	OFFLINE = 1
)

type Status struct {
	Status           int
	LastSeen         time.Time
	ApplicationSpace string
	DiskSpace        []*DiskSpaceEntry
	Uptime           string
	CurrentLoad      string
}

type DiskSpaceEntry struct {
	Filesystem    string
	Blocks        string
	Used          string
	Available     string
	UsePercentage string
	MountedOn     string
}

type StatusHandler struct {
}

var statusList map[string]Status

func NewStatusHandler() *StatusHandler {
	return &StatusHandler{}
}

func (handler *StatusHandler) HandleStatusMessage(msg *api.ResponseMessage, recvAddr *net.UDPAddr) {
	if statusList == nil {
		statusList = make(map[string]Status, 0)
	}
	data := strings.Split(string(msg.Value), "\t\n\t\n")
	newStatus := Status{
		1,
		time.Now(),
		strings.TrimSpace(data[0]),
		parseDiskSpace(strings.TrimSpace(data[1])),
		strings.TrimSpace(data[2]),
		strings.TrimSpace(data[3]),
	}
	statusList[recvAddr.String()] = newStatus
}

func GetStatusList() map[string]Status {
	return statusList
}

func parseDiskSpace(input string) []*DiskSpaceEntry {
	diskSpaceEntries := make([]*DiskSpaceEntry, 0)

	diskSpaceInfo := strings.Fields(input)
	if len(diskSpaceInfo) < 7 || len(diskSpaceInfo)%7 == 0 {
		return nil
	}
	tableInfo := diskSpaceInfo[7:]
	for i := 0; i < len(tableInfo); i = i + 6 {
		diskSpaceEntry := &DiskSpaceEntry{
			Filesystem:    tableInfo[0+i],
			Blocks:        tableInfo[1+i],
			Used:          tableInfo[2+i],
			Available:     tableInfo[3+i],
			UsePercentage: tableInfo[4+i],
			MountedOn:     tableInfo[5+i],
		}

		diskSpaceEntries = append(diskSpaceEntries, diskSpaceEntry)
	}
	return diskSpaceEntries
}
