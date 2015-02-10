package handler

import (
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/shared/api"
	"net"
	"strings"
	"time"
)

const (
	UP        = 0
	OFFLINE   = 1
	UNDEFINED = -1
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

func NewStatusList(c *Config) {
	if statusList == nil {
		statusList = make(map[string]Status, 0)
	}
	//add all of the servers in the PeerList to the statusList
	for i := 0; i < len(c.PeerList); i++ { /*more items to be added to the statusList*/
		//set initial values for undefined status nodes here
		newStatus := Status{
			UNDEFINED,  /*using -1 for a node that has never checked in with the server	*/
			time.Now(), /*time //note that this field will hold the time the list was initalized*/
			"",         /*space used by the application*/
			DiskSpaceEntry{
				"", /*Filesystem	*/
				"", /*Blocks	*/
				"", /*Used	*/
				"", /*Available	*/
				"", /*UsePercentage	*/
				"", /*MountedOn	*/
			}, /*disk space on the system*/
			"", /*uptime*/
			"", /*current load*/
		}
		statusList[c.Peerlist[i]] = newStatus
	}

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
