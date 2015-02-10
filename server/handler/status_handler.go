package handler

import (
	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"html/template"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	UP             = "UP"
	OFFLINE        = "Offline"
	NOT_RESPONDING = "Not Responding"
)

type Status struct {
	Status           string
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
	StatusList map[string]*Status
}

func NewStatusHandler() *StatusHandler {

	statusList := make(map[string]*Status, 0)
	c := config.GetConfig()

	for _, node := range c.PeerList {
		var nodeStatus string
		if util.IsHostReachable(node, config.GetConfig().DialTimeout, config.GetConfig().DefaultPortList) {
			nodeStatus = UP
		} else {
			nodeStatus = OFFLINE
		}
		newStatus := &Status{
			nodeStatus,
			time.Now(), /*time //note that this field will hold the time the list was initalized*/
			"",         /*space used by the application*/
			[]*DiskSpaceEntry{},
			"", /*uptime*/
			"", /*current load*/
		}
		statusList[node] = newStatus
	}
	return &StatusHandler{
		StatusList: statusList,
	}
}

func (handler *StatusHandler) HandleStatusMessage(msg *api.ResponseMessage, recvAddr *net.UDPAddr) {
	data := strings.Split(string(msg.Value), "\t\n\t\n")
	//TODO - update old status instead of creating new status
	newStatus := &Status{
		UP,
		time.Now(),
		strings.TrimSpace(data[0]),
		parseDiskSpace(strings.TrimSpace(data[1])),
		strings.TrimSpace(data[2]),
		strings.TrimSpace(data[3]),
	}

	peers, err := net.LookupAddr(recvAddr.IP.String())
	if err != nil {
		log.E.Println(err)
	}

	for _, peer := range peers {
		// for some reason the return value has a . at the end
		if len(peer) > 0 {
			peer = peer[:len(peer)-1]
		}
		_, ok := handler.StatusList[peer]
		if ok {
			handler.StatusList[peer] = newStatus
		}
	}
	handler.CheckNodeReach()
}

func (handler *StatusHandler) CheckNodeReach() {
	for node, status := range handler.StatusList {
		if time.Now().Sub(status.LastSeen) > config.GetConfig().NodeTimeout {
			if status.Status == UP || status.Status == NOT_RESPONDING {
				if util.IsHostReachable(node, config.GetConfig().DialTimeout, config.GetConfig().DefaultPortList) {
					status.Status = NOT_RESPONDING
				} else {
					status.Status = OFFLINE
				}
			}
		}
	}
}

func (handler *StatusHandler) ServeHttp(writer http.ResponseWriter, req *http.Request) {

	t := template.New("status.html")
	t, err := t.ParseFiles("templates/status.html")
	if err != nil {
		log.E.Println(err)
	}
	err = t.Execute(writer, handler.StatusList)
	if err != nil {
		log.E.Println(err)
	}

}

/*
func GetStatusList() map[string]Status {
	return statusList
}
*/

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
