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
	OFFLINE        = "Host Offline"
	NOT_RESPONDING = "Node Not Responding"
	UNKNOWN        = "Unknown"
)

type Status struct {
	Status           string
	LastSeen         *time.Time
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
	HostIPMap  map[string]string
}

func NewStatusHandler() *StatusHandler {

	statusList := make(map[string]*Status, 0)
	hostIPMap := make(map[string]string, 0)
	c := config.GetConfig()

	for _, node := range c.PeerList {
		/*
			var nodeStatus string
			if util.IsHostReachable(node, c.DialTimeout, c.DefaultPortList) {
				nodeStatus = UP
			} else {
				nodeStatus = OFFLINE
			}
		*/
		newStatus := &Status{
			UNKNOWN,
			nil,
			"", /*space used by the application*/
			[]*DiskSpaceEntry{},
			"", /*uptime*/
			"", /*current load*/
		}
		ip, err := net.LookupIP(node)
		if err != nil {
			log.E.Println(err)
		}
		if len(ip) > 0 {
			hostIPMap[ip[0].String()] = node
		}
		statusList[node] = newStatus

	}

	go func(c *config.Config, statusList map[string]*Status) {
		for hostname, node := range statusList {
			if util.IsHostReachable(hostname, c.DialTimeout, c.DefaultPortList) {
				node.Status = UNKNOWN
			} else {
				node.Status = OFFLINE
			}
		}
	}(c, statusList)

	return &StatusHandler{
		StatusList: statusList,
		HostIPMap:  hostIPMap,
	}
}

func (handler *StatusHandler) HandleStatusMessage(msg *api.ResponseMessage, recvAddr *net.UDPAddr) {
	data := strings.Split(string(msg.Value), "\t\n\t\n")
	//TODO - update old status instead of creating new status
	t := time.Now()
	newStatus := &Status{
		UP,
		&t,
		strings.TrimSpace(data[0]),
		parseDiskSpace(strings.TrimSpace(data[1])),
		strings.TrimSpace(data[2]),
		strings.TrimSpace(data[3]),
	}

	handler.StatusList[handler.HostIPMap[recvAddr.IP.String()]] = newStatus
	go handler.CheckNodeReach()
}

func (handler *StatusHandler) CheckNodeReach() {
	for {
		now := time.Now()
		for node, status := range handler.StatusList {
			if status.LastSeen == nil {
				continue
			}
			if time.Now().Sub(*status.LastSeen) > config.GetConfig().NodeTimeout {
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
