package handler

import (
	"fmt"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
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
	NodeId           store.Key
	Hostname         string
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
}

func NewStatusHandler() *StatusHandler {
	statusList := make(map[string]*Status, 0)
	return &StatusHandler{
		StatusList: statusList,
	}
}

func (handler *StatusHandler) PruneStatusMap(peers map[store.Key]*node.Peer) {
	for host, status := range handler.StatusList {
		if _, ok := peers[status.NodeId]; !ok {
			delete(handler.StatusList, host)
		}
	}
}

func (handler *StatusHandler) UpdateStatusMap(peers map[store.Key]*node.Peer) {
	handler.PruneStatusMap(peers)
	for key, peer := range peers {
		if _, ok := handler.StatusList[peer.Addr.String()]; !ok {
			var st string
			if peer.Online {
				st = UP
			} else {
				st = OFFLINE
			}
			var hostname string
			hosts, err := net.LookupAddr(peer.Addr.IP.String())
			if err != nil {
				log.E.Println(err)
				hostname = peer.Addr.String()
			} else {
				hostname = fmt.Sprintf("%s:%d", hosts[0], peer.Addr.Port)
			}
			handler.StatusList[peer.Addr.String()] = &Status{
				key,
				hostname,
				st,
				&peer.LastSeen,
				"", /*space used by the application*/
				[]*DiskSpaceEntry{},
				"", /*uptime*/
				"", /*current load*/
			}
		}
	}
}

func (handler *StatusHandler) HandlePeerListUpdate(peers map[store.Key]*node.Peer) {
	handler.UpdateStatusMap(peers)
}

func (handler *StatusHandler) HandleStatusMessage(msg api.Message, recvAddr *net.UDPAddr) {
	status, ok := handler.StatusList[recvAddr.String()]
	if msg.Command() == api.RespStatusUpdateOK && ok {
		vMsg := msg.(*api.ValueDgram)
		data := strings.Split(string(vMsg.Value), "\t\n\t\n")
		t := time.Now()
		status.Status = UP
		status.LastSeen = &t
		status.ApplicationSpace = strings.TrimSpace(data[0])
		status.DiskSpace = parseDiskSpace(strings.TrimSpace(data[1]))
		status.Uptime = strings.TrimSpace(data[2])
		status.CurrentLoad = strings.TrimSpace(data[3])
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
