package httpServer

import "net/http"
import "strconv"
import "strings"
import "github.com/tsiemens/kvstore/server/handler"

//The location in the button will want to be updated to the actual address of the server we are running, it
//is set to the localhost for testing purposes at the moment.
var updateButton = "<input type=\"button\" onclick=\"location.href('http://localhost:8080/status');\" value=\"Update\">"

func CreateHttpServer() {
	http.HandleFunc("/status", HttpStatusHandler)
	http.HandleFunc("/", HttpDisplayHandler)
	http.ListenAndServe("localhost:8080", nil)
}

func HttpStatusHandler(writer http.ResponseWriter, req *http.Request) {
	statusList, err := handler.RequestStatusUpdate()
	statusTable := "<table>"
	if err != nil {
		writer.Write([]byte("<html><body><p>An error has occurred. Please try again</p></body></html>"))
	}
	
	for i,status := range statusList {
		statusTable = statusTable + "<tr><td>Node</td><td>" + strconv.Itoa(i) + "</td></tr>"
		statusTable = statusTable + "<tr><td>Deployment Space</td><td>" + status.ApplicationSpace + "</td></tr>"
		statusTable = statusTable + CreateDiskSpaceTable(status.DiskSpace)
		statusTable = statusTable + "<tr><td>Uptime</td><td>" + status.Uptime + "</td></tr>"
		statusTable = statusTable + "<tr><td>Current Load</td><td>" + status.CurrentLoad + "</td></tr>"
	}
	statusTable = statusTable + "</table>" 
	writer.Write([]byte("<html><body>" + updateButton + statusTable + updateButton + "</body></html>"))
}

func HttpDisplayHandler(writer http.ResponseWriter, req *http.Request) {
	writer.Write([]byte("<html><body><p>To view the status, please press the update button</p>" + updateButton + "</body></html>"))
}

// The diskspace output has to be formatted or we just get as one long string, so it is just put it
// into another table
func CreateDiskSpaceTable(diskStatus string) string {
	diskSpaceInfo := strings.Fields(diskStatus)
	diskSpaceTable := "<tr><td>Disk Space</td><td><table>"
	
	if len(diskSpaceInfo) < 7 {
		return ""
	}
	tableInfo := diskSpaceInfo[7:]

	//The mounted on field messes up splitting the string by white space, so the first
	//row is created manually
	diskSpaceTable = diskSpaceTable + "<tr><td>" + diskSpaceInfo[0] + "</td><td>" + 
		diskSpaceInfo[1] + "</td><td>" + diskSpaceInfo[2] + "</td><td>" + diskSpaceInfo[3] + 
		"</td><td>" + diskSpaceInfo[4] + "</td><td>" + diskSpaceInfo[5] + " " + diskSpaceInfo[6] + 
		"</td></tr>"	

	for k,diskspaceData := range tableInfo {
		if k % 6 == 0 {
			diskSpaceTable = diskSpaceTable + "<tr>"
		}
		
		diskSpaceTable = diskSpaceTable + "<td>" + diskspaceData + "</td>"	
		
		if k % 6 == 5 {
			diskSpaceTable = diskSpaceTable + "</tr>"
		}
	}
	diskSpaceTable = diskSpaceTable + "</table></td></tr>"
	return diskSpaceTable
}
