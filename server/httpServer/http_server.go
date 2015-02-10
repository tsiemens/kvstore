package httpServer

import "net/http"
import "github.com/tsiemens/kvstore/server/handler"

//import "github.com/tsiemens/kvstore/shared/log"
//import "html/template"

func CreateHttpServer(port string, handler *handler.StatusHandler) {
	http.HandleFunc("/", handler.ServeHttp)
	http.ListenAndServe(":"+port, nil)
}

/*
func HttpStatusHandler(writer http.ResponseWriter, req *http.Request) {

	t := template.New("status.html")
	t, err := t.ParseFiles("templates/status.html")
	if err != nil {
		log.E.Println(err)
	}
	statusList := handler.GetStatusList()
	err = t.Execute(writer, statusList)
	if err != nil {
		log.E.Println(err)
	}

}
*/
