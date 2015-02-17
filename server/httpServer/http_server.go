package httpServer

import "net/http"
import "github.com/tsiemens/kvstore/server/handler"
import "github.com/tsiemens/kvstore/shared/log"

func CreateHttpServer(port string, handler *handler.StatusHandler) {
	http.HandleFunc("/", handler.ServeHttp)
	log.Out.Println("Started http server on port " + port)
	http.ListenAndServe(":"+port, nil)
}
