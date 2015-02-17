package httpServer

import "net/http"
import "github.com/tsiemens/kvstore/server/handler"

func CreateHttpServer(port string, handler *handler.StatusHandler) {
	http.HandleFunc("/", handler.ServeHttp)
	http.ListenAndServe(":"+port, nil)
}
