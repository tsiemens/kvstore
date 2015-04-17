package main

import (
	"flag"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/tsiemens/kvstore/server/config"
	"github.com/tsiemens/kvstore/server/handler"
	"github.com/tsiemens/kvstore/server/httpServer"
	"github.com/tsiemens/kvstore/server/loop"
	"github.com/tsiemens/kvstore/server/node"
	"github.com/tsiemens/kvstore/server/protocol"
	"github.com/tsiemens/kvstore/server/store"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
)

const version = "1.1.0"

func main() {
	log.Init(ioutil.Discard, os.Stdout, os.Stderr)

	cl := getCommandLine()
	if cl.Debug {
		log.Init(os.Stdout, os.Stdout, os.Stderr)
	}
	config.Init(cl.ConfigPath, cl.UseLoopback)

	var port int
	if cl.StatusServer {
		port = config.GetConfig().StatusServerPort
	} else {
		port = cl.Port
	}

	conn, localAddr, err := util.CreateUDPSocket(cl.UseLoopback, port)
	if err != nil {
		log.E.Panic(err)
	}

	defer conn.Close()
	log.Out.Printf("Started server on %s", localAddr.String())

	if cl.StatusServer {
		log.Out.Printf("Starting http server")
		statusHandler := handler.NewStatusHandler()
		go httpServer.CreateHttpServer(strconv.Itoa(config.GetConfig().StatusServerHttpPort), statusHandler)
		log.Out.Printf("Starting status receiver")
		err = protocol.StatusReceiver(conn, statusHandler)

	} else {
		store := store.New()
		node.Init(localAddr, conn, store, protocol.SendKeyValuesToNode)
		loop.GoAll()
		msgHandler := handler.NewDefaultMessageHandler(conn, cl.PacketLossPct)
		err = protocol.LoopReceiver(conn, msgHandler)
	}
	log.E.Fatal(err)
}

type ServerCommandLine struct {
	Debug         bool
	UseLoopback   bool
	PacketLossPct int
	Port          int
	StatusServer  bool
	ConfigPath    string
}

func getCommandLine() *ServerCommandLine {
	if flag.Parsed() {
		return nil
	}

	debugPtr := flag.Bool("debug", false, "Enable debug logging")
	hPtr := flag.Bool("h", false, "Show help text")
	helpPtr := flag.Bool("help", false, "Show help text")
	loopbackPtr := flag.Bool("loopback", false, "Host the server on localhost")
	portPtr := flag.Int("port", 5555, "Port to run server on.")
	packetLossPtr := flag.Int("lossy", 0, "This percent of packets will be randomly dropped.")
	configPathPtr := flag.String("config", "config.json", "Path to the config file")

	statusServerPtr := flag.Bool("statsrv", false, "Use this node as a status server")
	flag.Parse()

	if *helpPtr || *hPtr {
		printHelp()
		os.Exit(0)
	}

	return &ServerCommandLine{
		Debug:         *debugPtr,
		UseLoopback:   *loopbackPtr,
		PacketLossPct: *packetLossPtr,
		Port:          *portPtr,
		StatusServer:  *statusServerPtr,
		ConfigPath:    *configPathPtr,
	}
}

func printHelp() {
	log.Out.Println("Server for the KVStore Key Value Store.\n\n" +
		"Version: " + version + "\n" +
		"Usage:\n    server [OPTIONS]\n" +
		"    eg. $ server -debug\n\n" +
		"Flags:")
	flag.PrintDefaults()
}
