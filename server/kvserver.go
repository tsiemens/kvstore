package main

import "flag"
import "os"
import "io/ioutil"

import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/util"
import "github.com/tsiemens/kvstore/server/handler"
import "github.com/tsiemens/kvstore/server/store"
import "github.com/tsiemens/kvstore/server/config"

func main() {
	log.Init(ioutil.Discard, os.Stdout, os.Stderr)

	cl := getCommandLine()
	if cl.Debug {
		log.Init(os.Stdout, os.Stdout, os.Stderr)
	}
	config.Init(cl.ConfigPath, cl.UseLoopback)

	store := store.New()
	conn, localAddr, err := util.CreateUDPSocket(cl.UseLoopback, cl.Port)
	if err != nil {
		log.E.Panic(err)
	}
	defer conn.Close()
	log.Out.Printf("Started server on %s", localAddr.String())

	if cl.StatusServer {
		statusHandler := handler.NewStatusHandler()
		err = api.StatusReceiver(conn, statusHandler)
	} else {
		msgHandler := handler.NewMessageHandler(store, conn, cl.PacketLossPct)
		err = api.LoopReceiver(conn, msgHandler)
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
		"Usage:\n    server [OPTIONS]\n" +
		"    eg. $ server -debug\n\n" +
		"Flags:")
	flag.PrintDefaults()
}
