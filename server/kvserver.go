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
	config.Init()

	cl := getCommandLine()
	if cl.Debug {
		log.Init(os.Stdout, os.Stdout, os.Stderr)
	}

	store := store.New()
	conn, localAddr, err := util.CreateUDPSocket(cl.UseLoopback, cl.Port)
	if err != nil {
		log.E.Panic(err)
	}
	defer conn.Close()
	log.Out.Printf("Started server on %s", localAddr.String())

	msgHandler := handler.New(store, conn, cl.PacketLossPct)
	err = api.LoopReceiver(conn, msgHandler)
	log.E.Fatal(err)
}

type ServerCommandLine struct {
	Debug         bool
	UseLoopback   bool
	PacketLossPct int
	Port          int
}

func getCommandLine() *ServerCommandLine {
	if flag.Parsed() {
		return nil
	}

	debugPtr := flag.Bool("debug", false, "Enable debug logging")
	hPtr := flag.Bool("h", false, "Show help text")
	helpPtr := flag.Bool("help", false, "Show help text")
	loopbackPtr := flag.Bool("loopback", false, "Host the server on localhost")
	portPtr := flag.Int("port", 0, "Port to run server on.")
	packetLossPtr := flag.Int("lossy", 0, "This percent of packets will be randomly dropped.")

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
	}
}

func printHelp() {
	log.Out.Println("Server for the KVStore Key Value Store.\n\n" +
		"Usage:\n    server [OPTIONS]\n" +
		"    eg. $ server -debug\n\n" +
		"Flags:")
	flag.PrintDefaults()
}
