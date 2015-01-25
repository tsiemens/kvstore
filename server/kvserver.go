package main

import "flag"
import "os"
import "io/ioutil"

import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/server/handler"

func main() {
	log.Init(ioutil.Discard, os.Stdout, os.Stderr)
	cl := getCommandLine()
	if cl.Debug {
		log.Init(os.Stdout, os.Stdout, os.Stderr)
	}

	exit, conn, localAddr, err := handler.StartHandler(cl.UseLoopback)
	if err != nil {
		log.E.Panic(err)
	}
	defer conn.Close()
	log.Out.Printf("Started server on %s", localAddr.String())
	<-exit
}

type ServerCommandLine struct {
	Debug       bool
	UseLoopback bool
}

func getCommandLine() *ServerCommandLine {
	if flag.Parsed() {
		return nil
	}

	debugPtr := flag.Bool("debug", false, "Enable debug logging")
	hPtr := flag.Bool("h", false, "Show help text")
	helpPtr := flag.Bool("help", false, "Show help text")
	loopbackPtr := flag.Bool("loopback", false, "Host the server on localhost")

	flag.Parse()

	if *helpPtr || *hPtr {
		printHelp()
		os.Exit(0)
	}

	return &ServerCommandLine{
		Debug:       *debugPtr,
		UseLoopback: *loopbackPtr,
	}
}

func printHelp() {
	log.Out.Println("Server for the KVStore Key Value Store.\n\n" +
		"Usage:\n    server [OPTIONS]\n" +
		"    eg. $ server -debug\n\n" +
		"Flags:")
	flag.PrintDefaults()
}
