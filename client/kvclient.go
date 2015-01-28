package main

import "flag"
import "os"
import "io/ioutil"

import "github.com/tsiemens/kvstore/client/commands"
import "github.com/tsiemens/kvstore/shared/log"

func main() {
	log.Init(ioutil.Discard, os.Stdout, os.Stderr)
	cl := getCommandLine()
	if cl.Debug {
		log.Init(os.Stdout, os.Stdout, os.Stderr)
	}

	err := cl.Command.Run(cl.URL, cl.Args)
	if err != nil {
		log.E.Println(err)
	}
}

type KVStoreCommandLine struct {
	Debug   bool
	URL     string
	Command commands.Command
	Args    []string
}

func getCommandLine() *KVStoreCommandLine {
	if flag.Parsed() {
		return nil
	}

	debugPtr := flag.Bool("debug", false, "Enable debug logging")
	hPtr := flag.Bool("h", false, "Show help text")
	helpPtr := flag.Bool("help", false, "Show help text")

	flag.Parse()

	if *helpPtr || *hPtr {
		printHelp()
		os.Exit(0)
	}

	args := flag.Args()
	if len(args) < 2 {
		log.E.Println("Expected \"COMMAND\" and \"HOST:PORT\" as arguments.\n" +
			"Use -help or -h for usage.")
		os.Exit(1)
	}

	cmd, err := commands.New(args[0])
	if err != nil {
		log.E.Fatal(err)
	}

	return &KVStoreCommandLine{
		Debug:   *debugPtr,
		URL:     args[1],
		Command: cmd,
		Args:    args[2:],
	}
}

func printHelp() {
	log.Out.Println("Client for the KVStore Key Value Store.\n\n" +
		"Usage:\n    client [OPTIONS] COMMAND HOST:PORT [ARGS...]\n" +
		"    eg. $ client get 168.235.153.23:5627 909090\n\n" +
		"Commands:")
	commands.PrintCommands()
	//"get	Gets the value for a key.\n" +
	//"		ARGS: KEY (32 bytes, in hexadecimal)\n" +
	//"put	Sets the value for a key.\n" +
	//"		ARGS: KEY (32 bytes, in hexadecimal)\n" +
	//"			  VALUE (Defaults to ascii. Other format flags may be added later)\n" +
	//"delete	Deletes the key value pair.\n" +
	//"		ARGS: KEY (32 bytes, in hexadecimal)\n\n" +
	log.Out.Println("\nFlags:")
	flag.PrintDefaults()
}
