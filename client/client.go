package main

import "flag"
import "fmt"
import "os"
import "strconv"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/dbg"

func main() {
	cl := getCommandLine()
	if cl.Debug {
		dbg.Enabled = true
	}

	var key [32]byte

	code, err := api.Get(cl.URL, key) // temporary to compile
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("Sending ID: %d\n"+
			"Secret code length: %d\n"+
			"Secret: %X\n",
			cl.StudentNo, len(code), code)
	}
}

type A1CommandLine struct {
	Debug     bool
	URL       string
	StudentNo int
}

func getCommandLine() *A1CommandLine {
	if flag.Parsed() {
		return nil
	}

	debugPtr := flag.Bool("debug", false, "Enable debug logging")
	helpPtr := flag.Bool("help", false, "Show help text")

	flag.Parse()

	if *helpPtr {
		printHelp()
		os.Exit(0)
	}

	args := flag.Args()
	if len(args) < 2 {
		fmt.Println("Expected \"host:port\" and \"sending id\" as arguments.")
		fmt.Println("Use -help for usage.")
		os.Exit(1)
	}

	studentNo, parseErr := strconv.ParseInt(args[1], 10, 32)
	if parseErr != nil {
		fmt.Println(parseErr)
		os.Exit(1)
	}

	return &A1CommandLine{
		Debug:     *debugPtr,
		URL:       args[0],
		StudentNo: int(studentNo),
	}
}

func printHelp() {
	fmt.Println("Gets the secret code for the given student number.\n")
	fmt.Println("Usage:\n    client [flags] host:port sending_id")
	fmt.Println("    eg. $ client 168.235.153.23:5627 909090\n")
	fmt.Println("Flags:")
	flag.PrintDefaults()
}
