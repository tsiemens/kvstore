package log

import (
	"io"
	"log"
	"os"
)

var (
	Out *log.Logger // stdout
	D   *log.Logger // debug
	I   *log.Logger // info
	E   *log.Logger // err
)

func Init(
	debugWriter io.Writer,
	infoWriter io.Writer,
	errorWriter io.Writer) {

	Out = log.New(os.Stdout, "", 0)
	D = log.New(debugWriter, "DEBUG: ",
		log.Ltime|log.Lmicroseconds|log.Lshortfile)

	I = log.New(infoWriter, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	E = log.New(errorWriter, "ERROR: ", log.Lshortfile)
}
