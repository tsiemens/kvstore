package dbg

import "fmt"

var Enabled = false

const debugColor = "\x1b[36m"
const defaultColor = "\x1b[39;49m"

func Println(a ...interface{}) (n int, err error) {
	if Enabled {
		fmt.Print(debugColor)
		n, err = fmt.Println(a...)
		fmt.Print(defaultColor)
	}
	return
}

func Printf(format string, a ...interface{}) (n int, err error) {
	if Enabled {
		fmt.Print(debugColor)
		n, err = fmt.Printf(format, a...)
		fmt.Print(defaultColor)
	}
	return
}

func Print(a ...interface{}) (n int, err error) {
	if Enabled {
		fmt.Print(debugColor)
		n, err = fmt.Print(a...)
		fmt.Print(defaultColor)
	}
	return
}
