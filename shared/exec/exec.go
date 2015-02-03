package exec

import "fmt"
import "sync"
import "strings"
import "os/exec"

var waitGroup sync.WaitGroup

func runCommand(command string) string {
	//var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	return execute(command, &waitGroup)
}

func getDiskSpace() string {
	waitGroup.Add(1)
	return execute("df", &waitGroup)
}

func uptime() string {
	waitGroup.Add(1)
	return execute("uptime", &waitGroup)
}

func currentLoad() string {
	waitGroup.Add(1)
	var date = execute("uptime", &waitGroup)
	parts := strings.Fields(date)
	parts = parts[(len(parts) - 3):len(parts)]
	return string(parts[0] + " " + parts[1] + " " + parts[2])
}

func execute(cmd string, wg *sync.WaitGroup) string {
	//function from http://stackoverflow.com/questions/20437336/how-to-execute-system-command-in-golang-with-unknown-arguments
	//	fmt.Println("command is ", cmd)
	// splitting head => g++ parts => rest of the command
	parts := strings.Fields(cmd)
	head := parts[0]
	parts = parts[1:len(parts)]

	out, err := exec.Command(head, parts...).Output()
	if err != nil {
		fmt.Printf("%s", err)
	}
	//fmt.Printf("%s", out)
	wg.Done() // Need to signal to waitgroup that this goroutine is done
	return string(out[:len(out)])
}
