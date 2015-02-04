package exec

import "fmt"
import "sync"
import "strings"
import "os/exec"

var waitGroup sync.WaitGroup

func RunCommand(command string) (bool, string) {
	//var waitGroup sync.WaitGroup
	waitGroup.Add(1)
	return execute(command, &waitGroup)
}

func GetDiskSpace() (bool, string) {
	waitGroup.Add(1)
	return execute("df", &waitGroup)
}

func Uptime() (bool, string) {
	waitGroup.Add(1)
	return execute("uptime", &waitGroup)
}

func CurrentLoad() (bool, string) {
	waitGroup.Add(1)
	success, date := execute("uptime", &waitGroup)
	parts := strings.Fields(date)
	parts = parts[(len(parts) - 3):len(parts)]
	return success, string(parts[0] + " " + parts[1] + " " + parts[2])

}

func execute(cmd string, wg *sync.WaitGroup) (bool, string) {
	//function from http://stackoverflow.com/questions/20437336/how-to-execute-system-command-in-golang-with-unknown-arguments
	//	fmt.Println("command is ", cmd)
	// splitting head => g++ parts => rest of the command
	parts := strings.Fields(cmd)
	head := parts[0]
	parts = parts[1:len(parts)]

	success := true
	out, err := exec.Command(head, parts...).Output()
	if err != nil {
		fmt.Printf("%s", err)
		success = false
	}
	//fmt.Printf("%s", out)
	wg.Done() // Need to signal to waitgroup that this goroutine is done
	return success, string(out[:len(out)])
}
