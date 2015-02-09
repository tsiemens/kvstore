package exec

import "fmt"
import "sync"
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

func GetDeploymentDiskSpace() (bool, string) {
	waitGroup.Add(1)
	return execute("du -sh | grep -i -o -E '[[:alnum:]]+'", &waitGroup)
}

func Uptime() (bool, string) {
	waitGroup.Add(1)
	return execute("uptime | grep -i -o '.*users'", &waitGroup)
}

func CurrentLoad() (bool, string) {
	waitGroup.Add(1)
	return execute("uptime | awk -F'[a-z]:' '{ print $2}'", &waitGroup)

}

func execute(cmd string, wg *sync.WaitGroup) (bool, string) {
	//function originaly from http://stackoverflow.com/questions/20437336/how-to-execute-system-command-in-golang-with-unknown-arguments
	//altered to execute all commands through bash insted of directly.
	//	running commands through bash allows for piping output

	//	fmt.Println("command is ", cmd)
	success := true
	out, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		fmt.Printf("%s", err)
		success = false
	}
	//fmt.Printf("%s\n", out)	//view result of command
	wg.Done() // Need to signal to waitgroup that this goroutine is done
	return success, string(out[:len(out)])
}
