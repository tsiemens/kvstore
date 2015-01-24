package commands

import "errors"
import "fmt"
import "encoding/hex"
import "github.com/tsiemens/kvstore/shared/api"

func New(cmdstr string) (cmd Command, err error) {
	switch cmdstr {
	case "get":
		cmd = newGetCommand()
		return
	default:
		err = errors.New("Unknown command \"" + cmdstr + "\"")
		return
	}
}

type Command interface {
	Name() string
	Desc() string
	Args() []string
	Run(url string, args []string) error
}

type BaseCommand struct {
	name string
	desc string
	args []string
}

func (c *BaseCommand) Name() string {
	return c.name
}

func (c *BaseCommand) Desc() string {
	return c.desc
}

func (c *BaseCommand) Args() []string {
	return c.args
}

type GetCommand struct {
	BaseCommand
}

func newGetCommand() *GetCommand {
	return &GetCommand{BaseCommand{
		name: "get",
		desc: "Gets the value for a key.",
		args: []string{"KEY (32 bytes, in hexadecimal)"},
	}}
}

func (c *GetCommand) Run(url string, args []string) error {
	if len(args) == 0 {
		return errors.New("get requires KEY argument")
	}

	keyslice, err := hex.DecodeString(args[0])
	if err != nil {
		return err
	}

	key, err := api.NewKey(keyslice)
	if err != nil {
		return err
	}

	fmt.Printf("Do get %x\n", key)
	return nil
}

func PrintCommands() {
	printCommandHelp(newGetCommand())
}

func printCommandHelp(cmd Command) {
	fmt.Printf("%s	%s\n", cmd.Name(), cmd.Desc())
	fmt.Println("	ARGS:")
	for _, arg := range cmd.Args() {
		fmt.Printf("		%s\n", arg)
	}
}
