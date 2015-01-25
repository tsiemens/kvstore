package commands

import "errors"
import "encoding/hex"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"

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

	key, err := keyFromHex(args[0])
	if err != nil {
		return err
	}

	val, err := api.Get(url, key)
	if err != nil {
		return err
	}

	log.Out.Println(string(val))
	return nil
}

func keyFromHex(keystring string) (key [32]byte, err error) {
	keyslice, err := hex.DecodeString(keystring)
	if err != nil {
		return
	}

	key, err = api.NewKey(keyslice)
	return
}

type PutCommand struct {
	BaseCommand
}

func newPutCommand() *PutCommand {
	return &PutCommand{BaseCommand{
		name: "put",
		desc: "Sets the value for a key.",
		args: []string{"KEY (32 bytes, in hexadecimal)",
			"VALUE (Defaults to ascii. Other format flags may be added later)"},
	}}
}

func (c *PutCommand) Run(url string, args []string) error {
	if len(args) < 2 {
		return errors.New("put requires KEY and VALUE arguments")
	}

	key, err := keyFromHex(args[0])
	if err != nil {
		return err
	}

	value := args[1]
	err = api.Put(url, key, []byte(value))
	if err != nil {
		return err
	}

	log.Out.Printf("Set value of %x to %s\n", key, value)
	return nil
}

func PrintCommands() {
	printCommandHelp(newGetCommand())
	printCommandHelp(newPutCommand())
}

func printCommandHelp(cmd Command) {
	log.Out.Printf("%s	%s\n", cmd.Name(), cmd.Desc())
	log.Out.Println("	ARGS:")
	for _, arg := range cmd.Args() {
		log.Out.Printf("		%s\n", arg)
	}
}
