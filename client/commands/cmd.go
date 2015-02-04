package commands

import "errors"
import "encoding/hex"
import "github.com/tsiemens/kvstore/shared/api"
import "github.com/tsiemens/kvstore/shared/log"
import "crypto/rand"

func New(cmdstr string) (cmd Command, err error) {
	switch cmdstr {
	case "get":
		cmd = newGetCommand()
	case "put":
		cmd = newPutCommand()
	case "remove":
		cmd = newRemoveCommand()
	case "status":
		cmd = newStatusUpdateCommand()
	case "adhoc":
		cmd = newAdhocUpdateCommand()
	default:
		err = errors.New("Unknown command \"" + cmdstr + "\"")
	}
	return
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

	log.Out.Println("Retreived:")
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

type RemoveCommand struct {
	BaseCommand
}

func newRemoveCommand() *RemoveCommand {
	return &RemoveCommand{BaseCommand{
		name: "remove",
		desc: "Deletes the value for a key.",
		args: []string{"KEY (32 bytes, in hexadecimal)"},
	}}
}

func (c *RemoveCommand) Run(url string, args []string) error {
	if len(args) == 0 {
		return errors.New("remove requires KEY argument")
	}

	key, err := keyFromHex(args[0])
	if err != nil {
		return err
	}

	err = api.Remove(url, key)
	if err != nil {
		return err
	}

	log.Out.Printf("Deleted %x\n", key)
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

type StatusUpdateCommand struct {
	BaseCommand
}

func newStatusUpdateCommand() *StatusUpdateCommand {
	return &StatusUpdateCommand{BaseCommand{
		name: "status update",
		desc: "Sends a script to a node.",
		args: []string{"KEY (32 bytes, in hexadecimal)",
			"VALUE (bash script)"},
	}}
}

func (c *StatusUpdateCommand) Run(url string, args []string) error {
	k := make([]byte, 32)
	_, err := rand.Read(k)
	if err != nil {
		return err
	}
	key, err := api.NewKey(k)
	if err != nil {
		return err
	}

	err = api.StatusUpdate(url, key)
	if err != nil {
		return err
	}

	//log.Out.Printf("Set value of %x to %s\n", key, value)
	return nil
}

type AdhocUpdateCommand struct {
	BaseCommand
}

func newAdhocUpdateCommand() *AdhocUpdateCommand {
	return &AdhocUpdateCommand{BaseCommand{
		name: "adhoc update",
		desc: "Sends an adhoc script to a node to be executed.",
		args: []string{"VALUE (bash script)"},
	}}
}

func (c *AdhocUpdateCommand) Run(url string, args []string) error {
	if len(args) == 0 {
		return errors.New("status update requires VALUE argument")
	}
	k := make([]byte, 32)
	_, err := rand.Read(k)
	if err != nil {
		return err
	}
	key, err := api.NewKey(k)
	if err != nil {
		return err
	}

	value := args[0]
	err = api.AdhocUpdate(url, key, []byte(value))
	if err != nil {
		return err
	}

	//log.Out.Printf("Set value of %x to %s\n", key, value)
	return nil
}
