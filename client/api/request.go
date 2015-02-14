package api

import "github.com/tsiemens/kvstore/shared/api"

/* Retrieves the value from the server at url,
 * using the kvstore protocol */
func Get(url string, key [32]byte) ([]byte, error) {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), CmdGet, key)
	})
	if err != nil {
		return nil, err
	} else if cmdErr := ResponseError(msg); cmdErr != nil {
		return msg, cmdErr
	} else if vmsg, ok := msg.(ValueDgram); ok {
		return vmsg.Value, nil
	} else {
		return nil, errors.New("Invalid dgram for get")
	}
}

/* Sets the value on the server at url,
 * using the kvstore protocol */
func Put(url string, key [32]byte, value []byte) error {
	msg, err := SendRecv(url, func(addr *net.UDPAddr) Message {
		return api.NewKeyValueDgram(api.NewMessageUID(addr), CmdPut, key, value)
	})
	if err != nil {
		return err
	} else if cmdErr := ResponseError(msg); cmdErr != nil {
		return cmdErr
	} else if vmsg, ok := msg.(BaseDgram); ok {
		return nil
	} else {
		return errors.New("Invalid dgram for put")
	}
}

/* Removes the value from the server at url,
 * using the kvstore protocol */
func Remove(url string, key [32]byte) error {
	msg, err := SendRecv(url, func(addr *net.UDPAddr) Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), CmdRemove, key)
	})
	if err != nil {
		return err
	} else if cmdErr := ResponseError(msg); cmdErr != nil {
		return cmdErr
	} else if vmsg, ok := msg.(BaseDgram); ok {
		return nil
	} else {
		return errors.New("Invalid dgram for put")
	}
}

func ResponseError(msg Message) error {
	switch msg.Command() {
	case RespOk:
		return nil
	case RespInvalidKey:
		return errors.New("Non-existent key requested")
	case RespOutOfSpace:
		return errors.New("Response out of space")
	case RespSysOverload:
		return errors.New("System overload")
	case RespInternalError:
		return errors.New("Internal KVStore failure")
	case RespUnknownCommand:
		return errors.New("Unrecognized command")
	default:
		return nil
	}
}
