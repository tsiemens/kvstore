package api

import (
	"errors"
	"github.com/tsiemens/kvstore/client/test"
	"github.com/tsiemens/kvstore/shared/api"
	"net"
	"strconv"
)

/* Retrieves the value from the server at url,
 * using the kvstore protocol */
func Get(url string, key [32]byte) ([]byte, error) {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdGet, key)
	})
	if err != nil {
		return nil, err
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return nil, cmdErr
	} else if vmsg, ok := msg.(*api.ValueDgram); ok {
		return vmsg.Value, nil
	} else {
		return nil, errors.New("Invalid dgram for get")
	}
}

/* Sets the value on the server at url,
 * using the kvstore protocol */
func Put(url string, key [32]byte, value []byte) error {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyValueDgram(api.NewMessageUID(addr), api.CmdPut, key, value)
	})
	if err != nil {
		return err
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return cmdErr
	} else {
		return nil
	}
}

/* Removes the value from the server at url,
 * using the kvstore protocol */
func Remove(url string, key [32]byte) error {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdRemove, key)
	})
	if err != nil {
		return err
	} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
		return cmdErr
	} else {
		return nil
	}
}

/* Runs a set of tests on the server at url,
 * using the kvstore protocol */
func Test(url string, args []string) error {
	sendCount := 100
	if len(args) > 0 {
		sendCount, _ = strconv.Atoi(args[0])
	}

	test.RunTestSuite(url, sendCount)
	//msg, err := api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
	//	return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdRemove, key)
	//})
	//if err != nil {
	//	return err
	//} else if cmdErr := api.ResponseError(msg); cmdErr != nil {
	//	return cmdErr
	//} else {
	//	return nil
	//}

	//run tests here

	//TODO move to test file once other tests are pushed

	//test at what point node faliures break the system

	var key [32]byte
	var addressList [2]string
	addressList[0] = "localhost:5555"
	addressList[1] = "localhost:5556"

	var i = 0
	for i = 0; i < len(addressList); i++ {
		err := api.Send(nil, addressList[i], func(addr *net.UDPAddr) api.Message {
			return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdShutdown, key)
		})
		if err != nil {
			return err
		} else {
			//return nil
			//do nothing, but progress to next loop iteration
		}
	}

	return nil
}
