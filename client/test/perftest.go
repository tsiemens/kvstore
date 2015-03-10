package test

import (
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
	"strings"
	"sync"
	"time"
)

func ResponseTime(url string, keyvals []KeyValue, command byte) (time.Duration, int) {

	var totalDuration time.Duration
	failures := 0
	s := StopWatch{}
	var err error
	for _, keyval := range keyvals {
		s.Start()
		if command == api.CmdGet || command == api.CmdRemove {
			_, err = api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
				return api.NewKeyDgram(api.NewMessageUID(addr), command, keyval.Key)
			})
		} else {
			_, err = api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
				return api.NewKeyValueDgram(api.NewMessageUID(addr), command, keyval.Key, keyval.Value)
			})

		}
		s.Stop()

		totalDuration += s.GetMilliseconds()
		if err != nil {
			failures++
			log.I.Println(err)
		}

	}

	return totalDuration, failures
}

func ThroughPut(url string, keyvals []KeyValue, command byte) (time.Duration, int) {

	var wg sync.WaitGroup
	failures := 0
	s := StopWatch{}
	errchan := make(chan int, len(keyvals))

	s.Start()
	for _, keyval := range keyvals {
		wg.Add(1)
		go func(url string, keyval KeyValue, command byte, errchan chan int) {
			var err error
			defer wg.Done()
			if command == api.CmdGet || command == api.CmdRemove {
				_, err = api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
					return api.NewKeyDgram(api.NewMessageUID(addr), command, keyval.Key)
				})
			} else {
				_, err = api.SendRecv(url, func(addr *net.UDPAddr) api.Message {
					return api.NewKeyValueDgram(api.NewMessageUID(addr), command, keyval.Key, keyval.Value)
				})

			}
			if err != nil {
				errchan <- 1
			} else {
				errchan <- 0
			}
		}(url, keyval, command, errchan)
	}
	wg.Wait()
	for i := 0; i < len(keyvals); i++ {
		failures += <-errchan
	}
	s.Stop()

	return s.GetMilliseconds(), failures

}

func Shutdown(url string, command byte) (time.Duration, int) {
	var totalDuration time.Duration
	failures := 0
	s := StopWatch{}
	var err error

	var addressList [3]string

	switch strings.Split(url, ":")[0] {
	default:
		log.I.Println("Unknown cluster, aborting shutdown test")
		return s.GetMilliseconds(), failures
	case "kc-sce-plab2.umkc.edu",
		"planetlab1.cs.uml.edu",
		"plonk.cs.uwaterloo.ca":
		addressList[0] = "kc-sce-plab2.umkc.edu:5555"
		addressList[1] = "planetlab1.cs.uml.edu:5555"
		addressList[2] = "plonk.cs.uwaterloo.ca:5555"
	case "planetlab2.cs.purdue.edu",
		"planetlab1.koganei.itrc.net",
		"planetlab1.aut.ac.nz":
		addressList[0] = "planetlab2.cs.purdue.edu:5555"
		addressList[1] = "planetlab1.koganei.itrc.net:5555"
		addressList[2] = "planetlab1.aut.ac.nz:5555"

	}

	var key [32]byte
	var i = 0

	for i = 0; i < len(addressList); i++ {
		s.Start()
		err = api.Send(nil, addressList[i], func(addr *net.UDPAddr) api.Message {
			return api.NewKeyDgram(api.NewMessageUID(addr), api.CmdShutdown, key)
		})
		if err != nil {
			failures++
			log.I.Println(err)
		}

		s.Stop()
		totalDuration += s.GetMilliseconds()
	}

	return s.GetMilliseconds(), failures

}
