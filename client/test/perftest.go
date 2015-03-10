package test

import (
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
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
