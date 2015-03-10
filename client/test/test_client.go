package test

import (
	"crypto/rand"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"time"
)

type KeyValue struct {
	Key   [32]byte
	Value []byte
}

type StopWatch struct {
	StartTime time.Time
	StopTime  time.Time
}

func (s *StopWatch) Start() {
	s.StartTime = time.Now()
}

func (s *StopWatch) Stop() {
	s.StopTime = time.Now()
}

func (s *StopWatch) Restart() {
	s.StopTime = time.Now()
	s.StartTime = time.Now()
}

func (s *StopWatch) GetMilliseconds() time.Duration {
	return s.StopTime.Sub(s.StartTime)
}

func RunTestSuite(url string, SendCount int, shutdown bool) {
	log.I.Println("Running test suite")
	keyvals := make([]KeyValue, SendCount)
	for i := 0; i < SendCount; i++ {
		key, _ := api.NewRandKey()
		value := make([]byte, 40)
		_, _ = rand.Read(value)
		keyvals[i] = KeyValue{key, value}
	}

	log.I.Println("Running", SendCount, "synchronous PUT commands")
	duration, failures := ResponseTime(url, keyvals, api.CmdPut)
	log.I.Println("Finished. Duration:", duration, "Failures:", failures)

	log.I.Println("Running", SendCount, "synchronous GET commands")
	duration, failures = ResponseTime(url, keyvals, api.CmdGet)
	log.I.Println("Finished. Duration:", duration, "Failures:", failures)

	log.I.Println("Running", SendCount, "synchronous REMOVE commands")
	duration, failures = ResponseTime(url, keyvals, api.CmdRemove)
	log.I.Println("Finished. Duration:", duration, "Failures:", failures)

	log.I.Println("Running", SendCount, "asynchronous PUT commands")
	duration, failures = ThroughPut(url, keyvals, api.CmdPut)
	log.I.Println("Finished. Duration:", duration, "Failures:", failures)

	log.I.Println("Running", SendCount, "asynchronous GET commands")
	duration, failures = ThroughPut(url, keyvals, api.CmdGet)
	log.I.Println("Finished. Duration:", duration, "Failures:", failures)

	log.I.Println("Running", SendCount, "asynchronous REMOVE commands")
	duration, failures = ThroughPut(url, keyvals, api.CmdRemove)
	log.I.Println("Finished. Duration:", duration, "Failures:", failures)

	if shutdown == true {
		log.I.Println("Running synchronous Shutdown command")
		duration, failures = Shutdown(url, api.CmdShutdown)
		log.I.Println("Finished. Duration:", duration, "Failures:", failures)
	}

}
