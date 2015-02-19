package util

//import "github.com/tsiemens/kvstore/shared/log"

// From http://www.golangpatterns.info/concurrency/semaphores

type Semaphore chan bool

func NewSemaphore() Semaphore {
	return make(Semaphore, 1)
}

func (s Semaphore) Lock() {
	s <- true
}

func (s Semaphore) Unlock() {
	<-s
}
