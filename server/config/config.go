package config

import (
	"fmt"
	"net"
	"os"
)

var config *Config

type Config struct {
	ShouldRelay      bool
	NotifyCount      int
	K                int
	NodeAddrMap      map[string]*net.UDPAddr
	StatusServerAddr *net.UDPAddr
}

func Init() {
	//TODO implement this properly
	addr, err := net.ResolveUDPAddr("udp", ":5066")
	if err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
	config = &Config{
		ShouldRelay:      true,
		NotifyCount:      2,
		K:                8,
		StatusServerAddr: addr,
	}
}

func GetRandAddr() *net.UDPAddr {
	//TODO return addr of random peer in network
	addr, _ := net.ResolveUDPAddr("udp", ":5066")
	return addr
}

func GetConfig() *Config {
	return config
}
