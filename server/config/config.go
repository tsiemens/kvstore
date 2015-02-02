package config

import (
	"fmt"
	"net"
	"os"
)

// This package manages the server configuration

var config *Config

type Config struct {
	NotifyCount      int                     // number of nodes notified using the gossip protocol
	K                int                     // K factor in gossip protocol
	NodeAddrMap      map[string]*net.UDPAddr // temp - need some sort of structure to store all nodes
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
