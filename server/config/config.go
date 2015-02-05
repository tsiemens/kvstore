package config

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/shared/log"
	"math/rand"
	"net"
	"os"
	"time"
)

// This package manages the server configuration
// TODO - handle errors when init not called

var config *Config

type Config struct {
	NotifyCount      int      // number of nodes notified using the gossip protocol
	K                int      // K factor in gossip protocol
	NodeAddrList     []string // temp - need some sort of structure to store all nodes
	PortList         []string
	StatusServerAddr *net.UDPAddr
	UpdateFrequency  time.Duration
	HostName         string
}

func Init(configPath string) {

	// Check if configPath is valid
	if ok, err := exists(configPath); !ok {
		if err == nil {
			log.E.Println("Configuration file not found. Please pass in the correct path in the command line arguments")
			os.Exit(1)
		} else {
			log.E.Println(err)
		}
	}

	// decode and unmarshal
	file, err := os.Open(configPath)
	if err != nil {
		log.E.Println(err)
		os.Exit(1)
	}
	decoder := json.NewDecoder(file)
	config = &Config{}
	err = decoder.Decode(config)
	if err != nil {
		log.E.Println(err)
	}

	// get host name
	hostname, err := os.Hostname()
	if err != nil {
		log.E.Printf("Error getting hostname:", err)
	}
	config.HostName = hostname

	log.D.Println(config)
}

func (c *Config) GetRandAddr() string {
	randHost := c.NodeAddrList[rand.Intn(len(c.NodeAddrList))]
	// prevent host from picking itself
	for randHost == c.HostName {
		randHost = c.NodeAddrList[rand.Intn(len(c.NodeAddrList))]
	}

	return randHost + ":" + c.PortList[0]
}

func GetConfig() *Config {
	return config
}

// Taken from github: http://stackoverflow.com/questions/10510691/how-to-check-whether-a-file-or-directory-denoted-by-a-path-exists-in-golang
// exists returns whether the given file or directory exists or not
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
