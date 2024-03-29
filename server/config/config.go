package config

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/shared/log"
	"net"
	"os"
	"strconv"
	"time"
)

// This package manages the server configuration
// TODO - handle errors when init not called

var config *Config

type Config struct {
	UseLoopback          bool
	NotifyCount          int          // number of nodes notified using the gossip protocol
	K                    int          // K factor in gossip protocol
	PeerList             []string     // hostnames of all other nodes in network
	DefaultLocalhostPort int          // default ports to communicate on
	StatusServer         string       // hostname of status server
	StatusServerAddr     *net.UDPAddr // addr of status server
	StatusServerPort     int
	StatusServerHttpPort int
	UpdateFrequency      time.Duration // how often the status server requests node updates
	NodeTimeout          time.Duration // how long a dial tried before timing out
	MembershipFrequency  time.Duration
	DialTimeout          time.Duration
	Hostname             string // this servers hostname
	MaxReplicas          int
}

func Init(configPath string, useloopback bool) {
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
	config.Hostname = hostname
	config.UseLoopback = useloopback
	// resolve status server addr
	if useloopback {
		config.StatusServer = "localhost"
	}
	addr, err := net.ResolveUDPAddr("udp", config.StatusServer+":"+strconv.Itoa(config.StatusServerPort))
	if err != nil {
		log.E.Printf("Error resolving status server:", err)
	}
	config.StatusServerAddr = addr
	log.D.Println(config.PeerList)
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
