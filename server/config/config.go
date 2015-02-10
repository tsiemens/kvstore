package config

import (
	"encoding/json"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"math/rand"
	"net"
	"os"
	"strconv"
	"time"
)

// This package manages the server configuration
// TODO - handle errors when init not called

var config *Config
var useLoopback bool

type Config struct {
	NotifyCount          int          // number of nodes notified using the gossip protocol
	K                    int          // K factor in gossip protocol
	PeerList             []string     // hostnames of all other nodes in network
	DefaultPortList      []string     // default ports to communicate on
	StatusServer         string       // hostname of status server
	StatusServerAddr     *net.UDPAddr // addr of status server
	StatusServerPort     int
	StatusServerHttpPort int
	UpdateFrequency      time.Duration // how often the status server requests node updates
	NodeTimeout          time.Duration // how long a dial tried before timing out
	DialTimeout          time.Duration
	Hostname             string // this servers hostname
}

func Init(configPath string, useloopback bool) {
	useLoopback = useloopback
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

	// resolve status server addr
	if useLoopback {
		config.StatusServer = "localhost"
	}
	addr, err := net.ResolveUDPAddr("udp", config.StatusServer+":"+strconv.Itoa(config.StatusServerPort))
	if err != nil {
		log.E.Printf("Error resolving status server:", err)
	}
	config.StatusServerAddr = addr
	log.D.Println(config.PeerList)
}

func (c *Config) GetRandAddr() string {
	rand.New(rand.NewSource(util.UnixMilliTimestamp()))
	if useLoopback {
		basePort, _ := strconv.Atoi(c.DefaultPortList[0])
		port := rand.Intn(len(c.PeerList)) + basePort
		addr := strconv.Itoa(port)
		return "localhost:" + addr
	}
	randHost := c.PeerList[rand.Intn(len(c.PeerList))]
	// prevent host from picking itself
	for randHost == c.Hostname {
		randHost = c.PeerList[rand.Intn(len(c.PeerList))]
	}

	return randHost + ":" + c.DefaultPortList[0]
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
