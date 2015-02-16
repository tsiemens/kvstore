package api

import (
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"net"
	"time"
)

const MaxMessageSize = 15000
const TimeoutLength = time.Millisecond * 3000

type MessageBuilder func(addr *net.UDPAddr) Message

func StatusUpdate(conn *net.UDPConn, url string, key [32]byte) error {
	log.D.Println("Sending status update to", url)
	return Send(conn, url, func(addr *net.UDPAddr) Message {
		return NewKeyValueDgram(NewMessageUID(addr), CmdStatusUpdate, key, make([]byte, 0, 0))
	})
}

func AdhocUpdate(conn *net.UDPConn, url string, key [32]byte, value []byte) error {
	return Send(conn, url, func(addr *net.UDPAddr) Message {
		return NewKeyValueDgram(NewMessageUID(addr), CmdAdhocUpdate, key, value)
	})
}

// Send to url, via conn, a UDP packet out of the message produced
// by buildMsg.
// Creates a new random socket if conn is nil
// Returns an error if the host is not available
func Send(conn *net.UDPConn, url string, buildMsg MessageBuilder) error {
	_, err := net.DialTimeout("udp", url, TimeoutLength)
	if err != nil {
		return err
	}
	remoteAddr, err := net.ResolveUDPAddr("udp", url)
	if err != nil {
		return err
	}
	if conn == nil {
		conn, _, err = util.CreateUDPSocket(remoteAddr.IP.IsLoopback(), 0)
		if err != nil {
			return err
		}
	}

	msgToSend := buildMsg(conn.LocalAddr().(*net.UDPAddr))
	log.D.Println("Sending msg to " + remoteAddr.String())
	//log.D.Printf("Sending: % x\n", msgToSend.Bytes())
	conn.WriteTo(msgToSend.Bytes(), remoteAddr)
	return nil
}

type TimeoutError string

func (e TimeoutError) Error() string   { return "timeout: " + string(e) }
func (e TimeoutError) Temporary() bool { return false }
func (e TimeoutError) Timeout() bool   { return true }
