package api

import (
	"net"
	"time"
)
import "github.com/tsiemens/kvstore/shared/log"
import "github.com/tsiemens/kvstore/shared/util"

const MaxMessageSize = 15000

var initialTimeout = 100
var retries = 3

type requestMessageBuilder func(addr *net.UDPAddr) *RequestMessage

/* Retrieves the value from the server at url,
 * using the kvstore protocol */
func Get(url string, key [32]byte) ([]byte, error) {
	return send(url, func(addr *net.UDPAddr) *RequestMessage {
		return newRequestMessage(addr, CmdGet, key, make([]byte, 0, 0))
	})
}

/* Sets the value on the server at url,
 * using the kvstore protocol */
func Put(url string, key [32]byte, value []byte) error {
	_, err := send(url, func(addr *net.UDPAddr) *RequestMessage {
		return newRequestMessage(addr, CmdPut, key, value)
	})
	return err
}

/* Removes the value from the server at url,
 * using the kvstore protocol */
func Remove(url string, key [32]byte) error {
	_, err := send(url, func(addr *net.UDPAddr) *RequestMessage {
		return newRequestMessage(addr, CmdRemove, key, make([]byte, 0, 0))
	})
	return err
}

func StatusUpdate(url string, key [32]byte, value []byte) error {
	_, err := send(url, func(addr *net.UDPAddr) *RequestMessage {
		return newRequestMessage(addr, CmdStatusUpdate, key, value)
	})
	return err
}

func send(url string, buildMsg requestMessageBuilder) ([]byte, error) {
	remoteAddr, err := net.ResolveUDPAddr("udp", url)
	if err != nil {
		return nil, err
	}

	con, localAddr, err := util.CreateUDPSocket(remoteAddr.IP.IsLoopback(), 0)
	if err != nil {
		return nil, err
	}
	defer con.Close()

	msgToSend := buildMsg(localAddr)
	receiver := &protocolReceiver{
		Conn:       con,
		RemoteAddr: remoteAddr,
		MsgUID:     msgToSend.UID,
	}

	// Try [retries] times to receive a message.
	// Timeout at [initialTimeout] ms, doubling the timeout after each retry
	timeout := initialTimeout
	var netErr net.Error
	for tries := retries; tries > 0; tries-- {
		// Send message/resend if timeout occurred
		con.WriteTo(msgToSend.Bytes(), remoteAddr)
		log.D.Printf("Sent: [% x]\n", msgToSend.Bytes())
		msg, err := receiver.recvMsg(timeout)
		netErr = err
		if netErr != nil {
			if netErr.Timeout() {
				timeout *= 2
			} else {
				// Some other error occured, which we won't recover from
				return nil, netErr
			}
		} else if msgErr := msg.Error(); msgErr != nil {
			return nil, msgErr
		} else {
			return msg.Value, nil
		}
	}

	// Timeout has occurred
	return nil, netErr
}

type protocolReceiver struct {
	Conn       *net.UDPConn
	RemoteAddr *net.UDPAddr
	MsgUID     [16]byte
}

// Attempts to receive the datagram, which must come from the correct ip/port,
// must be formatted correctly, and have the same UID originally sent.
// If timeout occurs, error returned will have .Timeout() == true
func (self *protocolReceiver) recvMsg(timeoutms int) (*ResponseMessage, net.Error) {
	buff := make([]byte, MaxMessageSize)
	for timeRemaining := timeoutms; timeRemaining > 0; {

		self.Conn.SetReadDeadline(
			time.Now().Add(time.Duration(timeRemaining) * time.Millisecond))

		startTime := time.Now()
		n, recvAddr, err := self.Conn.ReadFromUDP(buff)
		timeTaken := time.Since(startTime).Nanoseconds() / 1000000

		if err != nil {
			if netErr, ok := err.(net.Error); ok {
				return nil, netErr
			}
			log.E.Println(err)
		} else if recvAddr.IP.Equal(self.RemoteAddr.IP) &&
			recvAddr.Port == self.RemoteAddr.Port {

			log.D.Printf("Received [% x]\n", buff[0:60])
			serverMsg, err := parseResponseMessage(buff[0:n])
			if err == nil && serverMsg.UID == self.MsgUID {
				return serverMsg, nil
			}
			// Ignore malformatted messages, or ones not for our message
			log.D.Printf("Ignoring malformed message: %v", err)
		}

		timeRemaining -= int(timeTaken)
	}
	// Extra timeout to prevent locking up if repetedly getting invalid msgs
	return nil, TimeoutError("read udp, expecting from " + self.RemoteAddr.String())
}

type TimeoutError string

func (e TimeoutError) Error() string   { return "timeout: " + string(e) }
func (e TimeoutError) Temporary() bool { return false }
func (e TimeoutError) Timeout() bool   { return true }
