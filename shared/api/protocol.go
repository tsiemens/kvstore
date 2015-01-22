package api

import (
	"bytes"
	"errors"
	"net"
	"time"
)
import "github.com/tsiemens/kvstore/shared/dbg"

var initialTimeout = 100
var retries = 3

/* Retrieves the value from the server at url,
 * using the kvstore protocol */
func GetCode(url string, id int) ([]byte, error) {
	remoteAddr, err := net.ResolveUDPAddr("udp", url)
	if err != nil {
		return nil, err
	}

	// Set up socket
	myIP, err := getMyIP()
	if err != nil {
		return nil, err
	}
	localAddr := &net.UDPAddr{IP: myIP}

	con, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, err
	}
	localAddr = con.LocalAddr().(*net.UDPAddr) // localAddr has port set now

	msgToSend := newClientMessage(localAddr, id)
	receiver := &protocolReceiver{
		Conn:       con,
		RemoteAddr: remoteAddr,
		MsgUID:     msgToSend.UID(),
	}

	// Try [retries] times to receive a message.
	// Timeout at [initialTimeout] ms, doubling the timeout after each retry
	timeout := initialTimeout
	var netErr net.Error
	var secretCode []byte
	for tries := retries; tries > 0; tries-- {
		// Send message/resend if timeout occurred
		con.WriteTo(msgToSend.ToBytes(), remoteAddr)
		dbg.Printf("Sent: %v\n", msgToSend.ToBytes())
		msg, err := receiver.recvMsg(timeout)
		netErr = err
		if netErr != nil {
			if netErr.Timeout() {
				timeout *= 2
			} else {
				break // Some other error occured, which we won't recover from
			}
		} else {
			secretCode = msg.Code
			break
		}
	}

	con.Close()
	return secretCode, netErr
}

func getMyIP() (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP, nil
			}
		}
	}
	return nil, errors.New("No IPv4 addresses found")
}

type protocolReceiver struct {
	Conn       *net.UDPConn
	RemoteAddr *net.UDPAddr
	MsgUID     []byte
}

// Attempts to receive the datagram, which must come from the correct ip/port,
// must be formatted correctly, and have the same UID originally sent.
// If timeout occurs, error returned will have .Timeout() == true
func (self *protocolReceiver) recvMsg(timeoutms int) (*ServerMessage, net.Error) {
	buff := make([]byte, 16000)
	for timeRemaining := timeoutms; timeRemaining > 0; {

		self.Conn.SetReadDeadline(
			time.Now().Add(time.Duration(timeRemaining) * time.Millisecond))

		startTime := time.Now()
		n, recvAddr, netErr := self.Conn.ReadFromUDP(buff)
		timeTaken := time.Since(startTime).Nanoseconds() / 1000000

		if netErr != nil {
			return nil, netErr.(net.Error)
		} else if recvAddr.IP.Equal(self.RemoteAddr.IP) &&
			recvAddr.Port == self.RemoteAddr.Port {

			dbg.Printf("Received [% x]\n", buff[0:60])
			serverMsg, err := parseServerMessage(buff[0:n])
			if err == nil && bytes.Equal(serverMsg.UID, self.MsgUID) {
				return serverMsg, nil
			}
			// Ignore malformatted messages, or ones not for our message
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
