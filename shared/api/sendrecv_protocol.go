package api

import (
	"errors"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"net"
	"time"
)

var initialTimeout = 100
var retries = 3

func ResponseError(msg Message) error {
	switch msg.Command() {
	case RespOk:
		return nil
	case RespInvalidKey:
		return errors.New("Non-existent key requested")
	case RespOutOfSpace:
		return errors.New("Response out of space")
	case RespSysOverload:
		return errors.New("System overload")
	case RespInternalError:
		return errors.New("Internal KVStore failure")
	case RespUnknownCommand:
		return errors.New("Unrecognized command")
	default:
		return nil
	}
}

func SendRecv(url string, buildMsg MessageBuilder) (Message, error) {

	remoteAddr, err := net.ResolveUDPAddr("udp", url)
	if err != nil {
		return nil, err
	}

	con, _, err := util.CreateUDPSocket(remoteAddr.IP.IsLoopback(), 0)
	if err != nil {
		return nil, err
	}
	defer con.Close()

	msgToSend := buildMsg(con.LocalAddr().(*net.UDPAddr))
	receiver := &protocolReceiver{
		Conn:       con,
		RemoteAddr: remoteAddr,
		MsgUID:     msgToSend.UID(),
	}

	// Try [retries] times to receive a message.
	// Timeout at [initialTimeout] ms, doubling the timeout after each retry
	timeout := initialTimeout
	var netErr net.Error
	for tries := retries; tries > 0; tries-- {
		// Send message/resend if timeout occurred
		con.WriteTo(msgToSend.Bytes(), remoteAddr)
		msg, err := receiver.recvMsg(timeout)
		netErr = err
		if netErr != nil {
			if netErr.Timeout() {
				timeout *= 2
			} else {
				// Some other error occured, which we won't recover from
				return nil, netErr
			}
		} else {
			return msg, nil
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
func (self *protocolReceiver) recvMsg(timeoutms int) (Message, net.Error) {
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
			serverMsg, err, _ := ParseMessage(buff[0:n], RespMessageParsers)
			if err == nil && serverMsg.UID() == self.MsgUID {
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
