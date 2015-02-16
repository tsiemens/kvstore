package api

import "github.com/tsiemens/kvstore/shared/api"

var initialTimeout = 100
var retries = 3

/* Retrieves the value from the server at url,
 * using the kvstore protocol */
func Get(url string, key [32]byte) ([]byte, error) {
	msg, err := api.SendRecv(url, func(addr *net.UDPAddr) Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), CmdGet, key)
	})
	if err != nil {
		return nil, err
	} else if cmdErr := ResponseError(msg); cmdErr != nil {
		return msg, cmdErr
	} else if vmsg, ok := msg.(ValueDgram); ok {
		return vmsg.Value, nil
	} else {
		return nil, errors.New("Invalid dgram for get")
	}
}

/* Sets the value on the server at url,
 * using the kvstore protocol */
func Put(url string, key [32]byte, value []byte) error {
	msg, err := SendRecv(url, func(addr *net.UDPAddr) Message {
		return api.NewKeyValueDgram(api.NewMessageUID(addr), CmdPut, key, value)
	})
	if err != nil {
		return err
	} else if cmdErr := ResponseError(msg); cmdErr != nil {
		return cmdErr
	} else if vmsg, ok := msg.(BaseDgram); ok {
		return nil
	} else {
		return errors.New("Invalid dgram for put")
	}
}

/* Removes the value from the server at url,
 * using the kvstore protocol */
func Remove(url string, key [32]byte) error {
	msg, err := SendRecv(url, func(addr *net.UDPAddr) Message {
		return api.NewKeyDgram(api.NewMessageUID(addr), CmdRemove, key)
	})
	if err != nil {
		return err
	} else if cmdErr := ResponseError(msg); cmdErr != nil {
		return cmdErr
	} else if vmsg, ok := msg.(BaseDgram); ok {
		return nil
	} else {
		return errors.New("Invalid dgram for put")
	}
}

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
func (self *protocolReceiver) recvMsg(timeoutms int) (*ResponseMessage, net.Error) {
	buff := make([]byte, api.MaxMessageSize)
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

			//log.D.Printf("Received [% x]\n", buff[0:60])
			serverMsg, err := api.ParseMessage(buff[0:n], MessageParsers)
			if err == nil && serverMsg.UID == self.MsgUID {
				return serverMsg, nil
			}
			// Ignore malformatted messages, or ones not for our message
			log.D.Printf("Ignoring malformed message: %v", err)
		}

		timeRemaining -= int(timeTaken)
	}
	// Extra timeout to prevent locking up if repetedly getting invalid msgs
	return nil, api.TimeoutError("read udp, expecting from " + self.RemoteAddr.String())
}
