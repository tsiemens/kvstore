package debug

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"github.com/tsiemens/kvstore/shared/api"
	"github.com/tsiemens/kvstore/shared/log"
	"github.com/tsiemens/kvstore/shared/util"
	"net"
	"time"
)

type SendConfig struct {
	URL          string
	MsgType      string
	WaitForReply bool
	Value        string
	ValueLen     int
	Key          string
	UID          string
	Command      string
}

func Send(conf *SendConfig) {
	remoteAddr, err := net.ResolveUDPAddr("udp", conf.URL)
	if err != nil {
		log.E.Fatal(err)
	}

	con, _, err := util.CreateUDPSocket(remoteAddr.IP.IsLoopback(), 0)
	if err != nil {
		log.E.Fatal(err)
	}
	defer con.Close()

	msgToSend := createMsg(conf, con.LocalAddr().(*net.UDPAddr))

	log.I.Printf("Sending to %v: %x\n", remoteAddr, msgToSend)
	con.WriteTo(msgToSend, remoteAddr)

	if conf.WaitForReply {
		buff := make([]byte, api.MaxMessageSize)

		con.SetReadDeadline(
			time.Now().Add(time.Duration(3000) * time.Millisecond))

		n, _, err := con.ReadFromUDP(buff)
		if err != nil {
			log.E.Fatal(err)
		}
		log.I.Printf("Received: %x\n", buff[:n])
	}
}

func createMsg(conf *SendConfig, addr *net.UDPAddr) []byte {
	uid := createUID(conf.UID, addr)
	key, err := api.KeyFromHex(conf.Key)
	if err != nil {
		log.E.Fatal("Error parsing key: " + err.Error())
	}
	cmdslice, err := hex.DecodeString(conf.Command)
	if err != nil {
		log.E.Fatal("Error parsing command: " + err.Error())
	}
	cmd := cmdslice[0]

	buf := new(bytes.Buffer)
	buf.Write(uid[:])
	buf.WriteByte(cmd)

	switch conf.MsgType {
	case "k":
		buf.Write(key[:])
	case "kv":
		buf.Write(key[:])
		binary.Write(buf, binary.LittleEndian, int16(conf.ValueLen))
		buf.WriteString(conf.Value)
	case "v":
		binary.Write(buf, binary.LittleEndian, int16(conf.ValueLen))
		buf.WriteString(conf.Value)
	default:
		log.E.Fatal("Invalid msg type: " + conf.MsgType)
	}
	return buf.Bytes()
}

func createUID(uidHexStr string, addr *net.UDPAddr) [16]byte {
	if uidHexStr == "0000" {
		return api.NewMessageUID(addr)
	}

	uidslice, err := hex.DecodeString(uidHexStr)
	if err != nil {
		log.E.Fatal("Invalid UID:" + err.Error())
	}

	if len(uidslice) > 16 {
		log.E.Fatal("UID too large; max 16 bytes")
	}
	return api.ByteArray16(uidslice)
}
