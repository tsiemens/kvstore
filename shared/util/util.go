package util

import "errors"
import "net"
import "time"

func CreateUDPSocket(loopback bool) (*net.UDPConn, *net.UDPAddr, error) {
	myIP, err := GetMyIP(loopback)
	if err != nil {
		return nil, nil, err
	}
	localAddr := &net.UDPAddr{IP: myIP}
	//localAddr := &net.UDPAddr{IP: myIP}

	con, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, nil, err
	}
	localAddr = con.LocalAddr().(*net.UDPAddr) // localAddr has port set now
	return con, localAddr, nil
}

func GetMyIP(loopback bool) (net.IP, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}

	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && ipnet.IP.To4() != nil {
			if loopback == ipnet.IP.IsLoopback() {
				return ipnet.IP, nil
			}
		}
	}
	return nil, errors.New("No IPv4 addresses found")
}

func UnixMilliTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
