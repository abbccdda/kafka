package encodehost

import (
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

// This logic is intended to be moved to our common init container, once it's ready
// https://confluentinc.atlassian.net/wiki/spaces/~roger/pages/937957745/Init+Container+Plan
// https://github.com/confluentinc/confluent-platform-images/tree/master/components/init-container

func Encode(hostIp string) (string, error) {
	ipv4, err := parseIpv4(hostIp)
	if err != nil {
		return "", err
	}
	return encodeLastTwoOctetsHex(ipv4), nil
}

func parseIpv4(ipAddr string) (uint32, error) {
	ip := net.ParseIP(ipAddr)
	if ip == nil {
		return 0, errors.New(fmt.Sprintf("bad ipAddr format: %s", ipAddr))
	}
	ipv4 := ip.To4()
	if ipv4 == nil {
		return 0, errors.New(fmt.Sprintf("not a IPv4 address: %s", ipAddr))
	}
	return binary.BigEndian.Uint32(ipv4), nil
}

func encodeLastTwoOctetsHex(ipv4 uint32) string {
	return fmt.Sprintf("%02x", ipv4)[3:]
}
