package listener

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"regexp"
	"strings"
	"time"
)

// Config for the listener command
type Config struct {
	// ServerPropertiesPath specifies the path of a Kafka server.properties file.
	// The server.properties file will be parsed and used to derive the external listener port.
	ServerPropertiesPath string
	// Listener is the name of the Kafka external LISTENER
	Listener string

	InternalPort int
	// ReadTimeout is the maximum duration to wait for creating a connection to the nonce server
	ReadTimeout time.Duration

	// local bind addr, used for testing
	listenerAddrOverride string
}

// readAdvertisedListener will attempt to read a Kafka server.properties file and parse out
// the endpoint for the provided LISTENER name. Returns a hostport string.
func readAdvertisedListener(listenerName string, reader io.Reader) (string, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)
	advertisedListeners := ""
	for scanner.Scan() {
		line := scanner.Text()
		split := strings.SplitN(line, "=", 2)
		if split[0] == "advertised.listeners" {
			advertisedListeners = split[1]
			break
		}
	}

	if advertisedListeners == "" {
		return "", fmt.Errorf("advertised.listeners property not found")
	}

	allListeners := strings.SplitAfter(advertisedListeners, ",")
	re := regexp.MustCompile(`^(.*)://\[?([0-9a-zA-Z\-%._:]*)]?:(-?[0-9]+)`)
	for _, listener := range allListeners {
		subMatches := re.FindStringSubmatch(listener)
		if len(subMatches) == 4 && subMatches[1] == listenerName {
			port := subMatches[3]
			host := subMatches[2]
			if host == "" {
				return "", fmt.Errorf("invalid host string")
			}
			if port == "" {
				return "", fmt.Errorf("invalid port string")
			}
			hostport := net.JoinHostPort(host, port)

			return hostport, nil
		}
	}
	return "", fmt.Errorf("no valid advertised listeners were found")
}

func (c *Config) listenerAddr() (string, error) {
	if c.Listener != "" && c.ServerPropertiesPath != "" {
		fd, err := os.Open(c.ServerPropertiesPath)
		if err != nil {
			return "", fmt.Errorf("failed to open server properties file '%s': %v", c.ServerPropertiesPath, err)
		}
		defer fd.Close()

		hostport, err := readAdvertisedListener(c.Listener, fd)
		if err != nil {
			return "", fmt.Errorf("failed to read advertised listener '%s' from file '%s': %v", c.Listener, c.ServerPropertiesPath, err)
		}
		return hostport, nil
	}

	if c.listenerAddrOverride != "" {
		return c.listenerAddrOverride, nil
	}
	return "", fmt.Errorf("no listener endpoint provided")
}
