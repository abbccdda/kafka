package listener

import (
	"bufio"
	"context"
	"io/ioutil"
	"log"
	"net"
	"testing"
	"time"
)

func Test_handle(t *testing.T) {
	logger := log.New(ioutil.Discard, "listen", 0)
	nonce := "world"
	client, server := net.Pipe()

	go handle(logger, nonce, server)

	reader := bufio.NewReader(client)

	bytes, isPrefix, err := reader.ReadLine()
	if err != nil {
		t.Error(err)
	}
	if isPrefix {
		t.Error("expected to read line")
	}

	response := string(bytes)
	if response != nonce {
		t.Errorf("expected response %s to equal %s", response, nonce)
	}
}

func Test_bind(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	//logger := log.NewNopLogger()
	logger := log.New(ioutil.Discard, "", 0)
	roundTripper := NewTcpNonceRoundTripper(logger, Config{
		ServerPropertiesPath: "",
		Listener:             "EXTERNAL",
		InternalPort:         9092,
		listenerAddrOverride: "localhost:9092",
	})

	go func() {
		err := roundTripper.bind(ctx, 9092)
		if err != nil {
			t.Error(err)
		}
	}()

	conn, err := net.Dial("tcp", roundTripper.config.listenerAddrOverride)
	if err != nil {
		t.Error(err)
	}
	line, pre, err := bufio.NewReader(conn).ReadLine()
	if err != nil {
		t.Error(err)
	}
	if pre {
		t.Error("did not expect prefix")
	}
	parsed := string(line)
	if parsed != roundTripper.nonce {
		t.Errorf("expected return value %s to equal nonce %s", parsed, roundTripper.nonce)
	}
}

func TestTcpNonceRoundTripper_Run(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger := log.New(ioutil.Discard, "", 0)
	roundTripper := NewTcpNonceRoundTripper(logger, Config{
		ServerPropertiesPath: "",
		Listener:             "",
		InternalPort:         9092,
		listenerAddrOverride: "localhost:9092",
	})

	err := roundTripper.Run(ctx)
	if err != nil {
		t.Error(err)
	}
}

func TestTcpNonceRoundTripper_Run_fail(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger := log.New(ioutil.Discard, "", 0)
	roundTripper := NewTcpNonceRoundTripper(logger, Config{
		ServerPropertiesPath: "",
		Listener:             "",
		InternalPort:         9093,
		ReadTimeout:          1 * time.Millisecond,
		listenerAddrOverride: "localhost:9092",
	})

	err := roundTripper.Run(ctx)
	if err == nil {
		t.Error("expected roundTripper to fail")
	}
}
