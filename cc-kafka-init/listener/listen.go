package listener

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
	"time"
)

type TcpNonceRoundTripper struct {
	config Config
	logger *log.Logger
	nonce  string
}

func NewTcpNonceRoundTripper(logger *log.Logger, config Config) *TcpNonceRoundTripper {
	nonce := strconv.Itoa(rand.Int())
	rt := TcpNonceRoundTripper{config: config, logger: logger, nonce: nonce}
	return &rt
}

func handle(logger *log.Logger, nonce string, conn io.WriteCloser) {
	defer conn.Close()
	_, err := conn.Write([]byte(fmt.Sprintf("%s\n", nonce)))
	if err != nil {
		logger.Printf("error writing response: %v\n", err)
		return
	}
}

func (rt *TcpNonceRoundTripper) bind(ctx context.Context, internalPort int) error {
	rt.logger.Printf("bound to %d\n", internalPort)
	listenAddr := fmt.Sprintf("%s:%d", "0.0.0.0", internalPort)
	addr, err := net.ResolveTCPAddr("tcp", listenAddr)
	rt.logger.Printf("listen %s\n", listenAddr)
	if err != nil {
		return err
	}
	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	closeOnce := sync.Once{}
	go func() {
		<-ctx.Done()
		rt.logger.Println("closing listener")

		closeOnce.Do(func() {
			_ = listener.SetDeadline(time.Now())
			_ = listener.Close()
		})
	}()

	for {
		select {
		case <-ctx.Done():
		default:
			conn, err := listener.AcceptTCP()
			if err != nil {
				rt.logger.Printf("error accepting: %v\n", err)
				continue
			}
			rt.logger.Printf("accepting new connection: %v\n", conn.RemoteAddr())
			_ = conn.SetLinger(0)
			go func() {
				handle(rt.logger, rt.nonce, conn)
			}()
		}
	}
}

func (rt *TcpNonceRoundTripper) readNonce() (string, error) {
	addr, err := rt.config.listenerAddr()
	if err != nil {
		return "", err
	}
	rt.logger.Printf("dialing %s", addr)
	conn, err := net.DialTimeout("tcp", addr, rt.config.ReadTimeout)
	if err != nil {
		return "", err
	}
	responseRead := bufio.NewReader(conn)
	response, prefix, err := responseRead.ReadLine()
	if err != nil {
		return "", err
	}
	if prefix {
		return "", nil
	}
	return string(response), nil
}

func (rt *TcpNonceRoundTripper) Run(ctx context.Context) error {
	wg := sync.WaitGroup{}
	serverErrCh := make(chan error, 1)
	clientCh := make(chan struct {
		success bool
		err     error
	}, 1)

	wg.Add(1)
	go func() {
		defer close(serverErrCh)
		defer wg.Done()
		rt.logger.Println("starting server")
		defer rt.logger.Println("stopped server")
		if err := rt.bind(ctx, rt.config.InternalPort); err != nil {
			serverErrCh <- err
		}
	}()

	wg.Add(1)
	go func() {
		defer close(clientCh)
		defer wg.Done()
		rt.logger.Println("starting reading nonce")
		defer rt.logger.Println("finished reading nonce")

		nonceMatched := false
		var lastErr error = nil

		timeout := time.Second * 0
	outer:
		for {
			// sleep for a bit at the start of the loop, its likely the server isn't up anyway
			select {
			case <-time.After(timeout):
			case <-ctx.Done():
				break
			}
			responseNonce, err := rt.readNonce()
			if err != nil {
				lastErr = fmt.Errorf("error reading response nonce: %v", err)
				break outer
			} else if responseNonce != rt.nonce {
				rt.logger.Printf(fmt.Sprintf("nonce mismatch: %v\n", lastErr))
				lastErr = fmt.Errorf("nonce response '%s' did not match expected '%s'", responseNonce, rt.nonce)
			} else {
				nonceMatched = true
				break outer
			}
			timeout = time.Second * 1
		}
		clientCh <- struct {
			success bool
			err     error
		}{
			success: nonceMatched,
			err:     lastErr,
		}
	}()

	select {
	case resp, ok := <-clientCh:
		if !ok {
			return fmt.Errorf("client chan closed before response could be read")
		} else if resp.err != nil {
			return fmt.Errorf("client encountered error: %v", resp.err)
		} else if !resp.success {
			return fmt.Errorf("response nonce was incorrect")
		} else {
			return nil
		}
	case err := <-serverErrCh:
		return err
	case <-ctx.Done():
		return fmt.Errorf("canceled")
	}
}
