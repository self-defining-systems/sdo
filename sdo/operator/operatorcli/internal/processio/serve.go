package processio

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"
	"os/signal"
	"syscall"
	"time"
)

// DialReady reports whether address accepts a TCP connection.
func DialReady(address string) error {
	c, err := net.DialTimeout("tcp", address, time.Second)
	if err == nil {
		c.Close()
	}
	return err
}

// ServeHTTP serves handler on address until ctx ends or the process is told to
// stop. A graceful stop returns nil.
func ServeHTTP(ctx context.Context, address string, handler http.Handler) error {
	server := &http.Server{Addr: address, Handler: handler, ReadHeaderTimeout: 10 * time.Second}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			server.Close()
		case <-done:
		}
	}()
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// NewTransport builds the pooled transport an incident's API proxy shares.
func NewTransport(tlsConfig *tls.Config) *http.Transport {
	return &http.Transport{TLSClientConfig: tlsConfig, DisableCompression: true, MaxIdleConnsPerHost: 32, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ResponseHeaderTimeout: 60 * time.Second}
}
