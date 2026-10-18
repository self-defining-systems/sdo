package operatorcli

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"time"
)

// ProbeNetworkCanary checks one connection against the admission control's
// expected result. The allowed and denied Jobs use the same binary.
func ProbeNetworkCanary(ctx context.Context, address string, expectDenied bool, connect func(context.Context, string) error) error {
	if address == "" || connect == nil {
		return errors.New("network canary requires an address and connector")
	}
	err := connect(ctx, address)
	if expectDenied {
		if err == nil {
			return errors.New("default-deny network canary reached Kubernetes API")
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("allow network canary cannot reach Kubernetes API: %w", err)
	}
	return nil
}

func RunNetworkCanary(ctx context.Context, expectDenied bool) error {
	host := os.Getenv("KUBERNETES_SERVICE_HOST")
	if host == "" {
		return errors.New("network canary requires KUBERNETES_SERVICE_HOST")
	}
	port := os.Getenv("KUBERNETES_SERVICE_PORT_HTTPS")
	if port == "" {
		port = "443"
	}
	if _, err := strconv.Atoi(port); err != nil {
		return fmt.Errorf("invalid Kubernetes service port: %w", err)
	}
	address := net.JoinHostPort(host, port)
	connect := func(ctx context.Context, address string) error {
		var dialer net.Dialer
		connection, err := dialer.DialContext(ctx, "tcp", address)
		if err != nil {
			return err
		}
		return connection.Close()
	}
	// Give the network policy controller time to install the policy before the
	// denied canary is assessed. An allowed canary may need several retries.
	if err := waitCanary(ctx, 2*time.Second); err != nil {
		return err
	}
	for attempt := range 10 {
		probeCtx, cancel := context.WithTimeout(ctx, time.Second)
		err := ProbeNetworkCanary(probeCtx, address, expectDenied, connect)
		cancel()
		if err == nil {
			return nil
		}
		if expectDenied {
			return err
		}
		if attempt == 9 {
			return err
		}
		if err := waitCanary(ctx, 250*time.Millisecond); err != nil {
			return err
		}
	}
	return errors.New("network canary exhausted attempts")
}

func waitCanary(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
