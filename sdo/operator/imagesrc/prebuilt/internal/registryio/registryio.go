// Package registryio owns the registry transport of the prebuilt image source:
// the guarded HTTP client, its address policy and the cache clock. It is a
// declared edge package because it opens sockets and reads the wall clock.
package registryio

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"time"
)

// Doer is the transport the registry resolver needs.
type Doer interface {
	Do(*http.Request) (*http.Response, error)
}

// ErrRefusedTarget marks a connection or redirect the guarded client will not make.
var ErrRefusedTarget = errors.New("registry target refused")

// Now is the registry cache clock.
func Now() time.Time { return time.Now() }

// ClientOptions configure the guarded registry client. Lookup and Dial are
// injectable so a test can prove the guard refuses a target, not merely that
// the connection failed.
type ClientOptions struct {
	Timeout time.Duration
	// AllowHost is the one host[:port] exempt from the public-address guard
	// (the operator's configured, possibly in-cluster, registry).
	AllowHost string
	// Lookup resolves a hostname; nil uses the system resolver.
	Lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	// Dial opens a connection; nil uses a plain dialer.
	Dial func(ctx context.Context, network, address string) (net.Conn, error)
}

// NewGuardedClient returns a bounded client that follows redirects only over
// HTTPS and connects only to public addresses. The check runs on the resolved
// address at connection time, so it covers every request the client makes:
// the first hop, redirects, token realms, and DNS names that resolve to
// loopback, private, link-local or cluster addresses.
func NewGuardedClient(options ClientOptions) Doer {
	lookup, dial := options.Lookup, options.Dial
	if lookup == nil {
		lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
			return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		}
	}
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			if address == options.AllowHost {
				return dial(ctx, network, address)
			}
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			addresses := []netip.Addr{}
			if literal, parseErr := netip.ParseAddr(host); parseErr == nil {
				addresses = append(addresses, literal)
			} else if addresses, err = lookup(ctx, host); err != nil {
				return nil, err
			}
			if len(addresses) == 0 {
				return nil, fmt.Errorf("%w: registry host %q has no address", ErrRefusedTarget, host)
			}
			for _, address := range addresses {
				if !PublicAddress(address) {
					return nil, fmt.Errorf("%w: registry host %q resolves to %s, which is not a public address", ErrRefusedTarget, host, address)
				}
			}
			return dial(ctx, network, net.JoinHostPort(addresses[0].String(), port))
		},
	}
	return &http.Client{Timeout: options.Timeout, Transport: transport, CheckRedirect: func(req *http.Request, via []*http.Request) error {
		if req.URL.Scheme != "https" && via[0].URL.Scheme != "http" {
			return fmt.Errorf("%w: registry redirect leaves HTTPS", ErrRefusedTarget)
		}
		if len(via) >= 5 {
			return errors.New("too many registry redirects")
		}
		return nil
	}}
}

// PublicAddress reports whether an address is routable on the public internet.
func PublicAddress(address netip.Addr) bool {
	address = address.Unmap()
	if !address.IsValid() || address.IsLoopback() || address.IsPrivate() || address.IsUnspecified() || address.IsLinkLocalUnicast() ||
		address.IsLinkLocalMulticast() || address.IsMulticast() || address.IsInterfaceLocalMulticast() {
		return false
	}
	return !netip.MustParsePrefix("100.64.0.0/10").Contains(address)
}
