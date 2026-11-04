package deploymentio

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"testing"
)

func lookupTable(table map[string]string) func(context.Context, string) ([]netip.Addr, error) {
	return func(_ context.Context, host string) ([]netip.Addr, error) {
		address, ok := table[host]
		if !ok {
			return nil, errors.New("no such host")
		}
		return []netip.Addr{netip.MustParseAddr(address)}, nil
	}
}

// The guard is proven by a dialer that fails the test when it is reached for a
// refused address: removing the check fails the test instead of merely
// producing a connection error.
func TestGuardedClientNeverDialsNonPublicAddresses(t *testing.T) {
	lookup := lookupTable(map[string]string{
		"internal.example": "10.0.0.5", "metadata.example": "169.254.169.254", "loop.example": "127.0.0.1",
		"cgnat.example": "100.64.1.1", "v6.example": "fd00::1", "mapped.example": "::ffff:10.0.0.5", "public.example": "203.0.113.7",
	})
	var dialed []string
	client := NewGuardedClient(ClientOptions{Lookup: lookup, Dial: func(_ context.Context, _, address string) (net.Conn, error) {
		dialed = append(dialed, address)
		return nil, errors.New("stub dial")
	}})
	for _, target := range []string{
		"http://10.0.0.5/", "http://127.0.0.1:5000/", "http://[::1]:5000/", "http://[fd00::1]/",
		"http://internal.example/", "http://metadata.example/", "http://loop.example/", "http://cgnat.example/", "http://v6.example/", "http://mapped.example/",
	} {
		request, _ := http.NewRequest(http.MethodGet, target, nil)
		if _, err := client.Do(request); !errors.Is(err, ErrRefusedTarget) {
			t.Fatalf("%s: want a refused target, got %v", target, err)
		}
	}
	if len(dialed) != 0 {
		t.Fatalf("dialed refused addresses: %v", dialed)
	}
	request, _ := http.NewRequest(http.MethodGet, "http://public.example:80/", nil)
	if _, err := client.Do(request); errors.Is(err, ErrRefusedTarget) || len(dialed) != 1 || dialed[0] != "203.0.113.7:80" {
		t.Fatalf("a public name must be dialed at its resolved address: %v %v", dialed, err)
	}
}

// A registry that redirects to an internal address is refused at the redirect.
func TestGuardedClientRefusesARedirectToAnInternalHost(t *testing.T) {
	var dialed []string
	client := NewGuardedClient(ClientOptions{
		Lookup: lookupTable(map[string]string{"registry.example": "203.0.113.7", "internal.example": "10.0.0.5"}),
		Dial: func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = append(dialed, address)
			if address != "203.0.113.7:80" {
				t.Errorf("dialed %s", address)
				return nil, errors.New("unexpected dial")
			}
			client, server := net.Pipe()
			go func() {
				defer func() { _ = server.Close() }()
				if _, err := http.ReadRequest(bufio.NewReader(server)); err == nil {
					_, _ = io.WriteString(server, "HTTP/1.1 302 Found\r\nLocation: http://internal.example/secret\r\nContent-Length: 0\r\n\r\n")
				}
			}()
			return client, nil
		},
	})
	request, _ := http.NewRequest(http.MethodGet, "http://registry.example/v2/", nil)
	_, err := client.Do(request)
	if !errors.Is(err, ErrRefusedTarget) || len(dialed) != 1 {
		t.Fatalf("redirect to an internal host must be refused: %v (dialed %v)", err, dialed)
	}
}

func TestGuardedClientExemptsOnlyTheConfiguredRegistry(t *testing.T) {
	var dialed []string
	client := NewGuardedClient(ClientOptions{
		AllowHost: "registry.sdo.svc:5000",
		Lookup:    lookupTable(map[string]string{"other.sdo.svc": "10.0.0.9", "registry.sdo.svc": "10.0.0.8"}),
		Dial: func(_ context.Context, _, address string) (net.Conn, error) {
			dialed = append(dialed, address)
			return nil, errors.New("stub dial")
		},
	})
	for target, refused := range map[string]bool{"http://registry.sdo.svc:5000/": false, "http://other.sdo.svc:5000/": true, "http://registry.sdo.svc:5001/": true} {
		request, _ := http.NewRequest(http.MethodGet, target, nil)
		_, err := client.Do(request)
		if errors.Is(err, ErrRefusedTarget) != refused {
			t.Fatalf("%s: refused=%v, err=%v", target, refused, err)
		}
	}
}

func TestPublicAddress(t *testing.T) {
	for address, want := range map[string]bool{"8.8.8.8": true, "2606:4700::1111": true, "10.1.2.3": false, "172.16.0.1": false, "192.168.1.1": false, "127.0.0.1": false,
		"::1": false, "169.254.1.1": false, "0.0.0.0": false, "100.64.0.1": false, "fe80::1": false, "224.0.0.1": false, "::ffff:192.168.0.1": false} {
		if got := PublicAddress(netip.MustParseAddr(address)); got != want {
			t.Errorf("PublicAddress(%s) = %v, want %v", address, got, want)
		}
	}
}
