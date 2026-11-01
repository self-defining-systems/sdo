// Package h2testio configures HTTP/2 wire peers for in-memory conformance tests.
// It creates no listeners and is only imported by tests.
package h2testio

import (
	"golang.org/x/net/http2"
	"net/http"
)

// NewServer gives each simulated peer its own error-channel pool. The default
// unconfigured ServeConn path shares pooled channels across synctest bubbles.
func NewServer() (*http2.Server, error) {
	server := &http2.Server{}
	if err := http2.ConfigureServer(&http.Server{}, server); err != nil {
		return nil, err
	}
	return server, nil
}
