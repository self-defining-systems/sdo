// Package clientio builds the observation HTTP client over an in-memory
// round tripper; it opens no connections.
package clientio

import "net/http"

// New returns a client that never follows redirects.
func New(transport http.RoundTripper) *http.Client {
	return &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
