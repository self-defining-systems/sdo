// Package proxyio owns the API proxy's wall-clock and randomness reads.
package proxyio

import (
	"crypto/rand"
	"time"
)

// Now reads the wall clock.
func Now() time.Time { return time.Now() }

// Read fills b with cryptographically secure random bytes.
func Read(b []byte) { _, _ = rand.Read(b) }
