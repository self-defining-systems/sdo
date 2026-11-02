// Package apiio owns API-history wall-clock, timer and randomness reads.
package apiio

import (
	"crypto/rand"
	"time"
)

// Now reads the wall clock.
func Now() time.Time { return time.Now() }

// NewTicker starts a wall-clock ticker.
func NewTicker(interval time.Duration) *time.Ticker { return time.NewTicker(interval) }

// NewTimer starts a wall-clock timer.
func NewTimer(delay time.Duration) *time.Timer { return time.NewTimer(delay) }

// Read fills b with cryptographically secure random bytes.
func Read(b []byte) error { _, err := rand.Read(b); return err }

// Text returns a fresh random, filename-safe token.
func Text() string { return rand.Text() }
