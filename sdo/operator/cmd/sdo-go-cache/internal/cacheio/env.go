// Package cacheio owns the Go cache helper's environment and clock reads.
package cacheio

import (
	"os"
	"time"
)

// Getenv reads one process environment variable.
func Getenv(key string) string { return os.Getenv(key) }

// UnixNano returns the wall clock used to stamp cache entries.
func UnixNano() int64 { return time.Now().UnixNano() }
